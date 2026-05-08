#> DATASET: Meso Geographic Regions
#> Source: IBGE - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html?=&t=o-que-e
#> scale 1:250.000 ?????????????
#> Metadata:
# Título: Mesorregiões Geográficas
# Título alternativo: meso regions
# Frequência de atualização: decenal /// anual, encerrado em 2017
#
# Forma de apresentacao: Shape
# Linguagem: Pt-BR
# Character set: Utf-8
#
# Resumo: Mesoregiões Geográficas foram criadas pelo IBGE em 2000. Em 2017 o IBGE substituiu o conceito pelas regiões intermediárias.
#
# Estado: Em desenvolvimento
# Palavras chaves descritivas:****
# Informacao do Sistema de Referencia: SIRGAS 2000

# Observações: 
# Anos disponíveis: 2000 a 2018


# Download the data  -----------------------------------------------------------
download_mesoregions <- function(year){ # year = 2001
  
  ## 0. Generate the correct ftp link ------------------------------------------
  
  url_start <- paste0("https://geoftp.ibge.gov.br/organizacao_do_territorio/",
                      "malhas_territoriais/malhas_municipais/municipio_")
  
  # loads states tibble
  states <- states_geobr()
  
  # builds url
  if(year <2013) {
    ftp_link <- paste0(url_start, year, "/", states$abbrevm_state, "/")
    all_files <- lapply(X=ftp_link, FUN = function(i){list_folders(i)})
    all_files <- unlist(all_files)
    target_file <- all_files[all_files %like% "esorregioes|me2500"]
    ftp_link <- paste0(ftp_link, target_file)
    
    filenames <- basename(ftp_link)
    names(ftp_link) <- filenames
  }
  
  if(year %in% c(2013, 2014)) {
    ftp_link <- paste0(url_start, year, "/", states$abbrev_state, "/")
    all_files <- lapply(X=ftp_link, FUN = function(i){list_folders(i)})
    all_files <- unlist(all_files)
    target_file <- all_files[all_files %like% "esorregioes|me2500"]
    ftp_link <- paste0(ftp_link, target_file)
    
    filenames <- basename(ftp_link)
    names(ftp_link) <- filenames
  }
  
  # After 2015
  if(year %in% c(2015:2022)) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR/")
    all_files <- list_folders(ftp_link)
    target_file <- all_files[all_files %like% "esorregioes"]
    ftp_link <- paste0(ftp_link, target_file)
  }
  
  
  ## 1. Create temp folder -----------------------------------------------------
  
  zip_dir <- paste0(tempdir(), "/meso_regions/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  
  ## 2. Create direction for each download -------------------------------------
  
  file_raw <- fs::file_temp(tmp_dir = zip_dir,
                            ext = fs::path_ext(ftp_link))
  
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  ## 3. Download Raw data ------------------------------------------------------
  
  if(year %in% c(2000, 2001, 2010:2014)) {
    ### Download zipped files
    for (name_file in filenames) {
      download.file(ftp_link[name_file],
                    paste(zip_dir, name_file, sep = "\\"))
    }
  }
  
  if(year >= 2015) {
    httr::GET(url = ftp_link,
              httr::progress(),
              httr::write_disk(path = file_raw,
                               overwrite = T))
  }
  
  ## 4. Unzip Raw data ---------------------------------------------------------
  
  unzip_geobr(zip_dir = zip_dir, out_zip = out_zip)
  
  ## 5. Set correct encoding ---------------------------------------------------
  
  if (year == 2000) { #years without number of collumns errors
    encode <- "ENCODING=IBM437"
  }
  
  if (year %in% c(2001, 2005, 2007, 2010)) {
    encode <- "ENCODING=WINDOWS-1252"
  }
  
  if (year >= 2013) {
    encode =  "ENCODING=UTF8"
  }
  
  ## 6. Bind Raw data together -------------------------------------------------
  
  shp_names <- list.files(out_zip, pattern = "\\.shp$", full.names = TRUE)
  
  mesoregions_raw <- readmerge_geobr(
    folder_path = out_zip, 
    encoding = encode
    ) |> 
    janitor::clean_names()

  mesoregions_raw$year <- year
  
  return(mesoregions_raw)
  
}

# Clean the data  --------------------------------------------------------------
# mesoregions_raw <- tar_read(mesoregions_raw, 1)
clean_mesoregions <- function(mesoregions_raw){ # year = 2024
  
  ## 0. Create folder to save clean data ---------------------------------------
  yyyy <- mesoregions_raw$year[1]
  dir_clean <- paste0("./data/meso_regions/", yyyy)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. Checking ---------------------------------------------------------------

  # Há duplicações no nome? Ilhas
  # mesoregions_raw |> count(nm_meso) |> filter(n > 1)
  
  # Há duplicações pela geometria?
  # eq_list <- st_equals(mesoregions_raw)
  # duplicados_geom <- eq_list[lengths(eq_list) > 1]
  
  ## 2. Prepare for cleaning ---------------------------------------------------
  
  # standardize colnames
  mesoregions <- rename_cols_geobr(mesoregions_raw, dicionario_meso) |>
    dplyr::select(code_meso, name_meso) |>
    mutate(code_state = substr(code_meso, 1, 2)) |>
    filter(code_meso != 0)

  ## 2b. 2000: recupera 2 mesorregioes faltantes (Maranhao) usando 2001
  ## 2104 Leste Maranhense | 2105 Sul Maranhense
  ## ate 2026-04 essa correcao era feita por fix_mesoregions (target separado)
  ## que sobrescrevia mesoregions_2000.parquet, causando outdated perpetuo do
  ## branch mesoregions_clean[year=2000]. Integrar aqui elimina o conflito.
  if (yyyy == 2000) {
    raw_2001 <- download_mesoregions(2001)
    meso_2001 <- rename_cols_geobr(raw_2001, dicionario_meso) |>
      dplyr::select(code_meso, name_meso) |>
      mutate(code_state = substr(code_meso, 1, 2)) |>
      filter(code_meso != 0)
    missing_mesos <- meso_2001 |>
      filter(code_meso %in% c(2104, 2105))
    mesoregions <- rbind(mesoregions, missing_mesos)
  }


  # 666666666666666666666666666666
  # erro em 2000
  # if (yyyy == 2000) {
    # este ano tem 2 mesos no maranhão vazias de dados (Leste e Sul)
    # localizar os codes corretos do Sul e Leste Maranhense
    # a shape do centro está nomeada como Sul
    # uma pequena shape quadrada está nomeada como Centro
    

    # test <- mesoregions_raw |>
    #   mutate(code_state = stringr::str_sub(codigo, start = 1, end = 2)) |> 
    #   #filter(code_state %in% c("21", NA, "0")) |> 
    #   select(code_state, nome, geometry) |> 
    #   filter(code_state == "35")
    # #filter(str_detect(nome, pattern = "Maranhense")) 
    
    # mapa_test <- ggplot(data = mesoregions) +
    #   geom_sf(aes(fill = code_state), color = "black") + # Desenha o mapa
    #   geom_sf_text(aes(label = name_meso), size = 3, color = "darkred") + 
    #   theme_minimal()
    #plot(mapa_test)
    
    # # preparar as shapes trocadas e erradas
    # shp_trocada <- mesoregions |> 
    #   filter(code_state == "21", str_detect(name_meso, pattern = "Sul")) |> 
    #   mutate(name_meso = "Centro Maranhense",
    #          code_meso = "2103") |>
    #   select(code_state, code_meso, name_meso, geometry)
    #   
    # shp_erradas <- mesoregions_raw |>
    #   filter(codigo == "0") |>
    #   mutate(code_state = "21",
    #          code_meso = c("2105", "2104"),
    #          name_meso = as.character(seq(1:2))) |>
    #   select(code_state, code_meso, name_meso, geometry) |>
    #   #filter(name_meso == "2")
    #   mutate(name_meso = case_when(name_meso == "1" ~ "Sul Maranhense",
    #                                name_meso == "2" ~ "Leste Maranhense"))
# 
#    # corrigir
#     mesoregions <- mesoregions_raw |> 
#       st_make_valid() |> 
#       filter(!str_detect(nome, pattern = "Sul Maranhense"),
#              codigo != "0") |> 
#       mutate(code_state = stringr::str_sub(codigo, start = 1, end = 2)) |> 
#       rename(code_meso = codigo, name_meso = nome) |> 
#       select(code_meso, name_meso, code_state, geometry) |> 
#       rbind(shp_erradas, shp_trocada) |> 
#       group_by(code_meso, name_meso, code_state) |> 
#       summarise() |> 
#       ungroup() |> 
#       left_join(states, by = c("code_state")) |> 
#       relocate(geometry, .after = name_region)
#   
#   }
#   
  
  
  ## 3. Apply harmonize geobr cleaning -----------------------------------------

  temp_sf <- harmonize_geobr(
    temp_sf = mesoregions,
    year = yyyy,
    add_state = T,
    state_column = "code_state",
    add_region = T, 
    region_column = "code_state",
    add_snake_case = T,
    snake_colname = "name_meso",
    projection_fix = T,
    encoding_utf8 = T,
    topology_fix = T,
    remove_z_dimension = T,
    use_multipolygon = T
  )

  ## 3c. Enforce column order (geometry always last)
  temp_sf <- temp_sf |>
    dplyr::select(code_meso, name_meso,
                  code_state, abbrev_state, name_state,
                  code_region, name_region,
                  year, geometry)

  # sort by key columns
  temp_sf <- temp_sf |> 
    dplyr::arrange(code_state, code_meso)
  
  ## 3d. Validate before saving
  stopifnot(is.numeric(temp_sf$code_meso))
  stopifnot(is.numeric(temp_sf$code_state))
  stopifnot(is.numeric(temp_sf$code_region))
  stopifnot(is.character(temp_sf$abbrev_state))
  stopifnot(all(nchar(temp_sf$abbrev_state) == 2))
  stopifnot(!is.na(sf::st_crs(temp_sf)))
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")

  ## 4. Lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 6. Save datasets  ---------------------------------------------------------
  
  # Save in parquet
  write_geobr_parquet(
    temp_sf,
    paste0(dir_clean, "/mesoregions_", yyyy, ".parquet"))

  write_geobr_parquet(
    temp_sf_simplified,
    paste0(dir_clean,"/mesoregions_", yyyy, "_simplified.parquet"))
  
  ## 7. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}




# NOTA: fix_mesoregions foi REMOVIDO em 2026-04-26.
# A correcao das mesorregioes 2104 (Leste Maranhense) e 2105 (Sul Maranhense)
# faltantes em 2000 foi integrada dentro de clean_mesoregions(year=2000)
# acima. Razao: o target mesoregions_fixed sobrescrevia mesoregions_2000.parquet
# (output do branch mesoregions_clean[year=2000]), causando outdated perpetuo
# do branch (hash do arquivo nao batia mais com hash registrado pelo branch).
# Cada arquivo agora e output de UM unico target.


########################## OLD FILE BELOW HERE ##########

# 
# ###### Correcting number of digits of meso regions in 2010  --------------------------------
# # issue #20
# 
# 
# # Dirs
# meso_dir <- "L:////# DIRUR #//ASMEQ//geobr//data-raw//malhas_municipais//shapes_in_sf_all_years_cleaned2/meso_regiao"
# sub_dirs <- list.dirs(path =meso_dir, recursive = F)
# 
# # dirs of 2010 (problematic data) ad 2013 (reference data)
# sub_dir_2010 <- sub_dirs[sub_dirs %like% 2010]
# sub_dir_2013 <- sub_dirs[sub_dirs %like% 2013]
# 
# 
# # list sf files in each dir
# sf_files_2010 <- list.files(sub_dir_2010, full.names = T, pattern = ".gpkg")
# sf_files_2013 <- list.files(sub_dir_2013, full.names = T, pattern = ".gpkg")
# 
# 
# # Create function to correct number of digits of meso regions in 2010
# 
# # use data of 2013 to add code and name of meso regions in the 2010 data
# correct_meso_digits <- function(a2010_sf_meso_file){ # a2010_sf_meso_file <- sf_files_2010[1]
# 
#   # Get UF of the file
#   get_uf <- function(x){if (grepl("simplified",x)) {
#     substr(x, nchar(x)-19, nchar(x)-18)
#   } else {substr(x, nchar(x)-8, nchar(x)-7)}
#   }
#   uf <- get_uf(a2010_sf_meso_file)
# 
# 
#   # read 2010 file
#   temp2010 <- st_read(a2010_sf_meso_file)
# 
#   # read 2013 file
#   temp2013 <- sf_files_2013[ if (grepl("simplified",a2010_sf_meso_file)) {
#     (sf_files_2013 %like% paste0("/",uf)) & (sf_files_2013 %like% "simplified")
#  } else {
#     (sf_files_2013 %like% paste0("/",uf)) & !(sf_files_2013 %like% "simplified")
#   }]
#   temp2013 <- st_read(temp2013)
# 
#   # keep only code and name columns
#   table2013 <- temp2013 %>% as.data.frame()
#   table2013 <- dplyr::select(table2013, code_meso, name_meso)
# 
#   # update code_meso
#   sf2010 <- left_join(temp2010, table2013, by="name_meso")
#   sf2010 <- dplyr::select(sf2010, code_state, abbrev_state, name_state, code_meso=code_meso.y, name_meso, geom)
# 
#   # Save file
#   st_write(sf2010,a2010_sf_meso_file,append = FALSE,delete_dsn =T,delete_layer=T)
# }
# 
# # Apply function
# lapply(sf_files_2010, correct_meso_digits)
# 
# 
