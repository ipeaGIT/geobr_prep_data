#> DATASET: Micro Geographic Regions - 2017
#> Source: IBGE - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html?=&t=o-que-e
#> scale 1:250.000
#> Metadata:
# Título: Microrregiões Geográficas
# Título alternativo: micro regions
# Frequência de atualização: anual, encerrado em 2017.
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: Utf-8
#
# Resumo: Microrregiões Geográficas foram criadas pelo IBGE em 2000. Em 2017 o IBGE substituiu o conceito pelas regiões imediatas.
#
# Estado: Em desenvolvimento
# Palavras chaves descritivas:****
# Informacao do Sistema de Referencia: SIRGAS 2000

# Observações: 
# Anos disponíveis: 2000 a 2018

# Download the data  -----------------------------------------------------------
download_microregions <- function(year){ # year = 2001
  
  ## 0. Generate the correct ftp link ------------------------------------------
  
  url_start <- paste0("https://geoftp.ibge.gov.br/organizacao_do_territorio/",
                      "malhas_territoriais/malhas_municipais/municipio_")
  
  
  # loads states tibble
  states <- states_geobr()
    
    # builds url
    if (year < 2013) {
      ftp_link <- paste0(url_start, year, "/", states$abbrevm_state, "/")
      all_files <- lapply(X=ftp_link, FUN = function(i){list_folders(i)})
      all_files <- unlist(all_files)
      target_file <- all_files[all_files %like% "icrorregioes|mi2500"]
      ftp_link <- paste0(ftp_link, target_file)
      
      filenames <- basename(ftp_link)
      names(ftp_link) <- filenames
      }
    
    if (year %in% c(2013, 2014)) {
      ftp_link <- paste0(url_start, year, "/", states$abbrev_state, "/")
      all_files <- lapply(X=ftp_link, FUN = function(i){list_folders(i)})
      all_files <- unlist(all_files)
      target_file <- all_files[all_files %like% "icrorregioes|mi2500"]
      ftp_link <- paste0(ftp_link, target_file)
      
      filenames <- basename(ftp_link)
      names(ftp_link) <- filenames
    }
    
  
  # After 2015
  if (year %in% c(2015:2022)) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR/")
    all_files <- list_folders(ftp_link)
    target_file <- all_files[all_files %like% "icrorregioes"]
    ftp_link <- paste0(ftp_link, target_file)
  }
  
  
  ## 1. Create temp folder -----------------------------------------------------
  
  zip_dir <- paste0(tempdir(), "/micro_regions/", year)
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
      tryCatch(
        download.file(ftp_link[name_file],
                      paste(zip_dir, name_file, sep = "\\"), quiet = TRUE),
        error = function(e) message("Aviso: arquivo nao encontrado - ", name_file)
      )
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
  
  microregions_raw <- readmerge_geobr(
    folder_path = out_zip,
    encoding = encode
    ) |> 
    janitor::clean_names()
  
  microregions_raw$year <- year
  
  return(microregions_raw)
}

# Clean the data  --------------------------------------------------------------
# microregions_raw <- tar_read(microregions_raw, branches = 1)
clean_microregions <- function(microregions_raw){ # year = 2024
  
  ## 0. Create folder to save clean data ---------------------------------------
  yyyy <- microregions_raw$year[1]
  dir_clean <- paste0("./data/micro_regions/", yyyy)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  
  ## 2. Prepare for cleaning ---------------------------------------------------
  
  # standardize colnames
  microregions <- rename_cols_geobr(microregions_raw, dicionario_micro) |> 
    dplyr::select(code_micro, name_micro)

  
  microregions <- microregions |>
    mutate(code_state = substr(code_micro, 1, 2)) |>
    filter(code_state != 0) |>
    sf::st_make_valid() |>
    group_by(code_state, code_micro, name_micro) |>
    summarise() |>
    ungroup()


  ## 3. Manual corrections

  # strange error in Bahia 2000
  # remove geometries with area == 0
  microregions <- microregions[ as.numeric(sf::st_area(microregions)) != 0, ]

  ## 3b. 2000: recupera 2 microrregioes faltantes (Maranhao) usando 2001
  ## 210520 Gerais de Balsas | 210521 Chapadas das Mangabeiras
  ## ate 2026-04 essa correcao era feita por fix_microregions (target separado)
  ## que sobrescrevia microregions_2000.parquet, causando outdated perpetuo do
  ## branch microregions_clean[year=2000]. Integrar aqui elimina o conflito.
  if (yyyy == 2000) {
    raw_2001 <- download_microregions(2001)
    micro_2001 <- rename_cols_geobr(raw_2001, dicionario_micro) |>
      dplyr::select(code_micro, name_micro) |>
      mutate(code_state = substr(code_micro, 1, 2)) |>
      filter(code_state != 0) |>
      sf::st_make_valid() |>
      group_by(code_state, code_micro, name_micro) |>
      summarise() |>
      ungroup()
    missing_micros <- micro_2001 |>
      filter(code_micro %in% c(210520, 210521))
    microregions <- rbind(microregions, missing_micros)
  }

 

  ## 3. Apply harmonize geobr cleaning -----------------------------------------

  temp_sf <- harmonize_geobr(
    temp_sf = microregions,
    year = yyyy,
    add_state = T,
    state_column = "code_state",
    add_region = T,
    region_column = "code_state",
    add_snake_case = T,
    snake_colname = "name_micro",
    projection_fix = T,
    encoding_utf8 = T,
    topology_fix = T,
    remove_z_dimension = T,
    use_multipolygon = T
  )


  ## 3c. Enforce column order (geometry always last)
  temp_sf <- temp_sf |>
    dplyr::select(#dplyr::any_of(c("code_micro", "name_micro")),
                  code_micro, name_micro,
                  code_state, abbrev_state, name_state,
                  code_region, name_region,
                  year, geometry)

  # sort by key columns
  temp_sf <- temp_sf |> 
    dplyr::arrange(code_state, code_micro)
  
  ## 3d. Validate before saving
  stopifnot(is.numeric(temp_sf$code_micro))
  stopifnot(is.numeric(temp_sf$code_state))
  stopifnot(is.numeric(temp_sf$code_region))
  stopifnot(is.character(temp_sf$abbrev_state))
  stopifnot(all(nchar(temp_sf$abbrev_state) == 2))
  stopifnot(!is.na(sf::st_crs(temp_sf)))
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")

  ## 4. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 6. Save datasets  ---------------------------------------------------------
  
  write_geobr_parquet(
    sf_obj = temp_sf,
    path = paste0(dir_clean, "/microregions_", yyyy, ".parquet"))

  write_geobr_parquet(
    sf_obj = temp_sf_simplified,
    path = paste0(dir_clean,"/microregions_", yyyy, "_simplified", ".parquet"))
  
  
  ## 7. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}




# NOTA: fix_microregions foi REMOVIDO em 2026-04-26.
# A correcao das microrregioes 210520 e 210521 (faltantes em 2000) foi
# integrada dentro de clean_microregions(year=2000) acima. Razao: o target
# microregions_fixed sobrescrevia microregions_2000.parquet (output do
# branch microregions_clean[year=2000]), causando outdated perpetuo do
# branch (hash do arquivo nao batia mais com hash registrado pelo branch).
# Cada arquivo agora e output de UM unico target.

################ RAFAEL OLD CODE BELOW --------------
 



# ###### Correcting number of digits of micro regions in 2010  --------------------------------
# # issue #20
# # use data of 2013 to add code and name of micro regions in the 2010 data
# 
# # Dirs
# micro_dir <- "L:////# DIRUR #//ASMEQ//geobr//data-raw//malhas_municipais//shapes_in_sf_all_years_cleaned2/micro_regiao"
# sub_dirs <- list.dirs(path =micro_dir, recursive = F)
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
# # Create function to correct number of digits of meso regions in 2010, based on 2013 data
# correct_micro_digits <- function(a2010_sf_micro_file){ # a2010_sf_micro_file <- sf_files_2010[39]
# 
# 
#   # Get UF of the file
#   get_uf <- function(x){if (grepl("simplified",x)) {
#     substr(x, nchar(x)-19, nchar(x)-18)
#   } else {substr(x, nchar(x)-8, nchar(x)-7)}
#   }
#   uf <- get_uf(a2010_sf_micro_file)
# 
#   # read 2010 file
#   temp2010 <- st_read(a2010_sf_micro_file)
# 
#   # dplyr::rename and subset columns
#   temp2010 <- temp2010 %>%  dplyr::mutate(name_micro =as.character(name_micro))
#   temp2010 <- temp2010 %>%  dplyr::mutate(name_micro = ifelse(name_micro == "Moji Das Cruzes","Mogi Das Cruzes",
#                                                               ifelse(name_micro == "Piraçununga","Pirassununga",
#                                                                      ifelse(name_micro == "Moji-Mirim","Moji Mirim",
#                                                                             ifelse(name_micro == "São Miguel D'oeste","São Miguel Do Oeste",
#                                                                                    ifelse(name_micro == "Serras Do Sudeste","Serras De Sudeste",
#                                                                                           ifelse(name_micro == "Vão Do Paraná","Vão Do Paranã",name_micro)))))))
# 
# 
#   # read 2013 file
#   temp2013 <- sf_files_2013[if (grepl("simplified",a2010_sf_micro_file)) {
#     (sf_files_2013 %like% paste0("/",uf)) & (sf_files_2013 %like% "simplified")
#   } else {
#     (sf_files_2013 %like% paste0("/",uf)) & !(sf_files_2013 %like% "simplified")
#   }]
#   temp2013 <- st_read(temp2013)
# 
#   # keep only code and name columns
#   table2013 <- temp2013 %>% as.data.frame()
#   table2013 <- dplyr::select(table2013, code_micro, name_micro)
# 
#   # update code_micro
#     # subset(temp2010, name_micro %like% 'Moji')
#     # subset(temp2010, name_micro %like% 'Mogi')
# 
#   sf2010 <- left_join(temp2010, table2013, by="name_micro")
#   sf2010 <- dplyr::select(sf2010, code_state, abbrev_state, name_state, code_micro=code_micro.y, name_micro, geom)
#   head(sf2010)
# 
#   # Save file
#   # write_rds(sf2010, path = a2010_sf_micro_file, compress="gz" )
#   st_write(sf2010,a2010_sf_micro_file,append = FALSE,delete_dsn =T,delete_layer=T)
# }
# 
