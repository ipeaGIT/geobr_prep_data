#> DATASET: states 2010, 2022
#> Source: IBGE - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html?=&t=o-que-e
#> scale 1:250.000
#> Metadata: #####
# Título: Estados
# Título alternativo: states
# Frequência de atualização: ?????
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Poligonos dos estados brasileiros.
# Informações adicionais: Dados produzidos pelo IBGE, e utilizados na elaboração do shape dos estados com a melhor base oficial disponível.
# Propósito: Disponibilização das fronteiras estaduais do Brasil.
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas: ****
# Informação do Sistema de Referência: SIRGAS 2000
#
# Observações: 
# Anos disponíveis: 2010, 2022***

# Download the data  -----------------------------------------------------------

# year <- tar_read(years_states, branches = 1)[1]

download_states <- function(year){ # year = 2010
  
  ## 0. Generate the correct ftp link (UPDATE YEAR HERE) -----------------------
  
  url_start <- paste0("https://geoftp.ibge.gov.br/organizacao_do_territorio/",
                      "malhas_territoriais/malhas_municipais/municipio_")
  
  # Before 2015
  if(year %in% c(2000, 2001, 2010:2014)) {
    ### create states tibble
    states <- states_geobr()
    
    ### parts of url
    
    #2000 ou 2010
    if(year %in% c(2000, 2010)) {
      ftp_link <- paste0(url_start, year, "/", states$abbrevm_state, "/",
                         states$abbrevm_state, "_unidades_da_federacao.zip")
    }
    
    #2001
    if(year == 2001) {
      ftp_link <- paste0(url_start, year, "/", states$abbrevm_state, "/",
                         states$code_state, "uf2500g.zip")
    }
    
    #2013
    if(year == 2013) {
      ftp_link <- paste0(url_start, year, "/", states$abbrev_state, "/",
                         states$abbrevm_state, "_unidades_da_federacao.zip")
    
      #correct spell error in AM
      ftp_link[3] <- paste0(url_start, year, "/", states$abbrev_state[3], "/",
                            states$abbrevm_state[3], "_unidades_da_fedecao.zip")
      
    }
    
    #2014
    if(year == 2014) {
      ftp_link <- paste0(url_start, year, "/", states$abbrev_state, "/",
                         states$abbrevm_state, "_unidades_da_federacao.zip")
    }
    
    filenames <- basename(ftp_link)
    
    names(ftp_link) <- filenames
  } 
  
  # 2015 until 2019
  if(year %in% c(2015:2019)) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR/br_unidades_da_federacao.zip")
  }
  
  # 2020 until 2022
  if(year %in% c(2020:2022)) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR/BR_UF_", year, ".zip")
  }
  
  # After 2023
  if(year >= 2023) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR_UF_", year, ".zip")
  }
  
  ## 1. Create temp folder -----------------------------------------------------
  
  zip_dir <- paste0(tempdir(), "/states/", year)
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
    
    file_raw <- download_file_geobr(
      file_url = ftp_link,
      dest_dir = zip_dir
    )
    
  }
  
  ## 4. Unzip Raw data ---------------------------------------------------------
  
  files <- unzip_geobr(zip_dir = zip_dir, out_zip = out_zip)
  
  ## 5. Set correct encoding ---------------------------------------------------
  
  if (year == 2000) { #years without number of columns errors
    encode <- "ENCODING=IBM437"
  }
  
  if (year %in% c(2001, 2005, 2007, 2010)) {
    encode <- "ENCODING=WINDOWS-1252"
  }
  
  if (year >= 2013) {
    encode =  "ENCODING=UTF8"
  }
  
  ## 6. Bind Raw data together -------------------------------------------------
  
  states_raw <- readmerge_geobr(
    folder_path = out_zip,
    encoding = encode
    )
  
  
  ## 7. Show result ------------------------------------------------------------
  states_raw <- sf::st_as_sf(states_raw) |> 
    janitor::clean_names()
  
  states_raw$year <- year
  
  return(states_raw)
  
}

# Clean the data  --------------------------------------------------------------

# states_raw <- tar_read(states_raw, branches = 2)
# mapview::mapview(states_raw)
clean_states <- function(states_raw){
  
  ## 0. Create folder to save clean data
  yyyy <- states_raw$year[1]
  dir_clean <- paste0("./data/states/", yyyy)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  
  ## 4. Rename columns and reorder columns and other post corrections
  
  # standardize colnames
  states_clean <- rename_cols_geobr(states_raw, dicionario_state) |> 
    dplyr::select(code_state) |> 
    dplyr::filter(code_state != 0)

  # dissolve borders to clean geometry and remove repetition
  temp_sf <- dissolve_polygons_no_split(
    mysf = states_clean,
    group_column = "code_state"
  )
  
  
  ## 5. Apply harmonize geobr cleaning -----------------------------------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = temp_sf,
    year = yyyy,
    add_state = T, 
    state_column = "code_state",
    add_region = T,
    region_column = "code_state",
    add_snake_case = T,
    snake_colname = c("name_state", "name_region"),
    projection_fix = T, # dissolve_polygons fixed it already
    encoding_utf8 = T,
    topology_fix = T,
    remove_z_dimension = T,
    use_multipolygon = T
  )

  # mapview::mapview(temp_sf)
  
  ## 6. Check integrity and do post corrections --------------------------------

  temp_sf <- temp_sf |> 
    dplyr::select(code_state, name_state, abbrev_state, 
                    code_region, name_region, year, geometry)
  

  if (nrow(temp_sf) != 27) {stop("existem apenas 27 unidades da federacao")}
  
  ## 7. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 8. Save datasets  ---------------------------------------------------------
  
  ### Save in parquet
  write_geobr_parquet(
    sf_obj = temp_sf,
    path = paste0(dir_clean, "/states_", yyyy, ".parquet"))

  write_geobr_parquet(
    sf_obj = temp_sf_simplified, 
    path = paste0(dir_clean,"/states_", yyyy, "_simplified", ".parquet"))


  ## 9. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}

############### OLD CODE BELOW #########

# 
#     # strange error in Bahia 2000
#     # remove geometries with area == 0
#     temp_sf <- subset(temp_sf, !is.na(abbrev_state))
#     temp_sf <- temp_sf[ as.numeric(st_area(temp_sf)) != 0, ]
#     # if (year==2000 & any(temp_sf$abbrev_state=='BA')) { temp_sf <- temp_sf[which.max(st_area(temp_sf)),] }
# 
