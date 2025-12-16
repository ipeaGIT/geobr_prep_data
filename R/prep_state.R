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

### Libraries (use any library as necessary) -----------------------------------

# library(RCurl)
# library(arrow)
# library(geoarrow)
# library(stringr)
# library(sf)
# library(purrr)
# library(janitor)
# library(dplyr)
# library(readr)
# library(data.table)
# library(magrittr)
# library(devtools)
# library(lwgeom)
# library(stringi)
# library(tidyverse)
# library(mirai)
# library(rvest)
# source("./R/support_harmonize_geobr.R")
# source("./R/support_fun.R")


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
  
  ### Alternative folder
  # zip_dir <- paste0("./data_raw/", "/states/", year)
  # dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  # dir.exists(zip_dir)
  
  ## 2. Create direction for each download -------------------------------------
  
  ### zip folder
  in_zip <- paste0(zip_dir, "/zipped/")
  dir.create(in_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(in_zip)
  
  file_raw <- fs::file_temp(tmp_dir = in_zip,
                            ext = fs::path_ext(ftp_link))
  
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  ## 3. Download Raw data ------------------------------------------------------
  
  if(year %in% c(2000, 2001, 2010:2014)) {
    ### Download zipped files
    for (name_file in filenames) {
      download.file(ftp_link[name_file],
                    paste(in_zip, name_file, sep = "\\"))
    }
  }
  
  if(year %in% 2015:2024) {
    httr::GET(url = ftp_link,
              httr::progress(),
              httr::write_disk(path = file_raw,
                               overwrite = T))
  }
  
  ## 4. Unzip Raw data ---------------------------------------------------------
  
  unzip_geobr(zip_dir = zip_dir, in_zip = in_zip, out_zip = out_zip, is_shp = TRUE)
  
  ## 5. Bind Raw data together -------------------------------------------------
  
  shp_names <- list.files(out_zip, pattern = "\\.shp$", full.names = TRUE)
  
  #### Before 2015
  if (year == 2000) { #years without number of collumns errors
    states_list <- pbapply::pblapply(
      X = shp_names,
      FUN = function(x){ sf::st_read(x, quiet = T, stringsAsFactors= F)
        }
    )
    
    states_raw <- data.table::rbindlist(states_list)
  }
  
  if (year %in% c(2001, 2010:2014))  {#years with error in number of collumns
    states_raw <- readmerge_geobr(folder_path = out_zip)
  }
  
  #### After 2015
  if (length(shp_names) == 1) {
    states_raw <- st_read(shp_names, quiet = T, stringsAsFactors= F)
  }
  
  ## 6. Integrity test ---------------------------------------------------------
  
  #### Before 2015
  # glimpse(states_raw)

  #### After 2015
  if (length(shp_names) == 1) {
    table_collumns <- tibble(name_collum = colnames(states_raw),
                             type_collum = sapply(states_raw, class)) |> 
      rownames_to_column(var = "num_collumn")
    
    # glimpse(table_collumns)
    # glimpse(states_raw)
  }
  
  ## 7. Show result ------------------------------------------------------------
  
  data.table::setDF(states_raw)
  
  states_raw <- sf::st_as_sf(states_raw) |> 
    clean_names()
  
  # glimpse(states_raw)
  
  return(states_raw)
  
}

# Clean the data  --------------------------------------------------------------

# year <- tar_read(years_states, branches = 1)[1]
# states_raw <- tar_read(states_raw, branches = 1)

clean_states <- function(states_raw, year){ # year = 2024
  
  ## 0. Create folder to save clean data ---------------------------------------
  
  dir_clean <- paste0("./data/states/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. Create states names reference table ------------------------------------
  
  states_geobr <- states_geobr()
  
  ## 2. Adjust and preparing for cleaning --------------------------------------
  
  #For years that have spelling problems
  if (year %in% c(2000, 2001, 2010, 2013:2018)){ 
    # glimpse(states_raw)
    # glimpse(states_geobr)
    
    states_thin <- states_geobr |> 
      select(code_state, abbrev_state, name_state)
    
    if (year %in% c(2000, 2001)){ 
      states_clean <- states_raw |> 
        select(-nome) |> 
        left_join(states_thin, by = c("codigo" = "code_state")) |> 
        relocate(name_state, .after = geocodigo)
    }
    if (year %in% c(2010)){ 
      states_clean <- states_raw |> 
        left_join(states_thin, by = c("cd_geocodu" = "code_state")) |>
        select(-nm_estado, -id) |> 
        relocate(nm_regiao, .before = geometry) |> 
        mutate(nm_regiao = str_to_title(nm_regiao))
    }
    
    # For years that have only uppercase
    if (year %in% c(2013:2018)){ 
      states_clean <- states_raw |> 
        left_join(states_thin, by = c("cd_geocuf" = "code_state")) |>
        mutate(nm_regiao = str_to_title(nm_regiao),
               nm_estado = str_to_title(nm_estado)) |> 
        select(cd_geocuf, abbrev_state, nm_estado, nm_regiao)
    }
    # glimpse(states_clean)
  }
  
  #For years that have no spelling problems
  if (year %in% c(2019:2024)){ 
    # glimpse(states_raw)
    # glimpse(states_geobr)
    states_clean <- states_raw
    # glimpse(states_clean)
  }
  
  ## 3. Create dicionario de equivalências para dataset states -----------------
  
  # 2000
  # c("mslink", "codigo", "area_1", "perimetro", "geocodigo", "nome", "area_tot_g", 
  # "reservado", "geometry")
  
  # 2001
  # c("mslink", "mapid", "codigo", "area_1", "perimetro", "geocodigo", "nome", 
  # "area_tot_g", "geometry")
  
  # 2010
  # c("id", "cd_geocodu", "nm_estado", "nm_regiao", "geometry")
  
  # 2013
  # c("nm_estado", "nm_regiao", "cd_geocuf", "geometry")
  
  # 2023
  # c([1] "cd_uf", "nm_uf", "sigla_uf", "cd_regiao", "nm_regiao", "area_km2",
  # "geometry") 
  
  #Este é um dicionário padrão com as denominações GEOBR:
  dicionario <- data.frame(
    # Lista de nomes padronizados de colunas
    padrao = c(
      #CÓDIGO DE MUNICÍPIO e número de variações associadas
      rep("code_muni", 7),
      #NOME DO MUNICÍPIO e número de variações associadas
      rep("name_muni", 4),
      #CÓDIGO DO ESTADO e número de variações associadas
      rep("code_state", 7),
      #ABREVIAÇÃO DO ESTADO e número de variações associadas
      rep("abbrev_state", 4),
      #NOME DO ESTADO e número de variações associadas
      rep("name_state", 3),
      #CÓDIGO DA REGIÃO e número de variações associadas
      rep("code_region", 2),
      #NOME DA REGIÃO e número de variações associadas
      rep("name_region", 2),
      #ABREVIAÇÃO DA REGIÃO e número de variações associadas
      rep("abbrev_region", 1)
    ),
    # Lista de variações
    variacao = c(
      #Variações que convergem para "code_muni"
      "cod_uf", "cd_uf", "code_uf", "codigo_uf", "cod_state", "cd_mun", "cod_mun",
      #Variações que convergem para "name_muni"
      "nome_cidade", "cidade", "nm_muni", "nome_muni",
      #Variações que convergem para "code_state"
      "cod_uf", "cd_uf", "code_uf", "codigo_uf", "cod_state", "cd_geocodu",
      "cd_geocuf",
      #Variações que convergem para "abbrev_state"
      "sigla", "sigla_uf", "uf", "sg_uf",
      #Variações que convergem para "name_state"
      "nm_uf", "nm_state", "nm_estado",
      #Variações que convergem para "code_region"
      "cd_regia", "cd_regiao",
      #Variações que convergem para "name_region"
      "nm_regia", "nm_regiao",
      #Variações que convergem para "abbrev_region"
      "sigla_rg"
      ), stringsAsFactors = FALSE)
  
  ## 4. Rename collumns and reorder collumns -----------------------------------
  
  states_clean <- standardcol_geobr(states_clean, dicionario)
  
  # glimpse(states_clean)
  
  # ordem recomendada
  # c(temp_sf, 'code_state', 'abbrev_state', 'name_state', 'code_region',
  #  'name_region', 'geom')
  
  
  ## 5. Apply harmonize geobr cleaning -----------------------------------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = states_clean,
    year = year,
    add_state = T, state_column = "name_state",
    add_region = T, region_column = "code_state",
    add_snake_case = T,
    snake_colname = c("name_state", "name_region"),
    projection_fix = T,
    encoding_utf8 = T,
    topology_fix = T,
    remove_z_dimension = T,
    use_multipolygon = T
  )
  
  # glimpse(temp_sf)
  
  if (nrow(temp_sf) > 27) {stop("existem apenas 27 unidades da federacao")}
  
  
  ## 6. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 7. Save datasets  ---------------------------------------------------------
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/states_",  year,
  #                                   ".gpkg"), delete_dsn = TRUE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean, "/states_",
  #                                               year, "_simplified.gpkg"),
  #              delete_dsn = TRUE )
  
  ### Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/states_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 22
  )
  
  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean,"/states_", year, "_simplified", ".parquet"),
    compression='zstd',
    compression_level = 22
  )
  
  ## 8. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}

############### OLD CODE BELOW #########

####### Load Support functions to use in the preprocessing of the data
# 
# source("./prep_data/prep_functions.R")
# source('./prep_data/download_malhas_municipais_function.R')
# 
# 
# setwd('L:/# DIRUR #/ASMEQ/geobr/data-raw')
# 
# 
# ###### download raw data --------------------------------
# # pblapply(X=c(2000,2001,2005,2007,2010,2013:2020), FUN=download_ibge)
# 
# 
# 
# ###### Unzip raw data --------------------------------
# unzip_to_geopackage(region='uf',year='all')
# 
# 
# ###### Cleaning UF files --------------------------------
# setwd('L:/# DIRUR #/ASMEQ/geobr/data-raw/malhas_municipais')
# 
# # get folders with years
# uf_dir <-  paste0(getwd(),"./shapes_in_sf_all_years_original/uf")
# sub_dirs <- list.dirs(path =uf_dir, recursive = F)
# sub_dirs <- sub_dirs[sub_dirs %like% paste0(2000:2020,collapse = "|")]
# 
# 
# 
# # create a function that will clean the sf files according to particularities of the data in each year
# 
# clean_states <- function( e ){  #  e <- sub_dirs[ sub_dirs %like% 2000 ]
# 
#   # get year of the folder
#   last4 <- function(x){substr(x, nchar(x)-3, nchar(x))}   # function to get the last 4 digits of a string
#   year <- last4(e)
#   year
# 
#   # create directory to save original shape files in sf format
#   dir.create(file.path("shapes_in_sf_all_years_cleaned2"), showWarnings = FALSE)
# 
#   # create a subdirectory of states, municipalities, micro and meso regions
#   dir.create(file.path("shapes_in_sf_all_years_cleaned2/uf/"), showWarnings = FALSE)
# 
#   # create a subdirectory of years
#   dir.create(file.path(paste0("shapes_in_sf_all_years_cleaned2/uf/",year)), showWarnings = FALSE)
#   gc(reset = T)
# 
#   dir.dest<- file.path(paste0("./shapes_in_sf_all_years_cleaned2/uf/",year))
# 
# 
#   # list all sf files in that year/folder
#   sf_files <- list.files(e, full.names = T, recursive = T, pattern = ".gpkg$")
# 
# 
# 
#   # for each file
#   for (i in sf_files){ #  i <- sf_files[1]
# 
#     # read sf file
#     temp_sf <- st_read(i)
#     names(temp_sf) <- names(temp_sf) |> tolower()
# 
#     if (year %like% "2000|2001"){
#       # dplyr::rename and subset columns
#          #temp_sf <- dplyr::rename(temp_sf, code_state = geocodigo, name_state = nome)
#       temp_sf <- dplyr::select(temp_sf, c('code_state'=geocodigo, 'name_state'=nome, 'geom'))
#     }
# 
#     if (year %like% "2010"){
#       # dplyr::rename and subset columns
#       temp_sf <- dplyr::rename(temp_sf, code_state = cd_geocodu, name_state = nm_estado)
#       temp_sf <- dplyr::select(temp_sf, c('code_state', 'name_state', 'geom'))
#     }
# 
#     if (year %like% "2013|2014|2015|2016|2017|2018"){
#       # dplyr::rename and subset columns
#       temp_sf <- dplyr::rename(temp_sf, code_state = cd_geocuf, name_state = nm_estado)
#       temp_sf <- dplyr::select(temp_sf, c('code_state', 'name_state', 'geom'))
#     }
# 
#     if (year %like% "2019|2020"){
#       # dplyr::rename and subset columns
#       temp_sf <- dplyr::rename(temp_sf, code_state = cd_uf, name_state = nm_uf)
#       temp_sf <- dplyr::select(temp_sf, c('code_state', 'name_state', 'geom'))
#     }
# 
# 
#     # add name_state
#     temp_sf <- add_state_info(temp_sf,column = 'code_state')
# 
#     # Add Region codes and names
#     temp_sf <- add_region_info(temp_sf,'code_state')
# 
#     # reorder columns
#     temp_sf <- dplyr::select(temp_sf, 'code_state', 'abbrev_state', 'name_state', 'code_region', 'name_region', 'geom')
# 
#     # Use UTF-8 encoding
#     temp_sf <- use_encoding_utf8(temp_sf)
# 
#     # Capitalize the first letter
#     temp_sf$name_state <- stringr::str_to_title(temp_sf$name_state)
#     stringr::str_replace(a, " De ", " de ")
#     stringr::str_replace(a, " Do ", " do ")
# 
#     # Harmonize spatial projection CRS, using SIRGAS 2000 epsg (SRID): 4674
#     temp_sf <- harmonize_projection(temp_sf)
# 
#     # strange error in Bahia 2000
#     # remove geometries with area == 0
#     temp_sf <- subset(temp_sf, !is.na(abbrev_state))
#     temp_sf <- temp_sf[ as.numeric(st_area(temp_sf)) != 0, ]
#     # if (year==2000 & any(temp_sf$abbrev_state=='BA')) { temp_sf <- temp_sf[which.max(st_area(temp_sf)),] }
# 
#     # Make any invalid geom valid # st_is_valid( sf)
#     temp_sf <- sf::st_make_valid(temp_sf)
# 
#     # keep code as.numeric()
#     temp_sf$code_state <- as.numeric(temp_sf$code_state)
#     temp_sf$code_region <- as.numeric(temp_sf$code_region)
# 
#     # remove state repetition
#     temp_sf <- remove_state_repetition(temp_sf)
# 
#     # simplify
#     temp_sf_simplified <- simplify_temp_sf(temp_sf)
# 
#     # convert to MULTIPOLYGON
#     temp_sf <- to_multipolygon(temp_sf)
#     temp_sf_simplified <- to_multipolygon(temp_sf_simplified)
# 
#     # Save cleaned sf in the cleaned directory
#     dir.dest.file <- paste0(dir.dest,"/")
# 
#     # save each state separately
#     for( c in unique(temp_sf$code_state)){ # c <- 11
# 
#       temp2 <- subset(temp_sf, code_state ==c)
#       temp2_simplified <- subset(temp_sf_simplified, code_state ==c)
# 
#       file.name <- paste0(unique(substr(temp2$code_state,1,2)),"UF",".gpkg")
# 
#       # original
#       i <- paste0(dir.dest.file,file.name)
#       sf::st_write(temp2, i, overwrite=TRUE)
# 
#       # simplified
#       i <- gsub(".gpkg", "_simplified.gpkg", i)
#       sf::st_write(temp2_simplified, i, overwrite=TRUE)
#     }
# 
#   }
# }
# 
# # apply function in parallel
# future::plan(multisession)
# future_map(sub_dirs, clean_states)
# 
# rm(list= ls())
# gc(reset = T)
