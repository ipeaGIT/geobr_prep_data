#> DATASET: conservation unit
#> Source: 
#> https://dados.gov.br/dados/conjuntos-dados/limites-oficiais-de-unidades-de-conservacao-federais
#> https://dados.mma.gov.br/dataset/unidadesdeconservacao
#> https://www.gov.br/icmbio/pt-br/assuntos/dados_geoespaciais/mapa-tematico-e-dados-geoestatisticos-das-unidades-de-conservacao-federais
#> Old source: MMA - http://mapas.mma.gov.br/i3geo/datadownload.htm
#> Metadata:
# Título: Unidades de Conservação
# Título alternativo:
# Data: Atualização ***
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: Latin1
#
# Resumo: Polígonos e Pontos das unidades de conservação brasileiras.
# Informações adicionais: Dados produzidos pelo MMA, e utilizados na elaboração do shape de biomas com a melhor base oficial disponível. Foi feito download dos dados tal como estão no segundo semestre de cada ano.
# Propósito: Identificação das unidades de conservação brasileiras.
#
# Estado: Em desenvolvimento
# Palavras chaves descritivas:****
# Informação do Sistema de Referência: SIRGAS 2000

### Libraries (use any library as necessary) -----------------------------------

# library(RCurl)
# library(stringr)
# library(sf)
# library(janitor)
# library(dplyr)
# library(readr)
# library(data.table)
# library(magrittr)
# library(devtools)
# library(lwgeom)
# library(stringi)
# library(targets)
# library(tidyverse)
# library(mirai)
# library(rvest)
# library(arrow)
# library(geoarrow)
# source("./R/support_harmonize_geobr.R")
# source("./R/support_fun.R")

# Download the data  -----------------------------------------------------------
download_conservationunits <- function(year){ # year = 2024
  
  ## 0. Generate the correct ftp link (UPDATE YEAR HERE) -----------------------
  
  ftp_start <- "https://dados.mma.gov.br/dataset/44b6dc8a-dc82-4a84-8d95-1b0da7c85dac/resource/"
  
  if(year == 2024) {
    ftp_link <- paste0(ftp_start,
                       "9ec98f66-44ad-4397-8583-a1d9cc3a9835/download/shp_cnuc_2024_02.zip")
    
  }
  
  if(year == 2025) {
    ftp_link <- paste0(ftp_start, 
                       "6ba9a557-87e8-4882-acb7-b3e0f0ea192d/download/shp_cnuc_2025_08.zip")
  }

  ## 1. Create temp folder -----------------------------------------------------
  
  zip_dir <- paste0(tempdir(), "/conservation_units/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  ### Alternative folder
  # zip_dir <- paste0("./data_raw/", "/conservation_units/", year)
  # dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  # dir.exists(zip_dir)
  
  ## 2. Create direction for each download -------------------------------------
  
  # zip folder
  in_zip <- paste0(zip_dir, "/zipped/")
  dir.create(in_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(in_zip)
  
  file_raw <- fs::file_temp(tmp_dir = in_zip,
                            ext = fs::path_ext(ftp_link))
  
  #filenames <- basename(ftp_link)
  
  ## 3. Download Raw data ------------------------------------------------------
  
  httr::GET(url = ftp_link,
            httr::progress(),
            httr::write_disk(path = file_raw,
                             overwrite = T))
  
  ## 4. Unzip Raw data ---------------------------------------------------------
  
  ### unzip folder
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  unzip_geobr(zip_dir = zip_dir, in_zip = in_zip, out_zip = out_zip, is_shp = TRUE)
  
  ## 5. Check files ------------------------------------------------------------
  
  list.files(out_zip)
  
  shp_names <- list.files(out_zip, pattern = "\\.shp$",
                          full.names = TRUE) |> 
    str_subset(pattern = "pontos", negate = TRUE) # Deny files with "pontos"
  
  ## 6. Bind Raw data together -------------------------------------------------
  
  conservationunits_list <- pbapply::pblapply(
    X = shp_names, 
    FUN = function(x){ sf::st_read(x, quiet = T, stringsAsFactors= F) }
  )
  
  conservationunits_raw <- data.table::rbindlist(conservationunits_list)
  
  ## 7. Show result ------------------------------------------------------------
  
  data.table::setDF(conservationunits_raw)
  conservationunits_raw <- sf::st_as_sf(conservationunits_raw)
  
  glimpse(conservationunits_raw)
  
  return(conservationunits_raw)
  
}

# Clean the data  --------------------------------------------------------------
clean_conservationunits <- function(conservationunits_raw, year){ # year = 2024
  
  ## 0. Create folder to save clean data ---------------------------------------
  
  dir_clean <- paste0("./data/conservation_units/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. Rename columns and remove collumns -------------------------------------
  
  glimpse(conservationunits_raw)
  
  if (year == 2025) {
    conservationunits_raw <- subset(conservationunits_raw, select = -c(docleg_id))
  }
  
  conservationunits <- conservationunits_raw |> 
    clean_names() |> 
    select(-gml_id,
           -situacao,
           code_uc_last = uc_id,
           code_conservation_unit = cd_cnuc,
           code_wdpa = wdpa_pid,
           name_conservation_unit = nome_uc,
           creation_year = cria_ano,
           legislation = cria_ato,
           outro_ato,
           desc_manejo = pl_manejo,
           conselho_gestor = co_gestor,
           quality = quali_pol, 
           ppgr,
           ha_total,
           ha_ato, 
           government_level = esfera,
           name_states = uf,
           name_munis = municipio,
           name_organization = org_gestor,
           group_manejo = grupo,
           category_conservation_unit = categoria,
           category_iucn = cat_iucn,
           amazonia,
           caatinga, 
           cerrado, 
           matlantica, 
           pampa, 
           pantanal, 
           marinho, 
           limite, 
           geometry) 
           
  glimpse(conservationunits)
  
  ## 2. Apply harmonize geobr cleaning -----------------------------------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = conservationunits,
    year = year,
    add_state = F,
    add_region = F,
    add_snake_case = F,
    #snake_colname = snake_colname,
    projection_fix = T,
    encoding_utf8 = T,
    topology_fix = T,
    remove_z_dimension = T,
    use_multipolygon = F
  )
  
  glimpse(temp_sf)
  
  ## 3. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 4. Save datasets  ---------------------------------------------------------
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/conservationunits_",  year,
  #                                    ".gpkg"), delete_dsn = TRUE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean,
  #                                               "/conservationunits_",
  #                                               year, "_simplified.gpkg"),
  #              delete_dsn = TRUE )
  
  # Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/conservation_units_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 7
  )
  
  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean,"/conservation_units_", year, "_simplified",
                  ".parquet"),
    compression='zstd',
    compression_level = 7
  )
  
  
  ## 5. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}

################ RAFAEL OLD CODE BELOW HERE ############# ---------------------
# 
# 
# ####### Load Support functions to use in the preprocessing of the data
# 
# source("./prep_data/prep_functions.R")
# 
# 
# 
# 
# # If the data set is updated regularly, you should create a function that will have
# # a `date` argument download the data
# 
# update <- 201909
# 
# 
# 
# 
# 
# 
# 
# 
# getwd()
# 
# 
# ###### 0. Create Root folder to save the data -----------------
# # Root directory
# root_dir <- "L:/# DIRUR #/ASMEQ/geobr/data-raw"
# setwd(root_dir)
# 
# # Directory to keep raw zipped files
# dir.create("./conservation_units")
# destdir_raw <- paste0("./conservation_units/",update)
# dir.create(destdir_raw)
# 
# 
# # Create folders to save clean sf.rds files  -----------------
# dir.create("./conservation_units/shapes_in_sf_cleaned", showWarnings = FALSE)
# destdir_clean <- paste0("./conservation_units/shapes_in_sf_cleaned/",update)
# #dir.create(destdir_clean)
# 
# 
# 
# 
# 
# #### 1. Download original data sets from MMA website -----------------
# 
# # Download and read into CSV at the same time
# ftp <- 'http://mapas.mma.gov.br/ms_tmp/ucstodas.shp'
# 
# download.file(url = ftp,
#               destfile = paste0(destdir_raw,"/","ucstodas.shp"),mode = "wb") #mode = "wb" resolve o problema na hora de baixar o arquivo
# 
# ftp <- 'http://mapas.mma.gov.br/ms_tmp/ucstodas.shx'
# download.file(url = ftp,
#               destfile = paste0(destdir_raw,"/","ucstodas.shx"),mode = "wb")
# 
# ftp <- 'http://mapas.mma.gov.br/ms_tmp/ucstodas.dbf'
# download.file(url = ftp,
#               destfile = paste0(destdir_raw,"/","ucstodas.dbf"),mode = "wb")
# 
# 
# 
# 
# 
# 
# #### 2. Unzipe shape files -----------------
# # unecessary
# 
# 
# 
# 
# #### 3. Clean data set and save it in compact .rds format-----------------
# 
# # Root directory
# setwd('./conservation_units')
# 
# 
# # list all csv files
# shape <- list.files(path=paste0("./",update), full.names = T, pattern = ".shp$") # $ para indicar que o nome termina com .shp pois existe outro arquivo com .shp no nome
# 
# # read data
# temp_sf <- st_read(shape, quiet = F, stringsAsFactors=F, options = "ENCODING=latin1") #Encoding usado pelo IBGE (ISO-8859-1) usa-se latin1 para ler acentos
# head(temp_sf)
# 
# # add download date column
# temp_sf$date <- update
# 
# # Rename columns
# temp_sf <- dplyr::rename(temp_sf,
#                          code_conservation_unit = ID_UC0,
#                          name_conservation_unit = NOME_UC1,
#                          id_wcm = ID_WCMC2,
#                          category = CATEGORI3,
#                          group = GRUPO4,
#                          government_level = ESFERA5,
#                          creation_year = ANO_CRIA6,
#                          gid7 = GID7,
#                          quality = QUALIDAD8,
#                          code_u111 = CODIGO_U11,
#                          legislation = ATO_LEGA9,
#                          name_organization = NOME_ORG12,
#                          dt_ultim10 = DT_ULTIM10)
# head(temp_sf)
# 
# 
# # store original CRS
# original_crs <- st_crs(temp_sf)
# 
# # # Use UTF-8 encoding
# # temp_sf$name_state <- stringi::stri_encode(as.character((temp_sf$name_state), "UTF-8"))
# 
# 
# # Harmonize spatial projection CRS, using SIRGAS 2000 epsg (SRID): 4674
# temp_sf <- harmonize_projection(temp_sf)
# 
# 
# # Make any invalid geometry valid # st_is_valid( sf)
# temp_sf <- lwgeom::st_make_valid(temp_sf)
# 
# 
# # Make sure all geometry types are MULTIPOLYGON (fix isse #66)
# temp_sf <- sf::st_cast(temp_sf, "MULTIPOLYGON")
# unique(sf::st_geometry_type(temp_sf)) # [1] MULTIPOLYGON       GEOMETRYCOLLECTION
# 
# 
# # Use UTF-8 encoding in all character columns
# temp_sf <- temp_sf %>%
#   mutate_if(is.factor, function(x){ x %>% as.character() %>%
#       stringi::stri_encode("UTF-8") } )
# temp_sf <- temp_sf %>%
#   mutate_if(is.factor, function(x){ x %>% as.character() %>%
#       stringi::stri_encode("UTF-8") } )
# 
# 
# 
# ###### convert to MULTIPOLYGON -----------------
# temp_sf <- to_multipolygon(temp_sf)
# 
# ###### 7. generate a lighter version of the dataset with simplified borders -----------------
# # skip this step if the dataset is made of points, regular spatial grids or rater data
# 
# # simplify
# temp_sf7 <- st_transform(temp_sf, crs=3857) %>%
#   sf::st_simplify(preserveTopology = T, dTolerance = 100) %>% st_transform(crs=4674)
# head(temp_sf7)
# 
# 
# 
# 
# ###### 8. Clean data set and save it in geopackage format-----------------
# setwd(root_dir)
# 
# 
# 
# # Save cleaned sf in the cleaned directory
# readr::write_rds(temp_sf, path= paste0(destdir_clean,'/conservation_units_', update,'.rds'), compress = "gz")
# sf::st_write(temp_sf, dsn= paste0(destdir_clean,"/conservation_units_", update,".gpkg") )
# sf::st_write(temp_sf7, dsn= paste0(destdir_clean,"/conservation_units_", update," _simplified", ".gpkg"))
# 
