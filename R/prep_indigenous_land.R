#> DATASET: indigenous Lands
#> Source: FUNAI - http://www.funai.gov.br/index.php/shape #not working
#> Source: "https://www.gov.br/funai/pt-br/atuacao/terras-indigenas/geoprocessamento-e-mapas #not shp file
#> Source: Portal de mapas: "https://mapas2.funai.gov.br/portal_mapas/"
#> Metadata:
# Titulo: Terras Indígenas  / Terras Indígenas em Estudos
# Titulo alternativo: Terras Indígenas
# Data: Atualização Mensal
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: Utf-8
#
# Resumo: Polígonos e Pontos das terras indígenas brasileiras.
# Informações adicionais: Dados produzidos pela FUNAI, e utilizados na elaboração do shape de terras indígenas com a melhor base oficial disponível.
# Propósito: Identificação das terras indígenas brasileiras.
#
# Estado: Completado
# Palavras chaves descritivas:Terras Indígenas, Áreas Indígenas do Brasil, Áreas Indígenas, FUNAI, Ministério da Justiça (tema).
# Informação do Sistema de Referência: SIRGAS 2000


####### Load Support functions  -----------------

# library(RCurl)
# library(stringr)
# library(sf)
# library(dplyr)
# library(readr)
# library(tidyverse)
# library(data.table)
# library(magrittr)
# library(lwgeom)
# library(stringi)
# library(lubridate)
# library(arrow)
# library(geoarrow)
# library(rvest)
# library(purrr)

####### Download the data  -----------------
download_indigenousland <- function(year){ # year == 2025

  ###### 0. Create temp folders for download -----------------
  
  zip_dir <- paste0(tempdir(), "/indigenous_lands/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  # zipped folder
  in_zip <- paste0(zip_dir, "/zipped/")
  dir.create(in_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(in_zip)
  
  # unzip folder
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  ###### 1. Get the correct url and file names -----------------  
 
  # Portal de mapas
  # url <- "https://mapas2.funai.gov.br/portal_mapas"
  
  # # If the date is "2000"
  # if(year == 2000) {
  #   
  #   url <- "https://mapas2.funai.gov.br/portal_mapas/ti_sirgas/"
  #   page <- read_html(url)
  #   filenames <- page %>%
  #     html_nodes("a") %>%
  #     html_attr("href") %>%
  #     grep("2000", ., value = TRUE)
  #   #filenames <- paste0(url, filenames)
  # }
  
  # Last update from 2024
  if(year == 2024) {
    url <- "https://mapas2.funai.gov.br/portal_mapas/ti_sirgas_20240313.zip"
    filenames <- basename(url)
  }
  
  # Last update from 2025
  if(year == 2025) {
    url <- "https://mapas2.funai.gov.br/portal_mapas/ti_sirgas.zip"
    filenames <- basename(url)
  }
  
  ###### 2. Create temp file -----------------  
  
  # # the year 2000 is not zipped
  # if(year %in% c(2024, 2025)) {
  #   file_raw <- fs::file_temp(tmp_dir = zip_dir,
  #                             ext = fs::path_ext(url))
  # }
  
  ###### 3. Download the raw data -----------------  
  
  # if(year == 2000) {
  #   # Download files directly
  #   for (name_file in filenames) {
  #     download.file(paste(url, name_file, sep = ""),
  #                   paste(out_zip, name_file, sep = "\\"))
  #   }
  # }
  
  # If the date is 2024 or 2025
  if(year %in% c(2024, 2025)) {
    
    download.file(url, paste(in_zip, filenames, sep = "\\"))
  }
  
  ###### 4. Unzip Raw data -----------------
  
  # directory of zips
  zip_names <- list.files(in_zip, pattern = "\\.zip", full.names = TRUE)

  # unzip files

  if(year %in% c(2024, 2025)) {
    unzip(zipfile = zip_names,
          exdir = out_zip)
  }
  
  ###### 5. Read the file -----------------

  shp_names <- list.files(out_zip, pattern = "\\.shp$", full.names = TRUE)

  indigenousland_list <- pbapply::pblapply(
    X = shp_names,
    FUN = function(x){ sf::st_read(x, quiet = T, stringsAsFactors=F) }
  )

  indigenousland_raw <- data.table::rbindlist(indigenousland_list)
  data.table::setDF(indigenousland_raw)
  indigenousland_raw <- sf::st_as_sf(indigenousland_raw)
  
  ###### 6. Show result -----------------
  
  glimpse(indigenousland_raw)
  
  return(indigenousland_raw)
  }

  
  
  ###### 3. LINKS TO TEST HERE -----------------  
  
  #ftp <-  "https://mapas2.funai.gov.br/portal_mapas/shapes/ti_sirgas.zip" # NOT WORKING
  
  # ftp_link <- "https://geoserver.funai.gov.br/geoserver/Funai/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=Funai%3Atis_poligonais&maxFeatures=10000&outputFormat=SHAPE-ZIP"
  # 
  # url <- "https://mapas2.funai.gov.br/portal_mapas/"
  # 
  # url <- "https://www.gov.br/funai/pt-br/atuacao/terras-indigenas/geoprocessamento-e-mapas"
  # 
  # Most recent
  


# Clean the data ----------------------------------
clean_indigenousland <- function(indigenousland_raw, year) {

###### 0. Create folder to save clean data -----

  dir_clean <- paste0("./data/indigenous_land/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)

###### 1. Preparation -----------------

indigenousland_raw <- janitor::clean_names(indigenousland_raw)

###### 2. Apply harmonize geobr cleaning -----------------

temp_sf <- harmonize_geobr(
  temp_sf = indigenousland_raw,
  add_state = F,
  add_region = F,
  add_snake_case = F,
  #snake_colname = snake_colname,
  projection_fix = T,
  encoding_utf8 = T,
  topology_fix = T,
  remove_z_dimension = T,
  use_multipolygon = T
)

glimpse(temp_sf)

###### 3. Save results  -----------------

#sf::st_write(temp_sf, dsn= paste0(dir_clean,"/indigenousland_", year, ".gpkg"), delete_dsn=TRUE)

# Save in parquet
arrow::write_parquet(
  x = temp_sf,
  sink = paste0(dir_clean,"/indigenousland_", year, ".parquet"),
  compression='zstd',
  compression_level = 22
)

return(dir_clean)
}


# RAPHAEL OLD CODE BELOW ---------------


# #### 1. Download original data sets from FUNAI website -----------------
# 
# # Download and read into CSV at the same time
#   # ftp <- "http://mapas2.funai.gov.br/portal_mapas/shapes/ti_sirgas.zip"
#   # download.file(url = ftp,
#   #               destfile = paste0(destdir_raw,"/","indigenous_land.zip"))
#   ftp <-  "https://mapas2.funai.gov.br/portal_mapas/"
#   # ver arquvos antigos
# 
# # pelo menos desde 202103, download parece q tem q ser manual
#   #: https://www.gov.br/funai/pt-br/atuacao/terras-indigenas/geoprocessamento-e-mapas
# 
# 
#   d <- list_folders(ftp)
# 
# #### 2. Unzipe shape files -----------------
#   setwd(destdir_raw)
# 
#   # list and unzip zipped files
#   zipfiles <- list.files(pattern = ".zip")
#   unzip(zipfiles)
# 
# 
# 
# 
# 
# 
# 
# 
# #### 3. Clean data set and save it in compact .rds format-----------------
# 
# 
# # list all csv files
#   shape <- list.files(path=paste0("./",update), full.names = T, pattern = ".shp")
# 
# # read data
#   temp_sf <- st_read(shape, quiet = F, stringsAsFactors=F, options = "ENCODING=UTF8")
#   head(temp_sf)
# 
# # Rename columns
#   temp_sf <- dplyr::rename(temp_sf,
#                            abbrev_state = uf_sigla,
#                            name_muni = municipio_,
#                            code_terrai= terrai_cod)
#   head(temp_sf)
# 
# 
# # store original CRS
#   original_crs <- st_crs(temp_sf)
# 
# # Create columns with date and with state codes
#   setDT(temp_sf)[, date := update]
# 
# # Create column with state abbreviations
# 
# # Use UTF-8 encoding
#   temp_sf <- use_encoding_utf8(temp_sf)
# 
# 
# # Convert data.table back into sf
#   temp_sf <- st_as_sf(temp_sf, crs=original_crs)
# 
# 
# # Harmonize spatial projection CRS, using SIRGAS 2000 epsg (SRID): 4674
#   temp_sf <- harmonize_projection(temp_sf)
# 
# 
# # Make any invalid geometry valid # st_is_valid( sf)
#   temp_sf <- sf::st_make_valid(temp_sf)
# 
# 
# 
# 
#   # simplify
#   temp_sf_simplified <- simplify_temp_sf(temp_sf)
# 
#   # convert to MULTIPOLYGON
#   temp_sf <- to_multipolygon(temp_sf)
#   temp_sf_simplified <- to_multipolygon(temp_sf_simplified)
# 
# 
# 
# 
# # Save cleaned sf in the cleaned directory
#   sf::st_write(temp_sf, dsn = paste0("./shapes_in_sf_all_years_cleaned/",update,"/indigenous_land_", update,".gpkg") )
#   sf::st_write(temp_sf_simplified, dsn = paste0("./shapes_in_sf_all_years_cleaned/",update,"/indigenous_land_", update,"_simplified.gpkg") )
# 



