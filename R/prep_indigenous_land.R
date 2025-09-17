#> DATASET: indigenous Lands
#> Source: FUNAI - http://www.funai.gov.br/index.php/shape
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



# ####### Load Support functions to use in the preprocessing of the data -----------------
# source("./prep_data/prep_functions.R")
# 
# 
# # To Update the data, input the date YYYYMM and run the code
# 
# update <- 201909
# update <- 202103
# 
# library(RCurl)
# library(stringr)
# library(sf)
# library(dplyr)
# library(readr)
# library(data.table)
# library(magrittr)
# library(lwgeom)
# library(stringi)
# 
# 
# 
# # Create folders to save clean sf.rds files  -----------------
#   dir.create("./indigenous_land/shapes_in_sf_all_years_cleaned", showWarnings = FALSE)
#   destdir_clean <- paste0("./indigenous_land/shapes_in_sf_all_years_cleaned/",update)
#   dir.create(destdir_clean)
# 
# 
# 
# 
# 
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



