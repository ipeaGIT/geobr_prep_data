#> DATASET: Brazilian legal amazon
#> Source: MMA - http://mapas.mma.gov.br/i3geo/datadownload.htm (caiu)
#> Fonte alternativa a se analizar: https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/amazonia_legal/
#> Metadata:
# Título: Amazônia Legal
# Título alternativo: Amazonia legal
# Frequência de atualização: Nunca
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Polígonos e Pontos da Amazônia Legal brasileira.
# Informações adicionais: Dados produzidos pelo MMA com base legal que consta no código florestal (lei 12.651).
# Propósito: Identificaçãao da Amazônia legal.
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas:****
# Informação do Sistema de Referência: SIRGAS 2000
# Observações: 2014 a 2020 sem shape. 2021 a 2024 com shape.

### Libraries (use any library as necessary) -----------------------------------

# library(arrow)
# library(geoarrow)
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
# library(tidyverse)
# library(rvest)
# source("./R/support_harmonize_geobr.R")
# source("./R/support_fun.R")

# Download the data  -----------------------------------------------------------
download_amazonialegal <- function(year){ #

  ## 0. Set up the download links (UPDATE YEAR) --------------------------------
  
  base_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/amazonia_legal/"
  
  if (year == 2019) {
    ftp_zip <- paste0(base_link, year,
                      "/lista_de_municipios_da_amazonia_legal_", year,
                      "_SHP.zip")
  }
  
  if (year == 2020) {
    ftp_zip <- paste0(base_link, year,
                      "/lista_de_municipios_da_Amazonia_Legal_", year,
                      "_SHP.zip")
  }
  
  if (year %in% c(2021, 2022, 2024)) {
    ftp_zip <- paste0(base_link, year,
                      "/Limites_Amazonia_Legal_", year,
                      "_shp.zip")
  }
  
  ## 1. Directions do download the file ----------------------------------------

  zip_dir <- paste0(tempdir(), "/amazonia_legal/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  ### zip folder
  in_zip <- paste0(zip_dir, "/zipped/")
  dir.create(in_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(in_zip)
  
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
 
  ## 2. Download and save in the temp directory --------------------------------
  
  file_raw <- fs::file_temp(tmp_dir = in_zip,
                            ext = fs::path_ext(ftp_zip))
  
  download.file(url = ftp_zip,
                destfile = file_raw)
  
  file.exists(file_raw)
  
  ## 3. Unzip the shape file ---------------------------------------------------
  
  unzip_geobr(zip_dir = zip_dir, in_zip = in_zip, out_zip = out_zip, is_shp = TRUE)
  
  ## 4. Read data --------------------------------------------------------------
  
  if (year %in% c(2019, 2020)) {
    # lista todos os .shp na pasta (busca recursiva opcional)
    shp_files <- list.files(out_zip, pattern = "^[Aa].*\\.shp$", full.names = TRUE)
    
    amazonialegal_raw <- sf::st_read(
      shp_files, 
      quiet = F, 
      stringsAsFactors=F
    )
  }
  
  if (year %in% c(2021, 2022, 2024)) {
    amazonialegal_raw <- sf::st_read(
      out_zip, 
      quiet = F, 
      stringsAsFactors=F
    )
  }
  
  ## 5. Show result ------------------------------------------------------------
  glimpse(amazonialegal_raw)
  
  return(amazonialegal_raw)
  
}
  
# Clean the data ---------------------------------------------------------------
clean_amazonialegal <- function(amazonialegal_raw, year){
  
  ## 0. create clean directory -------------------------------------------------
  
  #create directory
  dir_clean <- paste0("./data/amazonia_legal/", year)
  dir.create(dir_clean, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. rename column names ----------------------------------------------------
  
  # # Rename columns
  # amazonialegal_raw$GID0 <- NULL
  # amazonialegal_raw$ID1 <- NULL
  
  ## 2. Apply geobr cleaning ---------------------------------------------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = amazonialegal_raw,
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
  
  ## 3. generate a lighter version of the dataset with simplified borders ------
  
  # simplify
  temp_sf2 <- simplify_temp_sf(temp_sf)
  head(temp_sf2)
  
  ## 4. Clean data set and save it in geopackage format ------------------------
  
  #save original and simplified datasets
  # sf::st_write(temp_sf, append = FALSE, dsn = paste0(dir_clean, "amazonialegal", ".gpkg") )
  # sf::st_write(temp_sf2, append = FALSE, dsn = paste0(dir_clean, "amazonialegal","_simplified", ".gpkg"))
  
  
  ## 5. Save original and simplified datasets in parquet -----------------------
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/amazonialegal_", year, ".parquet"),
    compression='zstd',
    compression_level = 22
  )
  
  arrow::write_parquet(
    x = temp_sf2,
    sink = paste0(dir_clean, "/amazonialegal_", year, "_simplified", ".parquet"),
    compression='zstd',
    compression_level = 22
  )
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}




####### CECÍLIA OLD CODE BELOW (Because MMA lost the source of files) ####

# Download the data  ----
# download_amazonialegal <- function(){ # i've removed the year argument,
#   # because the file is atomic. Only have had one update.
#
# 
#   ## 0. Download and read into CSV at the same time ----
#   ftp_shp <- 'http://mapas.mma.gov.br/ms_tmp/amazlegal.shp'
#   ftp_shx <- 'http://mapas.mma.gov.br/ms_tmp/amazlegal.shx'
#   ftp_dbf <- 'http://mapas.mma.gov.br/ms_tmp/amazlegal.dbf'
#   ftp <- c(ftp_shp,ftp_shx,ftp_dbf)
# 
# 
#   ## 1. Directions do download the file ----
#   temp_dir <- fs::path_temp()
#   download_temp_dir <- paste0(temp_dir,"/amazonia_legal/")
#   dir.create(download_temp_dir, showWarnings = FALSE, recursive = TRUE)
#   dir.exists(download_temp_dir)
#   
#   for(i in 1:length(ftp)){
#     
#     file_name <- basename(ftp[i])
#     
#     temp_download <-  httr::GET(
#       url = ftp[i], 
#       httr::write_disk(path = paste0(download_temp_dir, file_name),
#                        overwrite = T)
#     )
#    }
#   
#   ## 2. Save in the temp directory ----
#   shp_file <- basename(ftp_shp)
#   shp_dir <- paste0(download_temp_dir, shp_file)
#   
#   ## 3. Read data
#   amazonialegal_raw <- sf::st_read(
#     shp_dir, 
#     quiet = F, 
#     stringsAsFactors=F
#   )
#   
#   return(amazonialegal_raw)
#   
#   }


# amazonialegal_raw <- targets::tar_read("amazonialegal_raw")

# # Clean the data ----
# clean_amazonialegal <- function(amazonialegal_raw){
#   
#   ## 0. create clean directory ----
#   
#   #create directory
#   dir_clean <- "./data/amazonia_legal/"
#   dir.create(dir_clean, showWarnings = FALSE)
#   dir.exists(dir_clean)
#    
# 
#   ## 1. rename column names ----
#   
#   # Rename columns
#   amazonialegal_raw$GID0 <- NULL
#   amazonialegal_raw$ID1 <- NULL
#   
#   
#   ## 2. Apply geobr cleaning ----
#   
#   temp_sf <- harmonize_geobr(
#     temp_sf = amazonialegal_raw, 
#     add_state = F, 
#     add_region = F, 
#     add_snake_case = F, 
#     #snake_colname = snake_colname,
#     projection_fix = T,
#     encoding_utf8 = F, 
#     topology_fix = T,
#     remove_z_dimension = T,
#     use_multipolygon = T
#   )
#   
#   glimpse(temp_sf)
#   
#   ## 3. generate a lighter version of the dataset with simplified borders ----
#   
#   # simplify
#   temp_sf2 <- simplify_temp_sf(temp_sf)
#   head(temp_sf2)
#   
#   
#   ## 4. Clean data set and save it in geopackage format ----
#   
#   #save original and simplified datasets
#   # sf::st_write(temp_sf, append = FALSE, dsn = paste0(dir_clean, "amazonialegal", ".gpkg") )
#   # sf::st_write(temp_sf2, append = FALSE, dsn = paste0(dir_clean, "amazonialegal","_simplified", ".gpkg"))
#   
#   
#   ## 5. Save original and simplified datasets in parquet ----
#   arrow::write_parquet(
#     x = temp_sf,
#     sink = paste0(dir_clean, "amazonialegal", ".parquet"),
#     compression='zstd',
#     compression_level = 22
#   )
#   
#   arrow::write_parquet(
#     x = temp_sf2,
#     sink = paste0(dir_clean, "amazonialegal","_simplified", ".parquet"),
#     compression='zstd',
#     compression_level = 22
#   )
#   
#   files <- list.files(path = dir_clean, 
#                       pattern = ".parquet", 
#                       recursive = TRUE, 
#                       full.names = TRUE)
#   
#   return(files)
# }

