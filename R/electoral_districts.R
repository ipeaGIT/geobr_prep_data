#> DATASET: electoral districts
#> Source: TSE  - ??????????????????
#> scale 1:250.000
#> Metadata: #####
# Título: Locais de votação e zonas eleitorais
# Título alternativo: pooling places
# Frequência de atualização: a cada 2 anos
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Pontos dos locais de votação do Brasil
# Informações adicionais: Dados de estados produzidos pelo TSE, e utilizados na elaboração do shape das regiões com a melhor base oficial disponível.
# Propósito: Disponibilização das fronteiras eleitorais do Brasil com base em geolocalização geocodebr
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas: ****
# Informação do Sistema de Referência: SIRGAS 2000
#
# Observações: 
# Anos disponíveis: 2010 a 2024

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
# library(geobr)
# library(geocodebr)
# source("./R/support_harmonize_geobr.R")
# source("./R/support_fun.R")

# Download the data  -----------------------------------------------------------
download_electoraldistricts <- function(year){ # year = 2024
  
  ## 0. Generate the correct ftp link ------------------------------------------

  url_start <- "https://cdn.tse.jus.br/estatistica/sead/odsele/eleitorado_locais_votacao/eleitorado_local_votacao"
  
  ftp_link <- paste0(url_start, "_", year, ".zip")
  
  ## 1. Create temp folder -----------------------------------------------------
  
  zip_dir <- paste0(tempdir(), "/electoral_districts/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  ### Alternative folder
  # zip_dir <- paste0("./data_raw/", "/electoral_districts/", year)
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
  
  httr::GET(url = ftp_link,
            httr::progress(),
            httr::write_disk(path = file_raw,
                             overwrite = T))

  ## 4. Unzip Raw data ---------------------------------------------------------
  
  unzip_geobr(zip_dir = zip_dir, in_zip = in_zip, out_zip = out_zip, is_shp = TRUE)
  
  ## 6. Bind Raw data together -------------------------------------------------
  
  csv_file <- list.files(path = out_zip, pattern = "csv", full.names = TRUE)
  
  electoraldistricts_raw <- fread(csv_file, encoding = "Latin-1", sep = ";") |> 
    clean_names()
  
  ## 7. Show result ------------------------------------------------------------
  
  glimpse(electoraldistricts_raw)
  
  return(electoraldistricts_raw)
  }

# Clean the data  --------------------------------------------------------------
clean_electoraldistricts <- function(electoraldistricts_raw, year){ # year = 2024
  
  ## 0. Create folder to save clean data ---------------------------------------
  
  dir_clean <- paste0("./data/electoral_districts/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. Prepare the data  ------------------------------------------------------
  
  glimpse(electoraldistricts_raw)
  
  ## 3. Add geocoding collumn and create a sf dataset --------------------------
  
  ## 4. Apply harmonize geobr cleaning -----------------------------------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = electoraldistricts,
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
  ## 5. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 6. Save datasets  ---------------------------------------------------------
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/electoraldistricts_",  year,
  #                                   ".gpkg"), delete_dsn = TRUE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean, "/electoraldistricts_",
  #                                               year, "_simplified.gpkg"),
  #              delete_dsn = TRUE )
  
  ### Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/electoraldistricts_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 7
  )
  
  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean,"/electoraldistricts_", year, "_simplified", ".parquet"),
    compression='zstd',
    compression_level = 7
  )
  
  ## 7. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}

