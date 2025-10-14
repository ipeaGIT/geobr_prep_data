#> DATASET: Brazilian semi-arid
#> Source: IBGE - https://www.ibge.gov.br/geociencias/cartas-e-mapas/mapas-regionais/15974-semiarido-brasileiro.html?=&t=downloads
#> Metadata:
# Título: Semiárido brasileiro
# Título alternativo: Semiarido brasileiro
# Frequência de atualização: Ocasional
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Polígonos e Pontos do semiárido brasileiro.
# Informações adicionais: Dados produzidos pelo IBGE com base em decretos administrativos do Ministério da Integração Nacional.
# -"Resolução nº 115 do Ministério da Integração Nacional, de 23 de novembro de 2017"
# -"Portaria N°89 de 16 de março de 2005, do Ministério da Integração Nacional"
# Propósito: Identificação do clima semiárido brasileiro.

# Estado: Em desenvolvimento
# Palavras-chaves descritivas:****
# Informação do Sistema de Referência: SIRGAS 2000

### Libraries (use any library as necessary) ----

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

# Download the data  ----
download_semiarid <- function(year){ # year = 2022

  ## 0. Get ftp link from source website ----

  # get correct ftp url link

  if(year == 2005) {
    ftp <- 'https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/semiarido_brasileiro/Situacao_2005a2017/lista_municipios_semiarido.xls'
  }

  if (year == 2017) {
    ftp <- 'https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/semiarido_brasileiro/Situacao_23nov2017/lista_municipios_Semiarido_2017_11_23.xlsx'
  }

  if (year == 2021) {
    ftp <- 'https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/semiarido_brasileiro/Situacao_2021/lista_municipios_Semiarido_2021.xls'
  }

  if (year == 2022) {
    ftp <- 'https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/semiarido_brasileiro/Situacao_2022/lista_municipios_Semiarido_2022.xlsx'
  }

  ## 1. Create temp folder ----
  
  #folder_geobr(folder_name = "semiarid", temp = TRUE)
  
  zip_dir <- paste0(tempdir(), "/semiarid/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  # zip folder
  in_zip <- paste0(zip_dir, "/zipped/")
  dir.create(in_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(in_zip)
  
  # directions to download file
  # file_raw <- paste0(dir_raw,"/", year, "_", basename(ftp))
  file_raw <-fs::file_temp(tmp_dir = in_zip,
                           ext = fs::path_ext(ftp))

  ## 2. Download  ----
  httr::GET(url = ftp,
            httr::progress(),
            httr::write_disk(path = file_raw,
                             overwrite = T))

  if (year == 2005){
    # read IBGE data frame
    munis_semiarid <- readxl::read_xls(path = file_raw,
                                       skip = 1, n_max = 1133)
    # Rename columns
    munis_semiarid <- dplyr::select(munis_semiarid,
                                    code_muni = `Código do Município`,
                                    name_muni = `Nome do Município`)
  }
  
  if (year == 2017){
    # read IBGE data frame
    munis_semiarid <- readxl::read_xlsx(path = file_raw,
                                        skip = 1, n_max = 1262)
    
    # Rename columns
    munis_semiarid <- dplyr::select(munis_semiarid,
                                    code_muni = `Código do Município`,
                                    name_muni = `Nome do Município`)
  }
  
  if (year == 2021) {
    # read IBGE data frame
    munis_semiarid <- readxl::read_xls(path = file_raw,
                                       n_max = 1263)
    # Rename columns
    munis_semiarid <- dplyr::select(munis_semiarid,
                                    code_muni = CD_MUN,
                                    name_muni = NM_MUN)
  }
  
  if (year == 2022) {
    # read IBGE data frame
    munis_semiarid <- readxl::read_xlsx(path = file_raw,
                                        n_max = 1477)
    # Rename columns
    munis_semiarid <- dplyr::select(munis_semiarid,
                                    code_muni = CD_MUN,
                                    name_muni = NM_MUN)
  }

  ## 3. Show result  ----
  
  munis_semiarid <- munis_semiarid |>
    mutate(year = year)
  
  glimpse(munis_semiarid)
  
  return(munis_semiarid)
}

# munis_semiarid <- tar_read("semiarid_raw", branches = 3)
# year <- tar_read("years_semiarid")[3]

# Clean the data   ----
clean_semiarid <- function(munis_semiarid, year) { 
  
  
  # #### 0. Create folders to save clean sf files  -----------------
  # dir_clean <- paste0("./data/semiarid/", year)
  # dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  
  
  #### 2. Clean data set -----------------

  # load all munis sf
  all_munis <- geobr::read_municipality(code_muni = 'all',
                                        year = year,
                                        simplified = FALSE)
  
  # if download fails, try again
  if(is.null(all_munis)){
    all_munis <- geobr::read_municipality(code_muni = 'all',
                                          year = year,
                                          simplified = FALSE)
  }
  
  # subset municipalities
  temp_sf <- subset(all_munis, code_muni %in% munis_semiarid$code_muni)
  
  #### 3. Apply geobr cleaning -----------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = temp_sf, 
    add_state = F, 
    add_region = F, 
    add_snake_case = F, 
    #snake_colname = snake_colname,
    projection_fix = T, 
    encoding_utf8 = F, 
    topology_fix = T, 
    remove_z_dimension = F,
    use_multipolygon = F
  )
  
  # glimpse(temp_sf)
  


  #### 4. lighter version --------------- 
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  #### 3. Save data set -----------------
  # sf::st_write(temp_sf, dsn= paste0(dir_clean,"/semiarid_", year, ".gpkg"), delete_dsn=TRUE)
  # sf::st_write(temp_sf_simplified, dsn= paste0(dir_clean,"/semiarid_", year, "_simplified.gpkg"), delete_dsn=TRUE )
  # 
  # Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean,"/semiarid_", year, ".parquet"),
    compression='zstd',
    compression_level = 22
  )

  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean,"/semiarid_", year, "_simplified", ".parquet"),
    compression='zstd',
    compression_level = 22
  )
  
  return(dir_clean)
}
