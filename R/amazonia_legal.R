#> DATASET: Brazilian legal amazon
#> Source: MMA - http://mapas.mma.gov.br/i3geo/datadownload.htm (caiu)
#> Fonte alternativa a se analizar: https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/amazonia_legal/
#> Metadata:
# Título: Amazônia Legal
# Título alternativo: Amazonia legal
# Frequência de atualização: ???
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
download_amazonialegal <- function(year){ #

  ## 0. Set up the download links (UPDATE YEAR) --------------------------------
  
  if (year %in% c(2019:2024)) {
    
    base_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/amazonia_legal/"
    
    if (year == 2019) {
      ftp_link <- paste0(base_link, year,
                        "/lista_de_municipios_da_amazonia_legal_", year,
                        "_SHP.zip")
    }
    
    if (year == 2020) {
      ftp_link <- paste0(base_link, year,
                        "/lista_de_municipios_da_Amazonia_Legal_", year,
                        "_SHP.zip")
    }
    
    if (year %in% c(2021, 2022, 2024)) {
      ftp_link <- paste0(base_link, year,
                        "/Limites_Amazonia_Legal_", year,
                        "_shp.zip")
    }
  }
  
  ## 1. Directions do download the file ----------------------------------------

  zip_dir <- paste0(tempdir(), "/amazonia_legal/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
 
  ## 2. Download and save in the temp directory --------------------------------
  
  if (year %in% c(2019:2024)) {
    file_raw <- fs::file_temp(tmp_dir = zip_dir,
                              ext = fs::path_ext(ftp_link))
    
    file_raw <- download_file_geobr(
      file_url = ftp_link,
      dest_dir = zip_dir
    )
    
    file.exists(file_raw)
  }
  
  ## 3. Unzip the shape file ---------------------------------------------------
  
  files <- unzip_geobr(zip_dir = zip_dir, out_zip = out_zip)
  
  # tres tipos de arquivo. no momento, lemos apenas o poligono geral
  # Amazonia_Legal_2019.shp
  # Mun_Amazonia_Legal_2019.shp
  # Sede_Mun_Amazonia_Legal_2019.shp
  
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
  
  amazonialegal_raw$year <- year
  
  return(amazonialegal_raw)
  
}
  
# Clean the data ---------------------------------------------------------------

# amazonialegal_raw <- tar_read("amazonialegal_raw", branches = 1)

clean_amazonialegal <- function(amazonialegal_raw){
  
  ## 0. create clean directory -------------------------------------------------
  
  #create directory
  yyyy <- amazonialegal_raw$year[1]
  dir_clean <- paste0("./data/amazonia_legal/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)
  
  ## 1. Dissolve municipalities for 2019/2020 ----------------------------------

  if (yyyy %in% c(2019, 2020)) {
    amazonialegal_raw <- dissolve_polygons_no_split(
        mysf = amazonialegal_raw, 
        group_column = "year"
      )
    
    sf::st_geometry(amazonialegal_raw) <- "geometry"
  }

  ## 2. Apply geobr cleaning ---------------------------------------------------

  temp_sf <- harmonize_geobr(
    temp_sf = amazonialegal_raw,
    year = yyyy,
    add_state = F,
    add_region = F,
    add_snake_case = F,
    projection_fix = T,
    encoding_utf8 = T,
    topology_fix = T,
    remove_z_dimension = T,
    use_multipolygon = T
  )


  ## 4. Select columns and validate --------------------------------------------

  temp_sf <- temp_sf |>
    dplyr::select(year, geometry)
  
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")

  ## 4. Lighter version --------------------------------------------------------

  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 500)


  
  ## 6. Save original and simplified datasets in parquet -----------------------
  write_geobr_parquet(
    temp_sf,
    paste0(dir_clean, "/amazonialegal_", yyyy, ".parquet"))

  write_geobr_parquet(
    temp_sf_simplified,
    paste0(dir_clean, "/amazonialegal_", yyyy, "_simplified", ".parquet"))
  
  ## 7. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}



