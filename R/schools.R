#> DATASET:  Escolas - Anual
#> Source: INEP - https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/inep-data/catalogo-de-escolas
#> #> scale ******
#> Metadata:
# Título: Escolas
# Título alternativo: schools
# Frequência de atualização: Anual - ***manualmente***
#
# Forma de apresentação: CSV
# Linguagem: Pt-BR
# Character set: Utf-8
#
# Resumo: Pontos com coordenadas gegráficas das escolas do censo escolar
# Informações adicionais: Dados produzidos pelo INEP. Os dados de escolas e sua
# geolocalização são atualizados pelo INEP continuamente. Para finalidade do geobr,
# esses dados precisam ser baixados uma vez ao ano.
#
# Estado: Em desenvolvimento
# Palavras chaves descritivas:****
# Informação do Sistema de Referência: SIRGAS 2000

# Observações: 
# Anos disponíveis: ****************

### Libraries (use any library as necessary) ----

library(stringr)
library(sf)
library(janitor)
library(dplyr)
library(readr)
library(data.table)
library(magrittr)
library(lwgeom)
library(tidyverse)
library(arrow)
library(geoarrow)
source("./R/support_harmonize_geobr.R")
source("./R/support_fun.R")

# Download the data  ----
download_schools <- function(year){ # year = 2024
  
  ## 0. Set year ----
  
  year <- lubridate::year(Sys.Date())
  
  date_update <- paste0(lubridate::year(Sys.Date()), "_",
                        lubridate::month(Sys.Date()))
  
  ## 1. After manual download, bring the file to R ----
  
  #### manual download manual and standarize the collumns names
  # https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/inep-data/catalogo-de-escolas
  dt <- fread("./data_raw/schools/Análise - Tabela da lista das escolas - Detalhado.csv",
              encoding = 'UTF-8')
  
  ## 2. Test integrity ----
  glimpse(dt)
  
  ## 3. Show result ----
  
  schools_raw <- dt |> 
    clean_names()
  
  glimpse(schools_raw)
  
  return(schools_raw)
}

# Clean the data  ----
clean_schools <- function(schools_raw, year){ # year = 2025
  
  ## 0. Create folder to save clean data ----
  
  year <- year(Sys.Date())
  date_update <- paste0(lubridate::year(Sys.Date()), "_",
                        lubridate::month(Sys.Date())) 
  
  dir_clean <- paste0("./data/schools/", date_update)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. Rename collumns ----
  
  # names(schools_raw) <- c(abbrev_state, name_muni, code_school, name_school,
  #                         education_level, education_level_others,
  #                         admin_category, address, phone_number, 
  #                         government_level, private_school_type, 
  #                         private_government_partnership, 
  #                         regulated_education_council, service_restriction,
  #                         size, urban, location_type, date_update, y,  x)
  # 
  # glimpse(schools_raw)
  
  ## 2. Clean missing values ----
  
  # find points with missing coordinates
  head(schools_raw)
  schools_raw[is.na(latitude) | is.na(longitude),]
  schools_raw[latitude == 0,]
  
  # identify which points should have empty geo
  schools <- schools_raw |> 
    mutate(empty_geo = case_when(is.na(latitude) | is.na(longitude) ~ TRUE,
                            TRUE ~ NA))
  
  glimpse(schools)
  
     #df[codigo_inep=='11000180', x]
  
  
  # replace NAs with 0
  data.table::setnafill(schools,
                        type = "const",
                        fill = 0,
                        cols = c("latitude","longitude")
  )
  
  glimpse(schools)
  
  # convert to point empty
  # solution from: https://gis.stackexchange.com/questions/459239/how-to-set-a-geometry-to-na-empty-for-some-features-of-an-sf-dataframe-in-r
  #df$geometry[df$empty_geo == T] = sf::st_point()
  # 
  #   subset(temp_sf, code_school=='11000180')
  # 
  
  ## 3. Convert to geo spatial file ----
  
  # Convert originl data frame into sf
  df <- sf::st_as_sf(schools,
                     coords = c("latitude", "longitude"),
                     crs = "+proj=longlat +datum=WGS84")
  
  ## 4. Apply harmonize geobr cleaning ----
  
  temp_sf <- harmonize_geobr(
    temp_sf = df,
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
  
  ## 5. Create geocode br collum ----
  
  
  
  
  ## 6. lighter version ----
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 7. Save datasets  ----
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/schools_",  year,
  #                                   ".gpkg"), delete_dsn = TRUE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean, "/schools_",
  #                                               year, "_simplified.gpkg"),
  #              delete_dsn = TRUE )
  
  ### Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/schools_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 22
  )
  
  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean,"/schools_", year, "_simplified", ".parquet"),
    compression='zstd',
    compression_level = 22
  )
  
  ## 8. Create the files for geobr index  ----
  
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}


# Rafael Old code HERE #############

# update_schools <- function(){
# 
# 
#   # If the data set is updated regularly, you should create a function that will have
#   # a `date` argument download the data
#   update <- 2023
#   date_update <- Sys.Date()
# 
#   # date shown to geobr user
#   geobr_date <- gsub('-',  '' , date_update)
#   geobr_date <- substr(geobr_date, 1, 6)
# 
# 
#   # download manual
#   # https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/inep-data/catalogo-de-escolas
#   dt <- fread('C:/Users/r1701707/Downloads/Análise - Tabela da lista das escolas - Detalhado.csv',
#               encoding = 'UTF-8')
#   head(dt)
# 
# 
#   ##### 4. Rename columns -------------------------
#   head(dt)
# 
#   df <- dplyr::select(dt,
#                         abbrev_state = 'UF',
#                         name_muni = 'Município',
#                         code_school = 'Código INEP',
#                         name_school = 'Escola',
#                         education_level = 'Etapas e Modalidade de Ensino Oferecidas',
#                         education_level_others = 'Outras Ofertas Educacionais',
#                         admin_category = 'Categoria Administrativa',
#                         address = 'Endereço',
#                         phone_number = 'Telefone',
#                         government_level = 'Dependência Administrativa',
#                         private_school_type = 'Categoria Escola Privada',
#                         private_government_partnership = 'Conveniada Poder Público',
#                         regulated_education_council = 'Regulamentação pelo Conselho de Educação',
#                         service_restriction ='Restrição de Atendimento',
#                         size = 'Porte da Escola',
#                         urban = 'Localização',
#                         location_type = 'Localidade Diferenciada',
#                         date_update = 'date_update',
#                         y = 'Latitude',
#                         x = 'Longitude'
#           )
# 
# 
# 
# 
#   head(df)
# 
# 
#   # add update date columns
#   df[, date_update := as.character(date_update)]
# 
# 
#   # deal with points with missing coordinates
#   head(df)
#   df[is.na(x) | is.na(y),]
#   df[x==0,]
# 
#   # identify which points should have empty geo
#   df[is.na(x) | is.na(y), empty_geo := T]
# 
#   df[code_school=='11000180', x]
# 
# 
#   # replace NAs with 0
#   data.table::setnafill(df,
#                         type = "const",
#                         fill = 0,
#                         cols=c("x","y")
#   )
# 
# 
# 
#   # Convert originl data frame into sf
#   temp_sf <- sf::st_as_sf(x = df,
#                           coords = c("x", "y"),
#                           crs = "+proj=longlat +datum=WGS84")
# 
# 
#   # convert to point empty
#   # solution from: https://gis.stackexchange.com/questions/459239/how-to-set-a-geometry-to-na-empty-for-some-features-of-an-sf-dataframe-in-r
#   temp_sf$geometry[temp_sf$empty_geo == T] = sf::st_point()
# 
#   subset(temp_sf, code_school=='11000180')
# 
# 
#   # Change CRS to SIRGAS  Geodetic reference system "SIRGAS2000" , CRS(4674).
#   temp_sf <- harmonize_projection(temp_sf)
# 
# 
#   # create folder to save the data
#   dest_dir <- paste0('./data/schools/', update,'/')
#   dir.create(path = dest_dir, recursive = TRUE, showWarnings = FALSE)
# 
# 
#   # Save raw file in sf format
#   sf::st_write(temp_sf,
#                dsn= paste0(dest_dir, 'schools_', update,".gpkg"),
#                overwrite = TRUE,
#                append = FALSE,
#                delete_dsn = T,
#                delete_layer = T,
#                quiet = T
#   )
# 
# }
