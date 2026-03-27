#> DATASET: pooling places
#> Source: TSE  - ??????????????????
#> scale 1:250.000
#> Metadata: #####
# Título: Locais de votação
# Título alternativo: electoral places
# Frequência de atualização: a cada 2 anos
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Pontos dos locais de votação do Brasil
# Informações adicionais: Dados de estados produzidos pelo TSE, e utilizados na elaboração do shape das regiões com a melhor base oficial disponível com base nos dados abertos de eleitorado.
# Propósito: Disponibilização dos pontos de locais de votação do Brasil com base em geolocalização geocodebr
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
download_poolingplaces <- function(year){ # year = 2024
  
  ## 0. Generate the correct ftp link ------------------------------------------

  # Error conditions
  if(year < 2010){
    stop("Geobr: Polling place data is available for all general and local elections starting in 2010.")}
  if(year %% 2 != 0){
    stop("Geobr: Please, review the year input. Electoral data is available even-numbered years.")
    }
  
  url_start <- "https://cdn.tse.jus.br/estatistica/sead/odsele/eleitorado_locais_votacao/eleitorado_local_votacao"
  
  ftp_link <- paste0(url_start, "_", year, ".zip")
  
  ## 1. Create temp folder -----------------------------------------------------
  
  zip_dir <- paste0(tempdir(), "/pooling_places/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  ### Alternative folder
  # zip_dir <- paste0("./data_raw/", "/pooling_places/", year)
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
  
  ## 5. Bind Raw data together -------------------------------------------------
  
  csv_file <- list.files(path = out_zip, pattern = "csv", full.names = TRUE)
  
  poolingplaces_raw <- fread(csv_file, #colClasses = rep("character", 41),
                             encoding = "Latin-1", sep = ";") |>
    mutate(NR_LONGITUDE = ifelse(NR_LONGITUDE == "-1", NA, NR_LONGITUDE),
           NR_LATITUDE  = ifelse(NR_LATITUDE  == "-1", NA, NR_LATITUDE),
           NR_SECAO_PRINCIPAL  = ifelse(NR_SECAO_PRINCIPAL  == "-1", NA,
                                        NR_SECAO_PRINCIPAL),
           NR_CEP  = ifelse(NR_CEP  == "0", NA, NR_CEP),
           NR_TELEFONE_LOCAL  = ifelse(NR_TELEFONE_LOCAL  == "-1", NA,
                                       NR_TELEFONE_LOCAL)) |> 
    clean_names()
  
  # Remove downloaded files
  file.remove(file_raw)
  file.exists(file_raw)
  
  ## 6. Show result ------------------------------------------------------------
  
  tibble(nome_coluna = names(poolingplaces_raw),
         classe_coluna = sapply(poolingplaces_raw, class))
  
  test <- table_collumns$classe
  
  test
  table_collumns <- tibble(ordem = 1:ncol(poolingplaces_raw),
                           nome_coluna = names(poolingplaces_raw),
                           classe = sapply(poolingplaces_raw, class),
                           n_unicos = sapply(poolingplaces_raw,
                                             function(x) n_distinct(x, na.rm = TRUE)),
                           n_na = colSums(is.na(poolingplaces_raw)),
                           exemplos = sapply(poolingplaces_raw,
                                             function(x) paste(head(unique(na.omit(x)), 6),
                                                               collapse = "; "))) 
    
    table_collumns$classe
  
  glimpse(poolingplaces_raw)
  
  return(poolingplaces_raw)
  }

# Clean Pooling Places  --------------------------------------------------------
clean_poolingplaces <- function(poolingplaces_raw, year){ # year = 2024
  
  ## 0. Create folder to save clean data ---------------------------------------
  
  dir_clean <- paste0("./data/pooling_places/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. Check the data  --------------------------------------------------------
  
  tabyl(poolingplaces_raw$nr_turno)
  tabyl(poolingplaces_raw$sg_uf)
  tabyl(poolingplaces_raw$ds_eleicao)
  tabyl(poolingplaces_raw, ds_tipo_local, cd_tipo_local)
  tabyl(poolingplaces_raw$ds_situ_local_votacao)
  tabyl(poolingplaces_raw, ds_situ_local_votacao, ds_tipo_local)
  tabyl(poolingplaces_raw$ds_situ_zona)
  tabyl(poolingplaces_raw$ds_situ_secao)
  tabyl(poolingplaces_raw$ds_tipo_secao_agregada)
  tabyl(poolingplaces_raw$ds_situ_secao_acessibilidade)
  tabyl(poolingplaces_raw$nr_secao_principal)
  tabyl(poolingplaces_raw$ds_situ_localidade)
  tabyl(poolingplaces_raw$ds_situ_secao)
  
  test <- poolingplaces_raw |> 
    filter(#nr_local_votacao != nr_local_votacao_original
           #qt_eleitor_secao == "0"
      cd_tipo_local == "4"
    )
  glimpse(test)
  
  ## 2. Prepare the data  ------------------------------------------------------

  glimpse(poolingplaces_raw)
  
  table_collumns <- tibble(nome_coluna = names(poolingplaces_raw),
                           classe_coluna = sapply(poolingplaces_raw, class))
  
  poolingplaces <- poolingplaces_raw |> 
    filter(nr_turno == 1, cd_situ_local_votacao == 1) |> 
    select(10, 15:21, 8, 9, 7, 23:26, 35) |> unique() |> 
    group_by(nr_zona, nr_local_votacao, nm_local_votacao, cd_tipo_local, 
             ds_tipo_local, ds_endereco, nm_bairro, nr_cep, cd_municipio,
             nm_municipio, sg_uf, nr_latitude, nr_longitude) |> 
    summarise(total_eleitores = sum(qt_eleitor_secao)) |> 
    ungroup()
  
  glimpse(poolingplaces)
  
  electoralzones <- poolingplaces_raw |> 
    filter(nr_turno == 1, cd_situ_local_votacao == 1) |> 
    select(10, 15:21, 8, 9, 7, 23:26, 35, 39:41) |> unique()
  
  ## 3. Separate the data with no spatial --------------------------------------
  
  # Filters observations that do not have recorded coordinates
  na_dados_latlon <- poolingplaces |>
    filter(is.na(nr_longitude))
  
  ## 3. Add geocoding collumn and create a sf dataset --------------------------
  
  poolingplaces_clean <- poolingplaces |> 
    filter(!is.na(nr_longitude)) |> 
    st_as_sf(coords = c("nr_longitude", "nr_latitude"),
             crs = 4674,
             remove = FALSE)
  
  glimpse(poolingplaces_clean)
  
  ## 4. Apply harmonize geobr cleaning -----------------------------------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = poolingplaces_clean,
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
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/poolingplaces_",  year,
  #                                   ".gpkg"), delete_dsn = TRUE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean, "/poolingplaces_",
  #                                               year, "_simplified.gpkg"),
  #              delete_dsn = TRUE )
  
  ### Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/poolingplaces_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 7
  )
  
  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean,"/poolingplaces_", year, "_simplified", ".parquet"),
    compression='zstd',
    compression_level = 7
  )
  
  ### Save in parquet the NA data
  arrow::write_parquet(
    x = na_dados_latlon,
    sink = paste0(dir_clean, "/poolingplaces_no_coords_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 7
  )
  
  ### Save in parquet the electoral zones
  arrow::write_parquet(
    x = electoralzones,
    sink = paste0(dir_clean, "/electoralzones_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 7
  )
  
  ## 7. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}

# Clean Electoral Zones  -------------------------------------------------------
clean_electoralzones <- function(poolingplaces_raw, year){ # year = 2024
  
  ## 0. Create folder to save clean data ---------------------------------------
  
  dir_clean <- paste0("./data/electoral_zones/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. Check the data  --------------------------------------------------------
  
  glimpse(poolingplaces_raw)
  
  tabyl(poolingplaces_raw$nr_turno)
  tabyl(poolingplaces_raw$ds_tipo_secao_agregada)       
  tabyl(poolingplaces_raw$ds_situ_local_votacao)
  tabyl(poolingplaces_raw$ds_situ_localidade)
  tabyl(poolingplaces_raw$ds_situ_secao_acessibilidade)
  tabyl(poolingplaces_raw$ds_situ_localidade)
  
  ## 2. Pre clean the data  ----------------------------------------------------
  
  table_collumns <- tibble(nome_coluna = names(poolingplaces_raw),
                           classe_coluna = sapply(poolingplaces_raw, class))
  
  electoralzones <- poolingplaces_raw |> 
    filter(nr_turno == 1, cd_situ_local_votacao == 1) |> 
    select(10, 15:21, 8, 9, 7, 23:26, 35, 39:41) |> unique()
  
  glimpse(electoralzones)
  
  electoralzones <- poolingplaces_raw |> 
    filter(nr_turno == 1, cd_situ_local_votacao == 1) |> 
    select(10, 15:21, 8, 9, 7, 23:26, 35, 39:41) |> unique()
  
  ## 3. Separate the data with no spatial --------------------------------------
  
  # Filters observations that do not have recorded coordinates
  na_dados_latlon <- electoralzones |>
    filter(is.na(nr_longitude))
  
  ## 3. Add geocoding collumn and create a sf dataset --------------------------
  
  electoralzones_clean <- electoralzones |> 
    filter(!is.na(nr_longitude)) |> 
    st_as_sf(coords = c("nr_longitude", "nr_latitude"),
             crs = 4674,
             remove = FALSE)
  
  glimpse(electoralzones_clean)
  
  ## 4. Apply harmonize geobr cleaning -----------------------------------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = electoralzones_clean,
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
  # temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  # 
  ## 6. Save datasets  ---------------------------------------------------------
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/electoralzones_",  year,
  #                                   ".gpkg"), delete_dsn = TRUE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean, "/electoralzones_",
  #                                               year, "_simplified.gpkg"),
  #              delete_dsn = TRUE )
  
  ### Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/electoralzones_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 7
  )
  
  # arrow::write_parquet(
  #   x = temp_sf_simplified,
  #   sink = paste0(dir_clean,"/electoralzones_", year, "_simplified", ".parquet"),
  #   compression='zstd',
  #   compression_level = 7
  # )
  
  ## 7. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}