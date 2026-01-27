#> DATASET: municipality 2000a 2024
#> Source: IBGE - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html?=&t=o-que-e
#> scale 1:250.000
#> Metadata: #####
# Título: Municípios
# Título alternativo: municipality
# Frequência de atualização: ?????
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Poligonos dos municípios brasileiros.
# Informações adicionais: Dados produzidos pelo IBGE, e utilizados na elaboração do shape dos municípios com a melhor base oficial disponível.
# Propósito: Disponibilização das fronteiras municipais do Brasil.
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas: ****
# Informação do Sistema de Referência: SIRGAS 2000
#
# Observações: 
# Anos disponíveis: 

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

# rm(list = setdiff(ls(), lsf.str()))

# Download the data  -----------------------------------------------------------

# year <- tar_read(years_states, branches = 1)[1]

download_municipality <- function(year){ # year = 2010
  
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
                         states$abbrevm_state, "_municipios.zip")
    }
    
    #2001
    if(year == 2001) {
      ftp_link <- paste0(url_start, year, "/", states$abbrevm_state, "/",
                         states$code_state, "mu2500g.zip")
    }
    
    #2013 e 2014
    if(year %in% c(2013:2014)) {
      ftp_link <- paste0(url_start, year, "/", states$abbrev_state, "/",
                         states$abbrevm_state, "_municipios.zip")
    }
    
    filenames <- basename(ftp_link)
    
    names(ftp_link) <- filenames
  } 
  
  # 2015 until 2018
  if(year %in% c(2015:2018)) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR/br_municipios.zip")
  }
  
  # 2019 
  if(year %in% c(2019)) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR/br_municipios_20200807.zip")
  }
  
  # 2020 until 2022
  if(year %in% c(2020:2022)) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR/BR_Municipios_", year, ".zip")
  }
  
  # After 2023
  if(year >= 2023) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR_Municipios_", year, ".zip")
  }
  
  ## 1. Create temp folder -----------------------------------------------------
  
  zip_dir <- paste0(tempdir(), "/municipality/", year)
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
    municipality_list <- pbapply::pblapply(
      X = shp_names,
      FUN = function(x){ sf::st_read(x, quiet = T, stringsAsFactors= F)
      }
    )
    
    municipality_raw <- data.table::rbindlist(municipality_list)
  }
  
  if (year %in% c(2001, 2010:2014))  {#years with error in number of collumns
    municipality_raw <- readmerge_geobr(folder_path = out_zip)
  }
  
  #### After 2015
  if (length(shp_names) == 1) {
    municipality_raw <- st_read(shp_names, quiet = T, stringsAsFactors= F)
  }
  
  ## 6. Integrity test ---------------------------------------------------------
  
  #### Before 2015
  glimpse(municipality_raw)
  
  #### After 2015
  if (length(shp_names) == 1) {
    table_collumns <- tibble(name_collum = colnames(municipality_raw),
                             type_collum = sapply(municipality_raw, class)) |> 
      rownames_to_column(var = "num_collumn")
    
    # glimpse(table_collumns)
    # glimpse(municipality_raw)
  }
  
  ## 7. Show result ------------------------------------------------------------
  
  data.table::setDF(municipality_raw)
  
  municipality_raw <- sf::st_as_sf(municipality_raw) |> 
    clean_names()
  
  glimpse(municipality_raw)
  
  return(municipality_raw)
  }

# Clean the data  --------------------------------------------------------------

# year <- tar_read(years_states, branches = 1)[1]
# states_raw <- tar_read(states_raw, branches = 1)

clean_municipality <- function(municipality_raw, year){ # year = 2024
  
  ## 0. Create folder to save clean data ---------------------------------------
  
  dir_clean <- paste0("./data/municipality/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. Create states names reference table ------------------------------------
  
  states <- states_geobr()
  
  ## 2. Adjust and preparing for cleaning --------------------------------------
  
  glimpse(municipality_raw)
  states_thin <- states |> 
    select(1:5)
  
  #For years that have spelling problems
  if (year %in% c(2000, 2001, 2010, 2013:2018)){ 
    glimpse(states_raw)
    # glimpse(states_geobr)
    
    if (year == 2000){ 
      states_clean <- states_raw |> 
        filter(geocodigo != 0) |> 
        select(-nome, -mslink, -geocodigo, -area_1, -reservado) |> # remover colunas originais
        left_join(states_thin, by = c("codigo" = "code_state")) |> 
        relocate(abbrev_state, name_state, code_region,
                 name_region, .after = codigo)
    }
    
    if (year == 2001){ 
      states_clean <- states_raw |> 
        filter(geocodigo != 0) |> 
        select(-mslink, -mapid, -nome, -geocodigo, -area_1) |> # remover colunas originais
        left_join(states_thin, by = c("codigo" = "code_state")) |> 
        relocate(abbrev_state, name_state, code_region,
                 name_region, .after = codigo)
    }
    
    if (year == 2010){ 
      states_clean <- states_raw |> 
        left_join(states_thin, by = c("cd_geocodu" = "code_state")) |>
        select(-nm_estado, -id, -nm_regiao)
    }
    
    # For years that have only uppercase
    if (year %in% c(2013:2018)){ 
      states_clean <- states_raw |> 
        left_join(states_thin, by = c("cd_geocuf" = "code_state")) |>
        mutate(nm_regiao = str_to_title(nm_regiao),
               nm_estado = str_to_title(nm_estado)) |> 
        select(cd_geocuf, abbrev_state, nm_estado, code_region, nm_regiao)
    }
    glimpse(states_clean)
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
      rep("code_state", 8),
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
      "cod_uf", "cd_uf", "code_uf", "codigo_uf", "cod_state", "cd_mun",
      "cod_mun", 
      #Variações que convergem para "name_muni"
      "nome_cidade", "cidade", "nm_muni", "nome_muni",
      #Variações que convergem para "code_state"
      "cod_uf", "cd_uf", "code_uf", "codigo_uf", "cod_state", "cd_geocodu",
      "codigo", "cd_geocuf",
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
  
  ## 4. Rename collumns and reorder collumns and other post corrections --------
  
  states_clean <- standardcol_geobr(states_clean, dicionario)
  
  glimpse(states_clean)
  
  # ordem recomendada
  # c(temp_sf, 'code_state', 'abbrev_state', 'name_state', 'code_region',
  #  'name_region', 'geom')
  
  if (year %in% c(2000, 2001)) {
    
    glimpse(states_clean)
    states_corrigido <- states_clean |> 
      group_by(code_state, abbrev_state, name_state, code_region, name_region) |> 
      summarise(total_peri = sum(perimetro),
                total_area = sum(area_tot_g),
                .groups = "drop") #|> 
    # mutate(teste_peri = as.numeric(sf::st_perimeter(geometry)),
    #        teste_area = as.numeric(sf::st_area(geometry))/1e6)
    
    glimpse(states_corrigido)
    nrow(states_corrigido)
    st_geometry_type(states_corrigido)
    
    states_clean <- states_corrigido
  }
  
  
  ## 5. Apply harmonize geobr cleaning -----------------------------------------
  
  glimpse(states_raw)
  glimpse(states_clean)
  
  temp_sf <- harmonize_geobr(
    temp_sf = states_clean,
    year = year,
    add_state = F, #state_column = "name_state",
    add_region = F,# region_column = "code_state",
    add_snake_case = F,
    #snake_colname = c("name_state", "name_region"),
    projection_fix = T,
    encoding_utf8 = T,
    topology_fix = T,
    remove_z_dimension = T,
    use_multipolygon = T
  )
  
  glimpse(temp_sf)
  
  ## 6. Check integrity and do post corrections --------------------------------
  
  # 2000, 2001, 2010:2018
  # harmonize_geobr remove: abbrev_state, colunas de perímetro e área
  # converte code_state em dbl
  if (year %in% c(2000, 2001, 2010, 2013:2018)) {
    
    temp_sf$code_state <- as.character(temp_sf$code_state)
    states_thin <- states |> select(1:2)
    
    temp_sf <- temp_sf |> 
      ungroup() |> 
      inner_join(states_thin, by = "code_state") |> 
      relocate(abbrev_state, .before = name_state)
    
    glimpse(temp_sf)  
  }
  
  if (year %in% c(2019:2022)) {
    states_thin <- states |> select(1:2,4)
    
    temp_sf <- temp_sf |> 
      ungroup() |> 
      rename(code_state = "code_muni") 
    
    temp_sf$code_state <- as.character(temp_sf$code_state)
    
    temp_sf <- temp_sf |> 
      inner_join(states_thin, by = "code_state") |> 
      relocate(abbrev_state, .before = name_state) |> 
      relocate(code_region, .before = name_region)
    
    glimpse(temp_sf)  
  }
  
  if (year %in% c(2023:2024)) {
    states_thin <- states |> select(1:2)
    
    temp_sf <- temp_sf |> 
      ungroup() |> 
      rename(code_state = "code_muni") 
    
    temp_sf$code_state <- as.character(temp_sf$code_state)
    
    temp_sf <- temp_sf |> 
      inner_join(states_thin, by = "code_state") |> 
      relocate(abbrev_state, .before = name_state) |> 
      relocate(code_region, .before = name_region)
    
    glimpse(temp_sf)  
  }
  
  if (nrow(temp_sf) > 27) {stop("existem apenas 27 unidades da federacao")}
  
  ## 7. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 8. Save datasets  ---------------------------------------------------------
  
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
  
  ## 9. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}