#> DATASET: municipality 2000 a 2024
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

# year <- tar_read(years_municipality, branches = 1)[1]

download_municipality <- function(year){ # year = 2010
  
  ## 0. Generate the correct ftp link (UPDATE YEAR HERE) -----------------------
  
  url_start <- paste0("https://geoftp.ibge.gov.br/organizacao_do_territorio/",
                      "malhas_territoriais/malhas_municipais/municipio_")
  
  ### create states tibble
  states <- states_geobr()
  
  # Before 2015
  if(year %in% c(2000, 2001, 2010:2014)) {
    
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
  # zip_dir <- paste0("./data_raw/", "/municipality/", year)
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

# year <- tar_read(years_municipality, branches = 1)[1]
# municipality_raw <- tar_read(municipality_raw, branches = 1)

clean_municipality <- function(municipality_raw, year){ # year = 2024
  
  ## 0. Create folder to save clean data ---------------------------------------
  
  dir_clean <- paste0("./data/municipality/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. Checks and detect possible problemns -----------------------------------

  glimpse(municipality_raw)
  
  # número de municípios por estado está ok?
  teste <- municipality_raw |> 
    filter(geocodigo != 0) |> 
    mutate(codigo_estado_uf = str_sub(codigo, start = 1, end = 2)) |> 
    group_by(codigo_estado_uf) |> 
    summarize(contagem = n())
  
  teste <- municipality_raw |> 
    filter(geocodigo != 0) |> 
    mutate(codigo_estado_uf = str_sub(codigo, start = 1, end = 2),
           cidade_original = as.character(nome)) |> 
    group_by(codigo_estado_uf, cidade_original) |> 
    summarize(areatot = sum(st_area(geometry))) |> 
    ungroup()
  
  # Detect all the cases with strange characters for special characters
  # regex that takes everything that is not a letter, a space or a hífen
  
  suspect <- municipality_raw |>
    select(nome, geocodigo) |> 
    #filter(str_detect(nome, "[^[:alpha:] '\\-]")) |> 
    mutate(nome_corrigido = nome |>
             str_replace_all(fixed("\xc6"), "a") |> #ã
             str_replace_all(fixed("\xb6"), "A") |> #Â
             str_replace_all(fixed("\x90"), "E") |> #É
             str_replace_all(fixed("\x83"), "a") |> #ã
             str_replace_all(fixed("\xa0"), "a") |> #á
             str_replace_all(fixed("\xa3"), "u") |> #ú
             str_replace_all(fixed("\xa1"), "i") |> #í
             str_replace_all(fixed("\x82"), "é") |> #é
             str_replace_all(fixed("\x88"), "e") |> #ê
             str_replace_all(fixed("\xa2"), "o") |> #ó
             str_replace_all(fixed("\x93"), "o") |> #ô
             str_replace_all(fixed("\x87"), "ç"),
           estranho_remanecente = str_detect(nome_corrigido, "[^[:alpha:] '\\-]")) |> 
    arrange(estranho_remanecente)
  
  
  sobrou <- suspect |> filter(estranho_remanecente == TRUE)
  sobrou$nome |> head(200)
  
           
  # Find a solution
  
  # corrigir_texto <- function(x) {
  #   stringi::stri_trans_general(x, "latin-ascii")
  # }
  # 
  # suspect <- suspect |>
  #   mutate(nome_corrigido = corrigir_texto(nome))
  # 
  # corrigir_municipio <- function(x) {
  #   raw <- charToRaw(x)
  #   raw[raw == as.raw(0xa0)] <- as.raw(0xe1)  # á
  #   raw[raw == as.raw(0xe3)] <- as.raw(0xe3)  # ã (mantém)
  #   raw[raw == as.raw(0xe7)] <- as.raw(0xe7)  # ç (mantém)
  #   rawToChar(raw)
  # }
  # 
  # suspect$nome <- vapply(
  #   suspect$nome,
  #   corrigir_municipio,
  #   character(1)
  # )
  
  # stringi::stri_encode(from='latin1', to="utf8", str= "S\u00e3o Paulo")
  # stringi::stri_encode('S\u00e3o Paulo', to="UTF-8")
  # gtools::ASCIIfy('S\u00e3o Paulo')
  
  # função para corrigir
  # corrigir_municipio <- function(x) {
  #   x <- gsub("\xa0", "á", x, fixed = TRUE)
  #   x <- gsub("\xe1", "á", x, fixed = TRUE)
  #   x <- gsub("\xe3", "ã", x, fixed = TRUE)
  #   x <- gsub("\xe9", "é", x, fixed = TRUE)
  #   x <- gsub("\xea", "ê", x, fixed = TRUE)
  #   x <- gsub("\xed", "í", x, fixed = TRUE)
  #   x <- gsub("\xf3", "ó", x, fixed = TRUE)
  #   x <- gsub("\xf4", "ô", x, fixed = TRUE)
  #   x <- gsub("\xf5", "õ", x, fixed = TRUE)
  #   x <- gsub("\xfa", "ú", x, fixed = TRUE)
  #   x <- gsub("\xe7", "ç", x, fixed = TRUE)
  #   x
  # }
  # 
 
  ## 1. Adjust and preparing for cleaning --------------------------------------
  
  #For years that have spelling problems
  if (year %in% c(2000, 2001, 2010, 2013:2018)){ 
    glimpse(municipality_raw)
    
    ### create states tibble
    states <- states_geobr()
    
    states_thin <- states |> 
      select(1:5)
    
    ### 2000 -------------------------------------------------------------------
    if (year %in% c(2000)) {
      
      glimpse(municipality_raw)
      
      municipality_raw$nome[municipality_raw$nome == "CANANEIA"] <- "Cananeia"
      
      municipality_clean <- municipality_raw |> 
        filter(geocodigo != 0) |> 
        select(-area_1, -perimetro, -sede, -latitudese, -longitudes,
               -area_tot_g, -reservado, # colunas com infos originais
               -mslink) |> # colunas que não há em todos os anos
        mutate(caractere_estranho = str_detect(nome, "[^[:alpha:] \\-']"),
               code_state = str_sub(codigo, start = 1, end = 2),
               name_muni = nome |>
                 str_replace_all(fixed("\xc6"), "a") |> #ã
                 str_replace_all(fixed("\xb6"), "A") |> #Â
                 str_replace_all(fixed("\x90"), "E") |> #É
                 str_replace_all(fixed("\x83"), "a") |> #ã
                 str_replace_all(fixed("\xa0"), "a") |> #á
                 str_replace_all(fixed("\xa3"), "u") |> #ú
                 str_replace_all(fixed("\xa1"), "i") |> #í
                 str_replace_all(fixed("\x82"), "é") |> #é
                 str_replace_all(fixed("\x88"), "e") |> #ê
                 str_replace_all(fixed("\xa2"), "o") |> #ó
                 str_replace_all(fixed("\x93"), "o") |> #ô
                 str_replace_all(fixed("\x87"), "ç"),
               estranho_remanecente = str_detect(name_muni, "[^[:alpha:] '\\-]")) |> 
        group_by(code_state, codigo, geocodigo, nome,
                 name_muni, caractere_estranho, estranho_remanecente) |> 
        summarise(total_area = sum(st_area(geometry))) |> 
        ungroup()
      
    }
   
    ### 2001 -------------------------------------------------------------------
    if (year == 2001){ 
      municipality_clean <- municipality_raw |> 
        filter(geocodigo != 0) |> 
        select(-mslink, -mapid, -nome, -geocodigo, -area_1) |> # remover colunas originais
        left_join(states_thin, by = c("codigo" = "code_state")) |> 
        relocate(abbrev_state, name_state, code_region,
                 name_region, .after = codigo)
    }
    
    if (year == 2010){ 
      municipality_clean <- municipality_raw |> 
        left_join(states_thin, by = c("cd_geocodu" = "code_state")) |>
        select(-nm_estado, -id, -nm_regiao)
    }
    
    # For years that have only uppercase
    if (year %in% c(2013:2018)){ 
      municipality_clean <- municipality_raw |> 
        left_join(states_thin, by = c("cd_geocuf" = "code_state")) |>
        mutate(nm_regiao = str_to_title(nm_regiao),
               nm_estado = str_to_title(nm_estado)) |> 
        select(cd_geocuf, abbrev_state, nm_estado, code_region, nm_regiao)
    }
    glimpse(municipality_clean)
  }
  
  #For years that have no spelling problems
  if (year %in% c(2019:2024)){ 
    # glimpse(municipality_raw)
    # glimpse(states_geobr)
    municipality_clean <- municipality_raw
    # glimpse(municipality_clean)
  }
  
  ## 2. Apply harmonize geobr cleaning -----------------------------------------
  
  glimpse(municipality_clean)

  temp_sf <- harmonize_geobr(
    temp_sf = municipality_clean,
    year = year,
    add_state = F, #state_column = "name_state",
    add_region = F,# region_column = "code_state",
    add_snake_case = F,
    #snake_colname = c("name_state", "name_region"),
    projection_fix = T,
    encoding_utf8 = T,
    topology_fix = T,
    remove_z_dimension = T,
    use_multipolygon = F
  )
  
  glimpse(temp_sf)
  
  ## 3. Create dicionario de equivalências para dataset states -----------------
  
  #Este é um dicionário padrão com as denominações GEOBR:
  dicionario <- data.frame(
    # Lista de nomes padronizados de colunas
    padrao = c(
      #CÓDIGO DE MUNICÍPIO e número de variações associadas
      rep("code_muni", 8),
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
      "cod_mun", "codigo",
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
  
  ## 4. Rename collumns --------------------------------------------------------
  
  temp_sf2 <- standardcol_geobr(temp_sf, dicionario)
  
  # ordem recomendada
  # c(temp_sf, 'code_state', 'abbrev_state', 'name_state', 'code_region',
  #  'name_region', 'geom')
  
  if (year %in% c(2000)) {
    glimpse(temp_sf2)

    }
  
  ## 5. Check integrity, reorder and do post corrections -----------------------
  
  # 2000, 2001, 2010:2018 ------------------------------------------------------
  # harmonize_geobr remove: abbrev_state, colunas de perímetro e área
  # converte code_state em dbl
  if (year %in% c(2000, 2001, 2010, 2013:2018)) {
    
    temp_sf2$code_state <- as.character(temp_sf2$code_state)
        
    states <- states_geobr()
    
    states_thin <- states |> select(1:5)
    
    municipality <- temp_sf2 |> 
      ungroup() |> 
      select(-caractere_estranho, -estranho_remanecente,
             -nome, -total_area, -geocodigo) |> 
      inner_join(states_thin, by = "code_state") |> 
      relocate(code_region, name_region, code_state, abbrev_state,
               code_muni, name_muni, year, .before = geometry)
    
    glimpse(municipality)  
    
    tabyl(municipality$abbrev_state)
  }
  
  # 2019 até 2022 --------------------------------------------------------------
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
  
  # 2023 em diante -------------------------------------------------------------
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
  
  ## 7. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(municipality, tolerance = 100)
  
  ## 8. Save datasets  ---------------------------------------------------------
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/states_",  year,
  #                                   ".gpkg"), delete_dsn = TRUE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean, "/states_",
  #                                               year, "_simplified.gpkg"),
  #              delete_dsn = TRUE )
  
  ### Save in parquet
  arrow::write_parquet(
    x = municipality,
    sink = paste0(dir_clean, "/municipality_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 7
  )
  
  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean,"/municipality_", year, "_simplified", ".parquet"),
    compression='zstd',
    compression_level = 7
  )
  
  ## 9. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}