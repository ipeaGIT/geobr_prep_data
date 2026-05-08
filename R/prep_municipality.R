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
  
  ### Year with parts of URL splitted in states --------------------------------
  if(year %in% c(2000, 2001, 2010:2014)) {
    
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
  
  ### Years with br URL (UPDATE YEAR) ------------------------------------------
  #2005 
  if(year == 2005) {
    options(timeout = 600)
    
    ftp_link <- paste0(url_start, year, "/escala_2500mil/proj_geografica/arcview_shp/brasil/55mu2500gc.zip")
    }
  
  # 2007
  if(year == 2007) {
    ftp_link <- paste0(url_start, year, "//escala_2500mil/proj_geografica_sirgas2000/brasil/55mu2500gsr.zip")
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
  
  if(year %in% c(2005, 2007, 2015:2024)) {
    httr::GET(url = ftp_link,
              httr::progress(),
              httr::write_disk(path = file_raw,
                               overwrite = T))
  }
  
  ## 4. Unzip Raw data ---------------------------------------------------------
  
  unzip_geobr(zip_dir = zip_dir, in_zip = in_zip,
              out_zip = out_zip, is_shp = TRUE)
  
  ## 5. Set correct encoding ----------------------------------------------------
  
  if (year == 2000) { #years without number of collumns errors
    encode <- "ENCODING=IBM437"
  }
  
  if (year %in% c(2001, 2005, 2007, 2010)) {
    encode <- "ENCODING=WINDOWS-1252"
  }
  
  if (year >= 2013) {
    encode =  "ENCODING=UTF8"
  }
  
  ## 6. Bind Raw data together -------------------------------------------------
  
  shp_names <- list.files(out_zip, pattern = "\\.shp$", full.names = TRUE)
  
  ### Read and merge
  municipality_raw <- readmerge_geobr(folder_path = out_zip,
                                      encoding = encode)
  glimpse(municipality_raw)
  
  ## 7. Show result ------------------------------------------------------------
  
  # data.table::setDF(municipality_raw)
  # 
  # municipality_raw <- sf::st_as_sf(municipality_raw) |>
  #   clean_names()
  
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

  ## 1. Checks and detect possible problems ------------------------------------

  ### create states tibble
  states <- states_geobr()
  states_clean <- states |> select(1:5)

  ## 2. Adjust and preparing for cleaning (UPDATE YEAR HERE) -------------------
  
  # Remove, rename, reorder collumns
  # Get dupes, merge island geometries, remove wrong rows
  # Give states codes, names and regions

  glimpse(municipality_raw)
  
  # Check projection
  st_crs(municipality_raw)
    
  names(municipality_raw)
  municipality_raw <- municipality_raw |> 
    clean_names()
  glimpse(municipality_raw)
  
  ### 2000 ----
  if (year == 2000) {
    # names_2000 <- c("mslink", "codigo", "area_1", "perimetro", "geocodigo",
    #                 "nome", "sede", "latitudese", "longitudes", "area_tot_g",
    #                 "reservado", "geometry")
    
    # tabyl(municipality_raw$sede)
    # tabyl(municipality_raw$reservado)
    
    glimpse(municipality_raw)
    st_crs(municipality_raw)
    
    # Apply the adjustments and set projection 

    municipality <-  municipality_raw |> 
      select(geocodigo, nome, geometry) |> # remove collumns
      #select(-mslink, -codigo, -area_1, -perimetro, -sede, -latitudese,
      #-longitudes, -area_tot_g, -reservado)
      rename(code_muni = geocodigo, name_muni = nome) |>  # rename collumns
      filter(code_muni != "0") |> # remove empty entries 
      mutate(name_muni = if_else(name_muni == "CANANEIA",
                                 "Cananéia", name_muni)) |> # remover erro em CANANEIA
      group_by(code_muni, name_muni) |> # dissolve island geometries
      summarise(.groups = "drop") |> # code_muni unique
      mutate(code_state = str_sub(code_muni, start = 1, end = 2)) |> # gerar coluna code_state
      inner_join(states_clean, by = c("code_state")) |> 
      relocate(geometry, .after = name_region)
    
    glimpse(municipality)
    st_crs(municipality)
    
    municipality_clean <- municipality |> 
    st_set_crs(4674)
    st_crs(municipality_clean)
    
  }
  
  ### 2001 ----
  if (year %in% c(2001, 2005)) {
    # "mslink"       "mapid"        "codigo"       "area_1"       "perimetro"   
    # "geocodigo"    "nome"         "sede"         "latitudese"   "longitudes"  
    # "area_tot_g"   "mslink_2"     "mapid_2"      "codigo_2"     "area_1_2"    
    # "perimetro_2"  "geocodigo_2"  "nome_2"       "sede_2"       "latitude_se" 
    # "longitude_s"  "area_tot_g_2" "gavprimary"   "gavprima_1"   "geometry_s"  
    # "latitude"     "longitude"    "areamunici"   "geometry"
  
    # tabyl(municipality_raw$sede)
    # tabyl(municipality_raw$reservado)
    
    glimpse(municipality_raw)
    st_crs(municipality_raw)
    
    # Apply the adjustments and set projection 
    
    municipality <-  municipality_raw |> 
      select(geocodigo, nome, geometry) |> # remove collumns
      rename(code_muni = geocodigo, name_muni = nome) |>  # rename collumns
      filter(code_muni != "0", !is.na(code_muni)) |> # remove empty entries 
      group_by(code_muni, name_muni) |> # dissolve island geometries
      summarise(.groups = "drop") |> # code_muni unique
      mutate(code_state = str_sub(code_muni, start = 1, end = 2)) |> # gerar coluna code_state
      inner_join(states_clean, by = c("code_state")) |> 
      relocate(geometry, .after = name_region)
    
    glimpse(municipality)
    st_crs(municipality)
    
    municipality_clean <- municipality |> 
      st_set_crs(4674)
    st_crs(municipality_clean)
    
    #check integrity
    test <- municipality_clean |> get_dupes(code_muni)
    
    }
  
  ### 2005 ----
  if (year %in% c(2005)) {
    # "geocodigo"  "nome"       "uf"         "id_uf"      "regiao"     "mesoregiao"
    # [7] "microregia" "latitude"   "longitude"  "sede"       "geometry"  
    
    # tabyl(municipality_raw$sede)
    # tabyl(municipality_raw$reservado)
    
    glimpse(municipality_raw)
    st_crs(municipality_raw)
  
    
  }
  
  ### 2007 ----
  if (year %in% c(2007)) {
    # "geocodig_m"  "uf"          "sigla"       "nome_munic"  "reg_iao"    
    # "mesorreg_ia" "nome_meso"   "microrregi"  "nome_micro"  "geometry"
  
    # tabyl(municipality_raw$sede)
    # tabyl(municipality_raw$reservado)
    
    glimpse(municipality_raw)
    st_crs(municipality_raw)
    
    # Apply the adjustments and set projection 
    
    municipality <-  municipality_raw |> 
      select(geocodig_m, nome_munic, geometry) |> # remove collumns
      rename(code_muni = geocodig_m, name_muni = nome_munic) |>  # rename collumns
      filter(code_muni != "0", !is.na(code_muni)) |> # remove empty entries 
      group_by(code_muni, name_muni) |> # dissolve island geometries
      summarise(.groups = "drop") |> # code_muni unique
      mutate(code_state = str_sub(code_muni, start = 1, end = 2)) |> # gerar coluna code_state
      inner_join(states_clean, by = c("code_state")) |> 
      relocate(geometry, .after = name_region)
    
    glimpse(municipality)
    st_crs(municipality)
    
    municipality_clean <- municipality |> 
      st_set_crs(4674)
    st_crs(municipality_clean)
    
    #check integrity
    test <- municipality_clean |> get_dupes(code_muni)
    
    }
  
  ### 2010 ----
  if (year %in% c(2010)) {
    #"id"         "cd_geocodm" "nm_municip" "geometry" 
  
    glimpse(municipality_raw)
    st_crs(municipality_raw)
    
    # Apply the adjustments and set projection 
    
    municipality <-  municipality_raw |> 
      st_set_crs(NA) |> 
      select(cd_geocodm, nm_municip, geometry) |> # remove collumns
      rename(code_muni = cd_geocodm, name_muni = nm_municip) |>  # rename collumns
      filter(code_muni != "0", !is.na(code_muni)) |> # remove empty entries 
      group_by(code_muni, name_muni) |> # dissolve island geometries
      summarise(.groups = "drop") |> # code_muni unique
      mutate(code_state = str_sub(code_muni, start = 1, end = 2),
             name_muni = str_to_title(name_muni)) |> # gerar coluna code_state
      inner_join(states_clean, by = c("code_state")) |> 
      relocate(geometry, .after = name_region)
    
    glimpse(municipality)
    st_crs(municipality)
    
    municipality_clean <- municipality |> 
      st_set_crs(4674)
    st_crs(municipality_clean)
    
    #check integrity
    test <- municipality_clean |> get_dupes(code_muni)
  }
  
  ### 2013 until 2018 ----
  if (year %in% c(2013:2018)) {
    #"nm_municip" "cd_geocmu"  "geometry"  
    
    glimpse(municipality_raw)
    st_crs(municipality_raw)
    
    # Apply the adjustments and set projection 
    
    municipality <-  municipality_raw |> 
      st_set_crs(NA) |> 
      select(cd_geocmu, nm_municip, geometry) |> # remove collumns
      rename(code_muni = cd_geocmu, name_muni = nm_municip) |>  # rename collumns
      filter(code_muni != "0", !is.na(code_muni)) |> # remove empty entries 
      group_by(code_muni, name_muni) |> # dissolve island geometries
      summarise(.groups = "drop") |> # code_muni unique
      mutate(code_state = str_sub(code_muni, start = 1, end = 2),
             name_muni = str_to_title(name_muni)) |> # gerar coluna code_state
      inner_join(states_clean, by = c("code_state")) |> 
      relocate(geometry, .after = name_region)
    
    glimpse(municipality)
    st_crs(municipality)
    
    municipality_clean <- municipality |> 
      st_set_crs(4674)
    st_crs(municipality_clean)
    
    #check integrity
    test <- municipality_clean |> get_dupes(code_muni)
  }
  
  ### 2019 pra frente ----
    if (year >= 2019) {
      #2019 "nm_municip" "cd_geocmu"  "geometry"
      # 2020 "cd_mun"   "nm_mun"   "sigla_uf" "area_km2" "geometry"
      
      glimpse(municipality_raw)
      st_crs(municipality_raw)
      
      # Apply the adjustments and set projection 
      
      municipality <-  municipality_raw |> 
        st_set_crs(NA) |> 
        select(cd_mun, nm_mun, geometry) |> # remove collumns
        rename(code_muni = cd_mun, name_muni = nm_mun) |>  # rename collumns
        filter(code_muni != "0", !is.na(code_muni)) |> # remove empty entries 
        group_by(code_muni, name_muni) |> # dissolve island geometries
        summarise(.groups = "drop") |> # code_muni unique
        mutate(code_state = str_sub(code_muni, start = 1, end = 2),
               name_muni = str_to_title(name_muni)) |> # gerar coluna code_state
        inner_join(states_clean, by = c("code_state")) |> 
        relocate(geometry, .after = name_region)
      
      glimpse(municipality)
      st_crs(municipality)
      
      municipality_clean <- municipality |> 
        st_set_crs(4674)
      st_crs(municipality_clean)
      
      #check integrity
      test <- municipality_clean |> get_dupes(code_muni)
    }
  
  ## 3. Apply harmonize geobr cleaning -----------------------------------------
  
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
    use_multipolygon = T)
  
  glimpse(temp_sf)
  
  ## 4. Check integrity and do post corrections --------------------------------
  
  temp_sf <- temp_sf |> 
    ungroup()
  glimpse(temp_sf)
  st_crs(temp_sf)
  
  ## 5. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 6. Save datasets  ---------------------------------------------------------
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/states_",  year,
  #                                   ".gpkg"), delete_dsn = TRUE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean, "/states_",
  #                                               year, "_simplified.gpkg"),
  #              delete_dsn = TRUE )
  
  ### Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/municipalities_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 7
  )
  
  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean,"/municipalities_", year, "_simplified", ".parquet"),
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
