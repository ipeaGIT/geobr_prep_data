#> DATASET: regions 2000 a 2024
#> Source: IBGE - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html?=&t=o-que-e
#> scale 1:250.000
#> Metadata: #####
# Título: Regiões
# Título alternativo: regions
# Frequência de atualização: ?????
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Polígonos das regiões brasileiras.
# Informações adicionais: Dados de estados produzidos pelo IBGE, e utilizados na elaboração do shape das regiões com a melhor base oficial disponível.
# Propósito: Disponibilização das fronteiras regionais do Brasil.
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas: ****
# Informação do Sistema de Referência: SIRGAS 2000
#
# Observações: 
# Anos disponíveis: 2000 a 2024

### Libraries (use any library as necessary) ----

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

# Download the data  ----
download_regions <- function(year){ # year = 2010
  
  ## 0. Generate the correct ftp link (UPDATE YEAR HERE) ----
  
  url_start <- paste0("https://geoftp.ibge.gov.br/organizacao_do_territorio/",
                      "malhas_territoriais/malhas_municipais/municipio_")
  
  # Before 2015
  if(year %in% c(2000, 2001, 2010:2014)) {
    ### create states tibble
    states <- states_geobr()
    
    ### parts of url
    
    #2000 ou 2010
    if(year %in% c(2000, 2010)) {
      ftp_link <- paste0(url_start, year, "/", states$sgm_state, "/",
                         states$sgm_state, "_unidades_da_federacao.zip")
    }
    
    #2001
    if(year == 2001) {
      ftp_link <- paste0(url_start, year, "/", states$sgm_state, "/",
                         states$cod_states, "uf2500g.zip")
    }
    
    #2013
    if(year == 2013) {
      ftp_link <- paste0(url_start, year, "/", states$sg_state, "/",
                         states$sgm_state, "_unidades_da_federacao.zip")
      
      #correct spell error in AM
      ftp_link[3] <- paste0(url_start, year, "/", states$sg_state[3], "/",
                            states$sgm_state[3], "_unidades_da_fedecao.zip")
      
    }
    
    #2014
    if(year == 2014) {
      ftp_link <- paste0(url_start, year, "/", states$sg_state, "/",
                         states$sgm_state, "_unidades_da_federacao.zip")
    }
    
    filenames <- basename(ftp_link)
    
    names(ftp_link) <- filenames
  } 
  
  # 2015 until 2019
  if(year %in% c(2015:2019)) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR/br_unidades_da_federacao.zip")
  }
  
  # 2020 until 2022
  if(year %in% c(2020:2022)) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR/BR_UF_", year, ".zip")
  }
  
  # After 2023
  if(year >= 2023) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR_UF_", year, ".zip")
  }
  
  ## 1. Create temp folder ----
  
  zip_dir <- paste0(tempdir(), "/regions/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  ### Alternative folder
  # zip_dir <- paste0("./data_raw/", "/regions/", year)
  # dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  # dir.exists(zip_dir)
  
  ## 2. Create direction for each download ----
  
  ### zip folder
  in_zip <- paste0(zip_dir, "/zipped/")
  dir.create(in_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(in_zip)
  
  file_raw <- fs::file_temp(tmp_dir = in_zip,
                            ext = fs::path_ext(ftp_link))
  
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  ## 3. Download Raw data (UPDATE YEAR) ----
  
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
  
  ## 4. Unzip Raw data ----
  
  unzip_geobr(zip_dir = zip_dir, in_zip = in_zip, out_zip = out_zip, is_shp = TRUE)
  
  ## 5. Bind Raw data together (UPDATE YEAR) ----
  
  shp_names <- list.files(out_zip, pattern = "\\.shp$", full.names = TRUE)
  
  #### Before 2015
  if (year == 2000) { #years without IBGE errors
    regions_list <- pbapply::pblapply(
      X = shp_names,
      FUN = function(x){ sf::st_read(x, quiet = T, stringsAsFactors= F) }
    )
    
    regions_raw <- data.table::rbindlist(regions_list)
  }
  
  if (year %in% c(2001, 2010:2014))  {#years with error in number of collumns
    regions_raw <- readmerge_geobr(folder_path = out_zip)
  }
  
  #### After 2015
  if (length(shp_names) == 1) {
    regions_raw <- st_read(shp_names, quiet = T, stringsAsFactors= F)
  }
  
  ## 6. Integrity test ----
  
  #### Before 2015
  glimpse(regions_raw)
  
  #### After 2015
  if (length(shp_names) == 1) {
    table_collumns <- tibble(name_collum = colnames(regions_raw),
                             type_collum = sapply(regions_raw, class)) |> 
      rownames_to_column(var = "num_collumn")
    
    glimpse(table_collumns)
    glimpse(regions_raw)
  }
  
  ## 7. Show result ----
  
  data.table::setDF(regions_raw)
  
  regions_raw <- sf::st_as_sf(regions_raw) %>% 
    clean_names()
  
  return(regions_raw)
  
}

# Clean the data  ----
clean_regions <- function(regions_raw, year){ # year = 2024
  
  ## 0. Create folder to save clean data -----
  
  dir_clean <- paste0("./data/regions/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. Check names of the states (UPDATE YEAR) -----
  
  states <- states_geobr()
  states <- states %>% 
    select(cod_states, cod_region) %>% 
    mutate(cod_states = as.character(cod_states))
  
  glimpse(regions_raw)
  
  #For years that have spelling problems
  if (year %in% c(2000, 2001, 2010, 2013:2018)){ 
    
    if (year %in% c(2000, 2001)){ 
      regions_clean <- regions_raw %>% 
        left_join(states, by = c("codigo" = "cod_states"))  |> 
        select(cod_region)
    }
    
    if (year %in% c(2010)){
      regions_clean <- regions_raw %>% 
        left_join(states, by = c("cd_geocodu" = "cod_states")) |> 
        select(cod_region)
    }
    
    # For years that have only uppercase
    if (year %in% c(2013:2018)){ 
      regions_clean <- regions_raw %>% 
        left_join(states, by = c("cd_geocuf" = "cod_states")) |> 
        select(cod_region)
    }
    glimpse(regions_clean)
  }
  
  #For years that have no spelling problems
  if (year %in% c(2019:2024)){ 
    regions_clean <- regions_raw |> 
      left_join(states, by = c("cd_uf" = "cod_states")) |> 
      select(cod_region)
  }
  
  # remove wrong-coded regions
  regions_clean <- subset(regions_clean, cod_region %in% c(1:5))

  # store original crs
  original_crs <- st_crs(regions_clean)
  
  ## 2. Transform states in regions -----
  
  ### Dissolve each region
  all_regions <- dissolve_polygons(mysf=regions_clean, group_column='cod_region')
  
  glimpse(all_regions)
  
  ### add region names
  
  all_regions <- add_region_info(temp_sf = all_regions, column = 'cod_region')
  all_regions <- select(all_regions, c('cod_region', 'name_region', 'geometry'))
  
  glimpse(all_regions)
  plot(all_regions)
  
  ## 3. Apply harmonize geobr cleaning ----
  
  temp_sf <- harmonize_geobr(
    temp_sf = all_regions,
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
  
  ## 4. lighter version ----
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 5. Save datasets  ----
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/regions_",  year,
  #                                   ".gpkg"), delete_dsn = TRUE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean, "/regions_",
  #                                               year, "_simplified.gpkg"),
  #              delete_dsn = TRUE )
  
  ### Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/regions_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 22
  )
  
  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean,"/regions_", year, "_simplified", ".parquet"),
    compression='zstd',
    compression_level = 22
  )
  
  ## 6. Create the files for geobr index  ----
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}
# 
# 
###### OLD CODE BELOW ####################
# 
# ###### 0. Create Root folder to save the data -----------------
# root_dir <- "L:\\# DIRUR #\\ASMEQ\\geobr\\data-raw"
# setwd(root_dir)
# dir.create("./regions")
# 
# 
# #### This function loads Brazilian stantes for an specified year {geobr::read_states} and
# #### and generates the sf boundaries of region
# prep_region <- function(year){
# 
#   y <- year
# 
#   # create year folder to save clean data
#   destdir <- paste0("./regions/",y)
#   dir.create(destdir)
# 
#   # a) reads all states sf files and pile them up
#   sf_states <- geobr::read_state(code_state = "all", year = y, simplified = F)
# 
#   # remove wrong-coded regions
#   sf_states <- subset(sf_states, code_region %in% c(1:5))
# 
# 
# # store original crs
#   original_crs <- st_crs(sf_states)
# 
#   # b) make sure we have valid geometries
#   temp_sf <- sf::st_make_valid(sf_states)
#   temp_sf <- temp_sf %>% st_buffer(0)
# 
#   sf_states1 <- to_multipolygon(temp_sf)
# 
# 
# ## Dissolve each region
# all_regions <- dissolve_polygons(mysf=temp_sf, group_column='code_region')
# 
# 
# ### add region names
# all_regions <- add_region_info(temp_sf = all_regions, column = 'code_region')
# all_regions <- select(all_regions, c('code_region', 'name_region', 'geometry'))
# 
# 
# 
# 
# ###### 7. generate a lighter version of the dataset with simplified borders -----------------
#   # skip this step if the dataset is made of points, regular spatial grids or rater data
# 
#   # simplify
#   temp_sf7 <- simplify_temp_sf(all_regions)
# 
# ###### convert to MULTIPOLYGON
# all_regions <- to_multipolygon(all_regions)
# temp_sf7 <- to_multipolygon(temp_sf7)
# 
#   # Save cleaned sf in the cleaned directory
#   sf::st_write(all_regions, dsn= paste0(destdir,"/regions_",y,".gpkg"))
#   sf::st_write(temp_sf7, dsn= paste0(destdir,"/regions_",y,"_simplified", ".gpkg"))
# 
# }
# 
# 
# 
# # Aplica para diferentes anos
# my_years <- c(2000, 2001, 2010, 2013, 2014, 2015, 2016, 2017, 2018)
# 
# prep_region(2020)
# 
# # Parallel processing using future.apply
# future::plan(future::multiprocess)
# future.apply::future_lapply(X =my_years, FUN=prep_region, future.packages=c('readr', 'sp', 'sf', 'dplyr', 'geobr'))
# 
