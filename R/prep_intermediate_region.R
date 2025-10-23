#> DATASET: Intermediate Geographic Regions
#> Source: IBGE - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html?=&t=o-que-e
#> scale 1:250.000 ?????????????
#> Metadata:
# Título: Regiões Geográficas Intermediárias
# Titulo alternativo: intermediate regions
# Frequencia de atualizacao: decenal
#
# Forma de apresentacao: Shape
# Linguagem: Pt-BR
# Character set: Utf-8
#
# Resumo: Regiões Geográficas Intermediárias foram criadas pelo IBGE em 2017 para substituir a mesorregiões
#
# Estado: Em desenvolvimento
# Palavras chaves descritivas:****
# Informacao do Sistema de Referencia: SIRGAS 2000

### Libraries (use any library as necessary) ----

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

# Download the data  ----
download_intermediateregions <- function(year){ # year = 2024
  
  ## 0. Generate the correct ftp link (UPDATE YEAR HERE) ----
  
  ftp_start <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_"
    
  if(year == 2019) {
    ftp_link <- paste0(ftp_start, year, "/Brasil/BR/br_regioes_geograficas_intermediarias.zip")
  }
  
  if(year == 2020) {
    ftp_link <- paste0(ftp_start, year, "/Brasil/BR/BR_RG_Intermediarias_2020.zip")
  }
  
  if(year == 2021) {
    ftp_link <- paste0(ftp_start, year, "/Brasil/BR/BR_RG_Intermediarias_2021.zip")
  }
  
  if(year == 2022) {
    ftp_link <- paste0(ftp_start, year, "/Brasil/BR/BR_RG_Intermediarias_2022.zip")
  }
  
  if(year == 2023) {
    ftp_link <- paste0(ftp_start, year, "/Brasil/BR_RG_Intermediarias_2023.zip")
  }
  
  if(year == 2024) {
    ftp_link <- paste0(ftp_start, year, "/Brasil/BR_RG_Intermediarias_2024.zip")
  }
  
  ## 1. Create temp folder ----
  
  zip_dir <- paste0(tempdir(), "/intermediate_regions/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  ### Alternative folder
  # zip_dir <- paste0("./data_raw/", "/intermediate_regions/", year)
  # dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  # dir.exists(zip_dir)
  
  ## 2. Create direction for each download ----
  
  # zip folder
  in_zip <- paste0(zip_dir, "/zipped/")
  dir.create(in_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(in_zip)
  
  file_raw <- fs::file_temp(tmp_dir = in_zip,
                            ext = fs::path_ext(ftp_link))
  
  #filenames <- basename(ftp_link)
  
  ## 3. Download Raw data ----
  
  httr::GET(url = ftp_link,
            httr::progress(),
            httr::write_disk(path = file_raw,
                             overwrite = T))
  
  ## 4. Unzip Raw data ----
  
  ### unzip folder
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  unzip_geobr(zip_dir = zip_dir, in_zip = in_zip, out_zip = out_zip, is_shp = TRUE)
  
  ## 5. Bind Raw data together ----
  
  in_zip <- paste0(zip_dir, "/zipped/")
  dir.create(in_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(in_zip)
  
  shp_names <- list.files(out_zip, pattern = "\\.shp$",
                          full.names = TRUE)
  
  intermediateregions_list <- pbapply::pblapply(
    X = shp_names, 
    FUN = function(x){ sf::st_read(x, quiet = T, stringsAsFactors=F) }
  )
  
  intermediateregions_raw <- data.table::rbindlist(intermediateregions_list)
  
  ## 6. Show result ----
  
  data.table::setDF(intermediateregions_raw)
  intermediateregions_raw <- sf::st_as_sf(intermediateregions_raw) %>% 
    clean_names()
  
  return(intermediateregions_raw)
  
}

# Clean the data  ----
clean_intermediateregions <- function(intermediateregions_raw, year){ # year = 2024
  
  ## 0. Create folder to save clean data ----
  
  dir_clean <- paste0("./data/intermediate_regions/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. Rename collumns names ----
  
  
  ## 2. Apply harmonize geobr cleaning ----
  
  temp_sf <- harmonize_geobr(
    temp_sf = intermediateregions_raw,
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
  
  ## 3. lighter version ---- 
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 4. Save datasets  ----
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/intermediateregions_",  year,
  #                                    ".gpkg"), delete_dsn = TRUE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean,
  #                                               "/intermediateregions_",
  #                                               year, "_simplified.gpkg"),
  #              delete_dsn = TRUE )
  
  # Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/intermediateregions_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 22
  )
  
  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean,"/intermediateregions_", year, "_simplified",
                  ".parquet"),
    compression='zstd',
    compression_level = 22
  )
  
  return(dir_clean)
}

################ RAPHAEL OLD CODE BELOW HERE #############

# ####### Load Support functions to use in the preprocessing of the data -----------------
# source("./prep_data/prep_functions.R")
# source('./prep_data/download_malhas_municipais_function.R')
# 
# 
# # If the data set is updated regularly, you should create a function that will have
# # a `date` argument download the data
# 
# update <- 2020
# 
# 
# # Root directory
# root_dir <- "L:////# DIRUR #//ASMEQ//geobr//data-raw"
# setwd(root_dir)
# 
# # Directory to keep raw zipped files
# dir.create("./intermediate_regions")
# setwd("./intermediate_regions")
# 
# 
# 
# # Create folders to save clean sf.rds files
# destdir_clean <- paste0("./shapes_in_sf_cleaned/",update)
# dir.create( destdir_clean , showWarnings = FALSE)
# 
# 
# 
# #### 0. Download original Intermediate Regions data sets from IBGE ftp -----------------
# 
# if(update == 2019){
#   ftp <- 'ftp://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2019/Brasil/BR/br_regioes_geograficas_intermediarias.zip'
#   download.file(url = ftp, destfile = "RG2019_rgint_20190430.zip")
# }
# 
# if(update == 2017){
#   ftp <- 'ftp://geoftp.ibge.gov.br/organizacao_do_territorio/divisao_regional/divisao_regional_do_brasil/divisao_regional_do_brasil_em_regioes_geograficas_2017/shp/RG2017_rgint_20180911.zip'
#   download.file(url = ftp, destfile = "RG2017_rgint_20180911.zip")
# }
# 
# 
# ########  1. Unzip original data sets downloaded from IBGE -----------------
# 
# if(update == 2019){
#   unzip("RG2019_rgint_20190430.zip")
# }
# 
# if(update == 2017){
#   unzip("RG2017_rgint_20180911.zip")
# }
# 
# 
# ###### Unzip raw data --------------------------------
# unzip_to_geopackage(region='intermediaria', year=2020)
# 
# 
# 
# ###### Cleaning UF files --------------------------------
# setwd('L:/# DIRUR #/ASMEQ/geobr/data-raw/malhas_municipais')
# 
# # get folders with years
# data_dir <-  paste0(getwd(),"./shapes_in_sf_all_years_original/intermediaria")
# sub_dirs <- list.dirs(path =data_dir, recursive = F)
# sub_dirs <- sub_dirs[sub_dirs %like% paste0(2000:2020,collapse = "|")]
# 
# 
# 
# 
# 
# ##### 2. Rename columns -------------------------
# #
# # # read data
# # if(update == 2019){
# #   temp_sf <- st_read("BR_RG_Intermediarias_2019.shp", quiet = F, stringsAsFactors=F, options = "ENCODING=UTF8")
# #
# #   temp_sf <- dplyr::rename(temp_sf, code_intermediate = CD_RGINT, name_intermediate = NM_RGINT)
# # }
# #
# # if(update == 2017){
# #   temp_sf <- st_read("RG2017_rgint.shp", quiet = F, stringsAsFactors=F, options = "ENCODING=UTF8")
# #
# #   temp_sf <- dplyr::rename(temp_sf, code_intermediate = rgint, name_intermediate = nome_rgint)
# # }
# #
# # # reorder columns
# # temp_sf <- dplyr::select(temp_sf, 'code_intermediate', 'name_intermediate','code_state', 'abbrev_state',
# #                          'name_state', 'code_region', 'name_region', 'geometry')
# 
# 
# 
# # create a function that will clean the sf files according to particularities of the data in each year
# clean_intermediate <- function( e , year){ #  e <- sub_dirs[ sub_dirs %like% 2020]
# 
#   # select year
#   if (year == 'all') {
#     message(paste('Processing all years'))
#   } else{
#     if (!any(e %like% year)) {
#       return(NULL)
#     }
#   }
# 
#   message(paste('Processing',year))
# 
#   options(encoding = "UTF-8")
# 
#   # get year of the folder
#   last4 <- function(x){substr(x, nchar(x)-3, nchar(x))}   # function to get the last 4 digits of a string
#   year <- last4(e)
#   year
# 
#   # create a subdirectory of years
#   dir.create(file.path(paste0("shapes_in_sf_all_years_cleaned2/intermediaria/",year)), showWarnings = FALSE, recursive = T)
#   gc(reset = T)
# 
#   dir.dest <- file.path(paste0("./shapes_in_sf_all_years_cleaned2/intermediaria/",year))
# 
# 
#   # list all sf files in that year/folder
#   sf_files <- list.files(e, full.names = T, recursive = T, pattern = ".gpkg$")
# 
#   #sf_files <- sf_files[sf_files %like% "Microrregioes"]
# 
#   # for each file
#   for (i in sf_files){ #  i <- sf_files[1]
# 
#     # read sf file
#     temp_sf <- st_read(i)
#     names(temp_sf) <- names(temp_sf) %>% tolower()
#     head(temp_sf)
# 
# 
#     if (year %like% "2020"){
#       # dplyr::rename and subset columns
#       temp_sf <- dplyr::select(temp_sf, c('code_intermediate'=cd_rgint,
#                                           'name_intermediate'=nm_rgint,
#                                           'abbrev_state'=sigla_uf,
#                                           'geom'))
#     }
# 
# 
#     # add name_state
#     temp_sf$code_state <- substring(temp_sf$code_intermediate, 1,2)
#     temp_sf <- add_state_info(temp_sf,column = 'code_state')
# 
#     # Use UTF-8 encoding
#     options(encoding = "UTF-8")
#     temp_sf <- use_encoding_utf8(temp_sf)
#     head(temp_sf)
# 
#     # reorder columns
#     temp_sf <- dplyr::select(temp_sf, 'code_intermediate', 'name_intermediate', 'code_state', 'abbrev_state', 'name_state', 'geom')
# 
# 
# 
#     # remove Z dimension of spatial data
#     temp_sf <- temp_sf %>% st_sf() %>% st_zm( drop = T, what = "ZM")
#     head(temp_sf)
# 
#     # Harmonize spatial projection CRS, using SIRGAS 2000 epsg (SRID): 4674
#     temp_sf <- harmonize_projection(temp_sf)
# 
# 
#     # Make an invalid geometry valid # st_is_valid( sf)
#     temp_sf <- sf::st_make_valid(temp_sf)
# 
# 
#     # simplify
#     temp_sf_simplified <- simplify_temp_sf(temp_sf)
# 
#     # convert to MULTIPOLYGON
#     temp_sf <- to_multipolygon(temp_sf)
#     temp_sf_simplified <- to_multipolygon(temp_sf_simplified)
# 
#     # Save cleaned sf in the cleaned directory
#     dir.dest.file <- paste0(dir.dest,"/")
#     file.name <- paste0("intermediate_regions_",year,".gpkg")
# 
#     # original
#     i <- paste0(dir.dest.file,file.name)
#     sf::st_write(temp_sf, i, overwrite=TRUE)
# 
#     # simplified
#     i <- gsub(".gpkg", "_simplified.gpkg", i)
#     sf::st_write(temp_sf_simplified, i, overwrite=TRUE)
#   }
# }
# 
# 
# 
# # apply function in parallel
# future::plan(multisession)
# future_map(.x=sub_dirs, .f=clean_intermediate, year=2020)
# 
# rm(list= ls())
# gc(reset = T)
# 