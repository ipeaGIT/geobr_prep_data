#> DATASET: Immediate Geographic Regions - 2024
#> Source: IBGE - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html?=&t=o-que-e
#> scale 1:250.000
#> Metadata:
# Título: Regiões Geográficas Imediatas
# Título alternativo:
# Frequência de atualização: decenal
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: Utf-8
#
# Resumo: Regiões Geográficas Imediatas foram criadas pelo IBGE em 2017 para substituir as micro-regiões
#
# Estado: Em desenvolvimento
# Palavras chaves descritivas:****
# Informacao do Sistema de Referencia: SIRGAS 2000

# Observações: 
# Anos disponíveis: ****************

### Libraries (use any library as necessary) --------

# library(RCurl)
# library(stringr)
# library(sf)
# library(janitor)
# library(dplyr)
# library(readr)
# library(parallel) # não existe no cran
# library(data.table)
# library(xlsx)
# library(magrittr)
# library(devtools)
# library(lwgeom)
# library(stringi)
# library(targets)
# library(tidyverse)
# library(mirai)
# library(rvest)
# source("./R/support_harmonize_geobr.R")
# source("./R/support_fun.R")


####### Download the data  -----------------
download_immediateregions <- function(year){ # year = 2024

  ###### 0. Get the correct url and file names -----------------
  
  # Year before 2016 ----
  # If the year is pre 2016, we don't have BR files
  
  
  # Year after 2016 ----
  # If the year is post 2016, we have BR files
  
  # if(year >= 2016) {
  # 
  # inicio_url <- "ftp://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_"
  # url <- paste0(inicio_url, year, "/Brasil/BR/br_regioes_geograficas_imediatas.zip")
  # }
  
  ###### 1. Generate the correct ftp link ----
 
  
  #2019 ----
  if(year == 2019) {
    ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2019/Brasil/BR/br_regioes_geograficas_imediatas.zip"
  }
  
  #2020 ----
  if(year == 2020) {
    ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2020/Brasil/BR/BR_RG_Imediatas_2020.zip"
  }
  
  #2021 ----
  if(year == 2021) {
    ftp_link <-  "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2021/Brasil/BR/BR_RG_Imediatas_2021.zip"
  }
  
  #2022 ----
  if(year == 2022) {
    ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/Brasil/BR/BR_RG_Imediatas_2022.zip"
  }
  
  #2023 ----
  if(year == 2023) {
    ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2023/Brasil/BR_RG_Imediatas_2023.zip"
  }
  
  #2024 ----
    if(year == 2024) {
    ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2024/Brasil/BR_RG_Imediatas_2024.zip"
    }

    
   ###### 2. Create temp folder -----------------

  zip_dir <- paste0(tempdir(), "/immediate_regions/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)

  ###### 3. Create direction for each download
  
  # zip folder
  in_zip <- paste0(zip_dir, "/zipped/")
  dir.create(in_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(in_zip)
  
  file_raw <- fs::file_temp(tmp_dir = in_zip,
                            ext = fs::path_ext(ftp_link))
  
  ###### 3. Download Raw data -----------------
  
  if(year < 2015) {
    # Download zipped files
    for (name_file in filenames) {
      download.file(ftp_link[name_file],
                    paste(in_zip, name_file, sep = "\\"))
    }
  }
  
  if(year >= 2015) {
    httr::GET(url = ftp_link,
              httr::progress(),
              httr::write_disk(path = file_raw,
                               overwrite = T))
  }
  
  # Download zipped files
  
  
  ###### 4. Unzip Raw data -----------------
  
  # directory of zips
  zip_names <- list.files(in_zip, pattern = "\\.zip", full.names = TRUE)
  
  # unzip folder
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  if (length(zip_names) == 1) {
    unzip(zipfile = zip_names,
          exdir = out_zip)
    
  }
  
  if (length(zip_names) > 1) {
    pbapply::pblapply(
      X = zip_names,
      FUN = function(x){ unzip(zipfile = x, exdir = out_zip) }
    )
  }
  
  ###### 5. Bind Raw data together -----------------
  
  shp_names <- list.files(out_zip, pattern = "\\.shp$", full.names = TRUE)
  
  immediateregions_list <- pbapply::pblapply(
    X = shp_names, 
    FUN = function(x){ sf::st_read(x, quiet = T, stringsAsFactors=F) }
  )
  
  immediateregions_raw <- data.table::rbindlist(immediateregions_list)
  
  ###### 6. Show result -----------------
  
  data.table::setDF(immediateregions_raw)
  immediateregions_raw <- sf::st_as_sf(immediateregions_raw) %>% 
    clean_names()
  
  return(immediateregions_raw)
}

# ####### Clean the data  -----------------
clean_immediateregions <- function(immediateregions_raw, year){ # year = 2024

  ###### 0. Create folder to save clean data -----

  dir_clean <- paste0("./data/immediate_regions/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ###### 1. Rename collumns names -----
  
  
  ###### 2. Apply harmonize geobr cleaning -----------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = immediateregions_raw,
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
  
  ###### 3. lighter version --------------- 
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ###### 4. Save datasets  -----------------
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/immediateregions_",  year,
  #                                   ".gpkg"), delete_dsn = TRUE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean, "/immediateregions_",
  #                                               year, "_simplified.gpkg"),
  #              delete_dsn = TRUE )

  # Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/immediateregions_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 22
  )
  
  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean,"/immediateregions_", year, "_simplified", ".parquet"),
    compression='zstd',
    compression_level = 22
  )
  
  return(dir_clean)
}


########################## OLD FILE BELOW HERE ##########

# 
# 
# ###### 1. download the raw data from the original website source -----------------
# 
# if(update == 2019){
#   ftp <- "ftp://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2019/Brasil/BR/br_regioes_geograficas_imediatas.zip"
# 
#   download.file(url = ftp, destfile = "RG2019_rgi_20190430.zip")
# }
# if(update == 2017){
#   ftp <- "ftp://geoftp.ibge.gov.br/organizacao_do_territorio/divisao_regional/divisao_regional_do_brasil/divisao_regional_do_brasil_em_regioes_geograficas_2017/shp/RG2017_rgi_20180911.zip"
# 
#   download.file(url = ftp, destfile = "RG2017_rgi_20180911.zip")
# }
# 
# 
# 
# 
# ###### 1.1. Unzip data files if necessary -----------------
# 
# if(update == 2019){
#   unzip("RG2019_rgi_20190430.zip")
# }
# if(update == 2017){
#   unzip("RG2017_rgi_20180911.zip")
# }
# 
# 
# ###### Unzip raw data --------------------------------
# unzip_to_geopackage(region='imediata', year=2020)
# 
# 
# 
# 
# ###### Cleaning UF files --------------------------------
# setwd('L:/# DIRUR #/ASMEQ/geobr/data-raw/malhas_municipais')
# 
# # get folders with years
# data_dir <-  paste0(getwd(),"./shapes_in_sf_all_years_original/imediata")
# sub_dirs <- list.dirs(path =data_dir, recursive = F)
# sub_dirs <- sub_dirs[sub_dirs %like% paste0(2000:2020,collapse = "|")]
# 
# 
# 
# 
# # ###### 2. rename column names -----------------
# #
# # # read data
# # if(update == 2019){
# #   temp_sf <- st_read("BR_RG_Imediatas_2019.shp", quiet = F, stringsAsFactors=F, options = "ENCODING=UTF8")
# #
# #   temp_sf <- dplyr::rename(temp_sf, code_immediate = CD_RGI, name_immediate = NM_RGI)
# # }
# #
# # if(update == 2017){
# #   temp_sf <- st_read("RG2017_rgi.shp", quiet = F, stringsAsFactors=F, options = "ENCODING=UTF8")
# #
# #   temp_sf <- dplyr::rename(temp_sf, code_immediate = rgi, name_immediate = nome_rgi)
# # }
# #
# # # reorder columns
# # temp_sf <- dplyr::select(temp_sf, 'code_immediate', 'name_immediate','code_state', 'abbrev_state',
# #                          'name_state', 'code_region', 'name_region', 'geometry')
# 
# 
# 
# # create a function that will clean the sf files according to particularities of the data in each year
# clean_immediate <- function( e , year){ #  e <- sub_dirs[ sub_dirs %like% 2020]
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
#   dir.create(file.path(paste0("shapes_in_sf_all_years_cleaned2/imediata/",year)), showWarnings = FALSE, recursive = T)
#   gc(reset = T)
# 
#   dir.dest <- file.path(paste0("./shapes_in_sf_all_years_cleaned2/imediata/",year))
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
#       temp_sf <- dplyr::select(temp_sf, c('code_immediate'=cd_rgi,
#                                           'name_immediate'=nm_rgi,
#                                           'abbrev_state'=sigla_uf,
#                                           'geom'))
#     }
# 
#     # Use UTF-8 encoding
#     temp_sf <- use_encoding_utf8(temp_sf)
# 
#     # add name_state
#     temp_sf$code_state <- substring(temp_sf$code_immediate, 1,2)
#     temp_sf <- add_state_info(temp_sf,column = 'code_state')
# 
#     # reorder columns
#     temp_sf <- dplyr::select(temp_sf, 'code_immediate', 'name_immediate', 'code_state', 'abbrev_state', 'name_state', 'geom')
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
#     file.name <- paste0("immediate_regions_",year,".gpkg")
# 
#       # original
#       i <- paste0(dir.dest.file,file.name)
#       sf::st_write(temp_sf, i, overwrite=TRUE)
# 
#       # simplified
#       i <- gsub(".gpkg", "_simplified.gpkg", i)
#       sf::st_write(temp_sf_simplified, i, overwrite=TRUE)
#     }
# }
# 
# 
# 
# # apply function in parallel
# future::plan(multisession)
# future_map(.x=sub_dirs, .f=clean_immediate, year=2020)
# 
# rm(list= ls())
# gc(reset = T)



