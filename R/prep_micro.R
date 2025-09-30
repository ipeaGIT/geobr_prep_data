#> DATASET: Micro Geographic Regions - 2017
#> Source: IBGE - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html?=&t=o-que-e
#> scale 1:250.000
#> Metadata:
# Título: Microrregiões Geográficas
# Título alternativo: micro regions
# Frequência de atualização: anual, encerrado em 2017.
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: Utf-8
#
# Resumo: Micro regiões Geográficas foram criadas pelo IBGE em 2000. Em 2017 o IBGE substituiu o conceito pelas regiões imediatas.
#
# Estado: Em desenvolvimento
# Palavras chaves descritivas:****
# Informacao do Sistema de Referencia: SIRGAS 2000

# Observações: 
# Anos disponíveis: 2000 a 2014

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
download_microregions <- function(year){ # year = 2024
  
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
  
  if(year %in% c(2000:2014)) {
    # create states tibble
    states <- tibble(cod_states = c(11, 12, 13, 14, 15, 16, 17, 21, 22, 23, 24,
                                    25, 26, 27, 28, 29, 31, 32, 33, 35, 41, 42,
                                    43, 50, 51, 52, 53),
                     sg_state = c("RO", "AC", "AM", "RR", "PA", "AP", "TO",
                                  "MA", "PI", "CE", "RN", "PB", "PE", "AL",
                                  "SE", "BA", "MG", "ES", "RJ", "SP", "PR",
                                  "SC", "RS", "MS", "MT", "GO", "DF"),
                     sgm_state = str_to_lower(sg_state))
    
    # parts of url
    url_start <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_"
    ftp_link <- paste0(url_start, year, "/", states$sg_state, "/", states$sgm_state, "_microrregioes.zip")
    
    filenames <- basename(ftp_link)
    
    names(ftp_link) <- filenames
  }
  
  ###### FTPs links by state
  
  # #2000 ----
  # if(year == 2000) {
  # ftp_link <- ""
  #   }
  # 
  # #2001 ----
  # if(year == 2001) {
  #   ftp_link <- ""
  # }
  # 
  # #2005 ----
  # if(year == 2005) {
  #   ftp_link <- ""
  # }
  # 
  # #2007 ----
  # if(year == 2007) {
  #   ftp_link <- ""
  # }
  # 
  # #2010 ----
  # if(year == 2010) {
  #   ftp_link <- ""
  # }
  # 
  # #2013 ----
  # if(year == 2013) {
  #   ftp_link <- ""
  # }
  # 
  # #2014 ----
  # if(year == 2014) {
  #   ftp_link <- ""
  # }
  
  ####### Ftp links com BR
  
  #2015 ----
  if(year == 2015) {
    ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2015/Brasil/BR/br_microrregioes.zip"
  }
  
  #2016 ----
  if(year == 2016) {
    ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2016/Brasil/BR/br_microrregioes.zip"
  }
  
  #2017 ----
  if(year == 2017) {
    ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2017/Brasil/BR/br_microrregioes.zip"
  }
  
  #2018 ----
  if(year == 2018) {
    ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2018/Brasil/BR/br_microrregioes.zip"
  }
  
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
  
  
  #   # Url final----
  #   url <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2000/ac/ac_microrregioes.zip"
  #   
  #   
  #   url <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2024/Brasil/BR_RG_Imediatas_2024.zip"
  # }
  # 
  # if(year == 2024) {
  # url <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2024/Brasil/BR_RG_Imediatas_2024.zip"
  # }
  
  ###### 2. Create temp folder -----------------
  
  zip_dir <- paste0(tempdir(), "/micro_regions/", year)
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
  
  microregions_list <- pbapply::pblapply(
    X = shp_names, 
    FUN = function(x){ sf::st_read(x, quiet = T, stringsAsFactors=F) }
  )
  
  microregions_raw <- data.table::rbindlist(microregions_list)
  
  ###### 6. Show result -----------------
  
  data.table::setDF(microregions_raw)
  microregions_raw <- sf::st_as_sf(microregions_raw) %>% 
    clean_names()
  
  return(microregions_raw)
}

# ####### Clean the data  -----------------
clean_microregions <- function(microregions_raw, year){ # year = 2024
  
  ###### 0. Create folder to save clean data -----
  
  dir_clean <- paste0("./data/micro_regions/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ###### 1. Rename collumns names -----
  
  
  ###### 2. Apply harmonize geobr cleaning -----------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = microregions_raw,
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
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/microregions_",  year,
  #                                   ".gpkg"), delete_dsn = TRUE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean, "/microregions_",
  #                                               year, "_simplified.gpkg"),
  #              delete_dsn = TRUE )
  
  # Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/microregions_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 22
  )
  
  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean,"/microregions_", year, "_simplified", ".parquet"),
    compression='zstd',
    compression_level = 22
  )
  
  return(dir_clean)
}



################ RAFAEL OLD CODE BELOW --------------

####### Load Support functions to use in the preprocessing of the data
# 
# source("./prep_data/prep_functions.R")
# source('./prep_data/download_malhas_municipais_function.R')
# 
# setwd('L:/# DIRUR #/ASMEQ/geobr/data-raw')
# 
# #pblapply(X=c(2000,2001,2005,2007,2010,2013:2020), FUN=download_ibge)
# 
# ###### download raw data --------------------------------
# # unzip_to_geopackage(region='micro_regiao', year='2019')
# unzip_to_geopackage(region='micro_regiao', year='all')
# 
# 
# ###### Cleaning MICRO files --------------------------------
# setwd('L:/# DIRUR #/ASMEQ/geobr/data-raw/malhas_municipais')
# 
# micro_dir <- paste0(getwd(),"/shapes_in_sf_all_years_original/micro_regiao")
# 
# sub_dirs <- list.dirs(path=micro_dir, recursive = F)
# 
# sub_dirs <- sub_dirs[sub_dirs %like% paste0(2000:2020,collapse = "|")]
# 
# # sub_dirs <- sub_dirs[sub_dirs %like% 2019]
# 
# # create a function that will clean the sf files according to particularities of the data in each year0
# clean_micro <- function( e , year){ #  e <- sub_dirs[ sub_dirs %like% 2000]
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
#   # create directory to save original shape files in sf format
#   dir.create(file.path("shapes_in_sf_all_years_cleaned2"), showWarnings = FALSE)
# 
#   # create a subdirectory of states, municipalities, micro and meso regions
#   dir.create(file.path("shapes_in_sf_all_years_cleaned2/micro_regiao/"), showWarnings = FALSE)
# 
#   # create a subdirectory of years
#   dir.create(file.path(paste0("shapes_in_sf_all_years_cleaned2/micro_regiao/",year)), showWarnings = FALSE)
#   gc(reset = T)
# 
#   dir.dest <- file.path(paste0("./shapes_in_sf_all_years_cleaned2/micro_regiao/",year))
# 
# 
#   # list all sf files in that year/folder
#   sf_files <- list.files(e, full.names = T, recursive = T, pattern = ".gpkg$")
# 
#   #sf_files <- sf_files[sf_files %like% "Microrregioes"]
# 
#   # for each file
#   for (i in sf_files){ #  i <- sf_files[8]
# 
#     # read sf file
#     temp_sf <- st_read(i)
#     names(temp_sf) <- names(temp_sf) %>% tolower()
# 
# 
#     if (year %like% "2000|2001"){
#       # dplyr::rename and subset columns
#       temp_sf <- dplyr::select(temp_sf, c('code_micro'= geocodigo, 'name_micro'=nome, 'geom'))
#     }
# 
# 
#     if (year %like% "2010"){
#       # dplyr::rename and subset columns
#       temp_sf <- dplyr::select(temp_sf, c('code_micro'=cd_geocodu, 'name_micro'=nm_micro, 'geom'))
#      }
# 
#     if (year %like% "2013|2014|2015|2016|2017|2018"){
#       # dplyr::rename and subset columns
#       temp_sf <- dplyr::select(temp_sf, c('code_micro'=cd_geocmi, 'name_micro'=nm_micro, 'geom'))
#     }
# 
#     if (year %like% "2019|2020"){
#       # dplyr::rename and subset columns
#       temp_sf <- dplyr::select(temp_sf, c('code_micro'=cd_micro, 'name_micro'=nm_micro, 'abbrev_state'=sigla_uf, 'geom'))
#     }
# 
#     # Use UTF-8 encoding
#     temp_sf <- use_encoding_utf8(temp_sf)
# 
# 
#     # add name_state
#     temp_sf$code_state <- substring(temp_sf$code_micro, 1,2)
#     temp_sf <- add_state_info(temp_sf,column = 'code_micro')
# 
#     # reorder columns
#     temp_sf <- dplyr::select(temp_sf, 'code_state', 'abbrev_state', 'name_state', 'code_micro', 'name_micro', 'geom')
# 
#     # Capitalize the first letter
#     temp_sf$name_micro <- stringr::str_to_title(temp_sf$name_micro)
# 
#     # Harmonize spatial projection CRS, using SIRGAS 2000 epsg (SRID): 4674
#     temp_sf <- harmonize_projection(temp_sf)
# 
#     # strange error in Bahia 2000
#     # remove geometries with area == 0
#     temp_sf <- temp_sf[ as.numeric(st_area(temp_sf)) != 0, ]
# 
#     # strange error in Maranhao 2000
#     # micro_21 <- geobr::read_micro_region(code_micro = 21, year=2000)
#     # mapview(micro_21) + temp_sf[c(7),]
# 
#     if (year==2000 & temp_sf$code_state[1]==21) {
#     temp_sf[3, c('code_state', 'abbrev_state', 'name_state', 'code_micro', 'name_micro')] <- c(21, 'MA', 'Maranhão', 210520, 'Gerais De Balsas' )
#     temp_sf[7, c('code_state', 'abbrev_state', 'name_state', 'code_micro', 'name_micro')] <- c(21, 'MA', 'Maranhão', 210521, 'Chapadas Das Mangabeiras' )
#     }
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
# 
#     # save each state separately
#     for( c in unique(temp_sf$code_state)){ # c <- 11
# 
#       temp2 <- subset(temp_sf, code_state ==c)
#       temp2_simplified <- subset(temp_sf_simplified, code_state ==c)
# 
#       file.name <- paste0(unique(substr(temp2$code_state,1,2)),"MI",".gpkg")
# 
#       # original
#       i <- paste0(dir.dest.file,file.name)
#       sf::st_write(temp2, i, overwrite=TRUE)
# 
#       # simplified
#       i <- gsub(".gpkg", "_simplified.gpkg", i)
#       sf::st_write(temp2_simplified, i, overwrite=TRUE)
#     }
# 
# 
#   }
# }
# 
# 
# # apply function in parallel
# future::plan(multisession)
# future_map(.x=sub_dirs, .f=clean_micro, year=2010)
# 
# rm(list= ls())
# gc(reset = T)
# 
# 
# ###### Correcting number of digits of micro regions in 2010  --------------------------------
# # issue #20
# # use data of 2013 to add code and name of micro regions in the 2010 data
# 
# # Dirs
# micro_dir <- "L:////# DIRUR #//ASMEQ//geobr//data-raw//malhas_municipais//shapes_in_sf_all_years_cleaned2/micro_regiao"
# sub_dirs <- list.dirs(path =micro_dir, recursive = F)
# 
# # dirs of 2010 (problematic data) ad 2013 (reference data)
# sub_dir_2010 <- sub_dirs[sub_dirs %like% 2010]
# sub_dir_2013 <- sub_dirs[sub_dirs %like% 2013]
# 
# 
# # list sf files in each dir
# sf_files_2010 <- list.files(sub_dir_2010, full.names = T, pattern = ".gpkg")
# sf_files_2013 <- list.files(sub_dir_2013, full.names = T, pattern = ".gpkg")
# 
# 
# # Create function to correct number of digits of meso regions in 2010, based on 2013 data
# correct_micro_digits <- function(a2010_sf_micro_file){ # a2010_sf_micro_file <- sf_files_2010[39]
# 
# 
#   # Get UF of the file
#   get_uf <- function(x){if (grepl("simplified",x)) {
#     substr(x, nchar(x)-19, nchar(x)-18)
#   } else {substr(x, nchar(x)-8, nchar(x)-7)}
#   }
#   uf <- get_uf(a2010_sf_micro_file)
# 
#   # read 2010 file
#   temp2010 <- st_read(a2010_sf_micro_file)
# 
#   # dplyr::rename and subset columns
#   temp2010 <- temp2010 %>%  dplyr::mutate(name_micro =as.character(name_micro))
#   temp2010 <- temp2010 %>%  dplyr::mutate(name_micro = ifelse(name_micro == "Moji Das Cruzes","Mogi Das Cruzes",
#                                                               ifelse(name_micro == "Piraçununga","Pirassununga",
#                                                                      ifelse(name_micro == "Moji-Mirim","Moji Mirim",
#                                                                             ifelse(name_micro == "São Miguel D'oeste","São Miguel Do Oeste",
#                                                                                    ifelse(name_micro == "Serras Do Sudeste","Serras De Sudeste",
#                                                                                           ifelse(name_micro == "Vão Do Paraná","Vão Do Paranã",name_micro)))))))
# 
# 
#   # read 2013 file
#   temp2013 <- sf_files_2013[if (grepl("simplified",a2010_sf_micro_file)) {
#     (sf_files_2013 %like% paste0("/",uf)) & (sf_files_2013 %like% "simplified")
#   } else {
#     (sf_files_2013 %like% paste0("/",uf)) & !(sf_files_2013 %like% "simplified")
#   }]
#   temp2013 <- st_read(temp2013)
# 
#   # keep only code and name columns
#   table2013 <- temp2013 %>% as.data.frame()
#   table2013 <- dplyr::select(table2013, code_micro, name_micro)
# 
#   # update code_micro
#     # subset(temp2010, name_micro %like% 'Moji')
#     # subset(temp2010, name_micro %like% 'Mogi')
# 
#   sf2010 <- left_join(temp2010, table2013, by="name_micro")
#   sf2010 <- dplyr::select(sf2010, code_state, abbrev_state, name_state, code_micro=code_micro.y, name_micro, geom)
#   head(sf2010)
# 
#   # Save file
#   # write_rds(sf2010, path = a2010_sf_micro_file, compress="gz" )
#   st_write(sf2010,a2010_sf_micro_file,append = FALSE,delete_dsn =T,delete_layer=T)
# }
# 
# 
# # apply function in parallel
# future::plan(multisession)
# future_map(sf_files_2010, correct_micro_digits)


