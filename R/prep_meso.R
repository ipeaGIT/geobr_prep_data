#> DATASET: Meso Geographic Regions
#> Source: IBGE - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html?=&t=o-que-e
#> scale 1:250.000 ?????????????
#> Metadata:
# Título: Mesorregiões Geográficas
# Título alternativo: meso regions
# Frequência de atualização: decenal /// anual, encerrado em 2017
#
# Forma de apresentacao: Shape
# Linguagem: Pt-BR
# Character set: Utf-8
#
# Resumo: Mesoregiões Geográficas foram criadas pelo IBGE em 2000. Em 2017 o IBGE substituiu o conceito pelas regiões intermediárias.
#
# Estado: Em desenvolvimento
# Palavras chaves descritivas:****
# Informacao do Sistema de Referencia: SIRGAS 2000

# Observações: 
# Anos disponíveis: 2000 a 2018

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

# Download the data  -----------------------------------------------------------
download_mesoregions <- function(year){ # year = 2001
  
  ## 0. Generate the correct ftp link ------------------------------------------
  
  url_start <- paste0("https://geoftp.ibge.gov.br/organizacao_do_territorio/",
                      "malhas_territoriais/malhas_municipais/municipio_")
  
  # Before 2015
  if(year %in% c(2000, 2001, 2010:2014)) {
    ### create states tibble
    states <- states_geobr()
    
    ### parts of url
    
    #2000
    if(year == 2000) {
      ftp_link <- paste0(url_start, year, "/", states$abbrevm_state, "/",
                         states$abbrevm_state, "_mesorregioes.zip")
    }
    
    #2001
    if(year == 2001) {
      ftp_link <- paste0(url_start, year, "/", states$abbrevm_state, "/",
                         states$code_state, "me2500g.zip")
    }
    
    #2010 
    if(year == 2010) {
      ftp_link <- paste0(url_start, year, "/", states$abbrevm_state, "/",
                         states$abbrevm_state, "_mesorregioes.zip")
    }
    
    #2013 
    if(year == 2013) {
      ftp_link <- paste0(url_start, year, "/", states$abbrev_state, "/",
                         states$abbrevm_state, "_mesorregioes.zip")
    }
    
    #2014
    if(year == 2014) {
      ftp_link <- paste0(url_start, year, "/", states$abbrev_state, "/",
                         states$abbrevm_state, "_mesorregioes.zip")
    }
    
    filenames <- basename(ftp_link)
    
    names(ftp_link) <- filenames
  } 
  
  # After 2015
  if(year >= 2015) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR/br_mesorregioes.zip")
  }
  
  ## 1. Create temp folder -----------------------------------------------------
  
  zip_dir <- paste0(tempdir(), "/meso_regions/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  ### Alternative folder
  # zip_dir <- paste0("./data_raw/", "/meso_regions/", year)
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
  
  if(year %in% 2015:2018) {
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
  if (year == 2000) { #years without IBGE errors
    mesoregions_list <- pbapply::pblapply(
      X = shp_names,
      FUN = function(x){ sf::st_read(x, quiet = T, stringsAsFactors= F) }
    )
    
    mesoregions_raw <- data.table::rbindlist(mesoregions_list)
  }
  
  if (year %in% c(2001, 2010:2014))  {#years with error in number of collumns
    mesoregions_raw <- readmerge_geobr(folder_path = out_zip)
    }
  
  #### After 2015
  if (length(shp_names) == 1) {
    mesoregions_raw <- st_read(shp_names, quiet = T, stringsAsFactors= F)
  }
  
  ## 6. Integrity test ---------------------------------------------------------
  
  #### Before 2015
  
  #### After 2015
  if (length(shp_names) == 1) {
    table_collumns <- tibble(name_collum = colnames(mesoregions_raw),
                             type_collum = sapply(mesoregions_raw, class)) |> 
      rownames_to_column(var = "num_collumn")
    
    glimpse(table_collumns)
    glimpse(mesoregions_raw)
  }
  
  ## 7. Show result ------------------------------------------------------------
  
  data.table::setDF(mesoregions_raw)
  mesoregions_raw <- sf::st_as_sf(mesoregions_raw) %>% 
    clean_names()
  
  glimpse(mesoregions_raw)
  
  return(mesoregions_raw)
  
}

# Clean the data  --------------------------------------------------------------
clean_mesoregions <- function(mesoregions_raw, year){ # year = 2024
  
  ## 0. Create folder to save clean data ---------------------------------------
  
  dir_clean <- paste0("./data/meso_regions/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  
  ## 1. Add state, code and region ---------------------------------------------
  
  states <- states_geobr()
  
  mesoregions_raw1 <- mesoregions_raw |> 
    mutate(code_state = str_sub(codigo, start = 1, end = 2)) |>
    left_join(states, by = "code_state")
  
  
  ## 1. Duplicates -------------------------------------------------------------
  
  # Há duplicações?
  get_dupes(mesoregions_raw)
  # Não
  
  # Há duplicações no nome?  
  mesoregions_raw |> 
    count(nome) |> 
    filter(n > 1)
  # Sim
    
  # Há duplicações pela geometria?
  eq_list <- st_equals(mesoregions_raw)
  
  duplicados_geom <- eq_list[lengths(eq_list) > 1]
  duplicados_geom
  
  ## 1. Adjust IBGE errors -----------------------------------------------------
  
  states <- states_geobr()
  
  if (year == 2000) {
    mesoregions <- mesoregions_raw |> 
      mutate(code_state = str_sub(codigo, start = 1, end = 2)) |>
      left_join(states, by = "code_state") |> 
      relocate(where(is.numeric), .before = geometry) |> 
      relocate(geocodigo, codigo, mslink, nome, code_state, abbrev_state, name_state,
               code_region, name_region) |> 
      select(-codigo, -reservado, -abbrevm_state) |> 
      
      # Pegar as shapes que estão com erro "0"
      mutate(zero = case_when(nome == "0" ~ TRUE,
                              TRUE ~ FALSE)) |> 
      arrange(desc(zero)) |> 
      filter(abbrev_state %in% c("MA", "PA", "TO", "PI") | zero == TRUE) |> 
      select(nome, zero, abbrev_state)
      
      plot(mesoregions)
      # Tem duas shapes vazias de dados 
      
  }
      
  ## 1. Duplicates -------------------------------------------------------------
  
  
  
  # Erros ano 2000
  
      ## 1.  ----
    # Tem 2 ocorrências com ZERO, porém possuem geometria:
    teste <- mesoregions |> 
      filter(nome == "0") |> 
      select(2, 6, 9, 9)
    
    plot(teste)
               
      
    
    
  
  
  
  
  
  
  # 2001
  # c("mslink", "mapid", "codigo", "area_1", "perimetro", "geocodigo",
  # "nome", "area_tot_g", "geometry")
  # 
  # 2010
  # c("id", "nm_meso", "cd_geocodu", "geometry")
  # 
  # 2013
  # "id"         "nm_meso"    "cd_geocodu" "geometry"  
  # Os nomes das mesorregiões estão com grafia errada.
  # "MADEIRA-GUAPOR\xc9",
  # 
  # 2015
  # c("nm_meso"   "cd_geocme" "geometry" )
  
  ## 2. Create dicionario de equivalências ----
  
  # dicionario <- data.frame(
  #   # Lista de nomes padronizados de colunas
  #   padrao = c(
  #     #CÓDIGO DE MUNICÍPIO e número de variações associadas
  #     rep("code_muni", 7),
  #     #NOME DO MUNICÍPIO e número de variações associadas
  #     rep("name_muni", 4),
  #     #CÓDIGO DO ESTADO e número de variações associadas
  #     rep("code_state", 5),
  #     #ABREVIAÇÃO DO ESTADO e número de variações associadas
  #     rep("abbrev_state", 4),
  #     #NOME DO ESTADO e número de variações associadas
  #     rep("name_state", 3),
  #     #CÓDIGO DA REGIÃO e número de variações associadas
  #     rep("code_region", 2),
  #     #NOME DA REGIÃO e número de variações associadas
  #     rep("name_region", 2),
  #     #ABREVIAÇÃO DA REGIÃO e número de variações associadas
  #     rep("abbrev_region", 1)
  #   ),
  #   # Lista de variações
  #   variacao = c(
  #     #Variações que convergem para "code_muni"
  #     "cod_uf", "cd_uf", "code_uf", "codigo_uf", "cod_state", "cd_mun", "cod_mun",
  #     #Variações que convergem para "name_muni"
  #     "nome_cidade", "cidade", "nm_muni", "nome_muni",
  #     #Variações que convergem para "code_state"
  #     "cod_uf", "cd_uf", "code_uf", "codigo_uf", "cod_state",
  #     #Variações que convergem para "abbrev_state"
  #     "sigla", "sigla_uf", "uf", "sg_uf",
  #     #Variações que convergem para "name_state"
  #     "nm_uf", "nm_state", "nm_estado",
  #     #Variações que convergem para "code_region"
  #     "cd_regia", "cd_regiao",
  #     #Variações que convergem para "name_region"
  #     "nm_regia", "nm_regiao",
  #     #Variações que convergem para "abbrev_region"
  #     "sigla_rg"
  #     ), stringsAsFactors = FALSE)
  
  
  ## 3. Rename collumns names ----
  
  #if (year %like% "2000|2001"){
    #       # dplyr::rename and subset columns
    #       temp_sf <- dplyr::select(temp_sf, c('code_meso'=geocodigo, 'name_meso'=nome, 'geom'))
    #     }
    # 
    #     if (year %like% "2010"){
    #       # dplyr::rename and subset columns
    #       temp_sf <- dplyr::select(temp_sf, c('code_meso'=cd_geocodu, 'name_meso'=nm_meso, 'geom'))
    #     }
    # 
    #     if (year %like% "2013|2014|2015|2016|2017|2018"){
    #       # dplyr::rename and subset columns
    #       temp_sf <- dplyr::select(temp_sf, c('code_meso'=cd_geocme, 'name_meso'=nm_meso, 'geom'))
    #     }
    # 
    #     if (year %like% "2019|2020"){
    #       # dplyr::rename and subset columns
    #       temp_sf <- dplyr::select(temp_sf, c('code_meso'=cd_meso, 'name_meso'=nm_meso, 'geom'))
    #     }
  
  
  ## 4. Apply harmonize geobr cleaning -----------------------------------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = mesoregions_raw,
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
  
  ## 5. Lighter version -------------------------------------------------------- 
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 4. Save datasets  ---------------------------------------------------------
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/mesoregions_",  year,
  #                                    ".gpkg"), delete_dsn = TRUE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean,
  #                                               "/mesoregions_",
  #                                               year, "_simplified.gpkg"),
  #              delete_dsn = TRUE )
  
  # Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/mesoregions_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 22
  )
  
  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean,"/mesoregions_", year, "_simplified",
                  ".parquet"),
    compression='zstd',
    compression_level = 22
  )
  ## 5. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}



########################## OLD FILE BELOW HERE ##########


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
# #unzip_to_geopackage(region='meso_regiao', year='2019')
# unzip_to_geopackage(region='meso_regiao', year='all')
# 
# 
# 
# ###### Cleaning MESO files --------------------------------
# setwd('L:/# DIRUR #/ASMEQ/geobr/data-raw/malhas_municipais')
# 
# meso_dir <- paste0(getwd(),"/shapes_in_sf_all_years_original/meso_regiao")
# 
# sub_dirs <- list.dirs(path = meso_dir, recursive = F)
# 
# sub_dirs <- sub_dirs[sub_dirs %like% paste0(2000:2020,collapse = "|")]
# 
# 
# 
# 
# # create a function that will clean the sf files according to particularities of the data in each year
# clean_meso <- function(e, year){ #  e <- sub_dirs[sub_dirs %like% 2000 ]
# 
#   # select year
#   if (year == 'all') {
#     message(paste('Processing all years'))
#   } else{
#     if (!any(e %like% year)) {
#       return(NULL)
#     }
#   }
#   message(paste('Processing',year))
# 
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
#   dir.create(file.path("shapes_in_sf_all_years_cleaned2/meso_regiao/"), showWarnings = FALSE)
# 
#   # create a subdirectory of years
#   dir.create(file.path(paste0("shapes_in_sf_all_years_cleaned2/meso_regiao/",year)), showWarnings = FALSE)
#   gc(reset = T)
# 
#   dir.dest<- file.path(paste0("./shapes_in_sf_all_years_cleaned2/meso_regiao/",year))
# 
#   # list all sf files in that year/folder
#   sf_files <- list.files(e, full.names = T, recursive = T, pattern = ".gpkg$")
# 
#   #sf_files <- sf_files[sf_files %like% "Mesorregioes"]
# 
#   # for each file
#   for (i in sf_files){ #  i <- sf_files[8]
# 
#     # read sf file
#     temp_sf <- st_read(i)
#     names(temp_sf) <- names(temp_sf) %>% tolower()
# 
#     if (year %like% "2000|2001"){
#       # dplyr::rename and subset columns
#       temp_sf <- dplyr::select(temp_sf, c('code_meso'=geocodigo, 'name_meso'=nome, 'geom'))
#     }
# 
#     if (year %like% "2010"){
#       # dplyr::rename and subset columns
#       temp_sf <- dplyr::select(temp_sf, c('code_meso'=cd_geocodu, 'name_meso'=nm_meso, 'geom'))
#     }
# 
#     if (year %like% "2013|2014|2015|2016|2017|2018"){
#       # dplyr::rename and subset columns
#       temp_sf <- dplyr::select(temp_sf, c('code_meso'=cd_geocme, 'name_meso'=nm_meso, 'geom'))
#     }
# 
#     if (year %like% "2019|2020"){
#       # dplyr::rename and subset columns
#       temp_sf <- dplyr::select(temp_sf, c('code_meso'=cd_meso, 'name_meso'=nm_meso, 'geom'))
#     }
# 
#     # Use UTF-8 encoding
#     temp_sf <- use_encoding_utf8(temp_sf)
# 
#     # add name_state
#     temp_sf$code_state <- substring(temp_sf$code_meso, 1,2)
#     temp_sf <- add_state_info(temp_sf,column = 'code_state')
# 
#     # reorder columns
#     temp_sf <- dplyr::select(temp_sf, 'code_state', 'abbrev_state', 'name_state', 'code_meso', 'name_meso', 'geom')
# 
#     # Capitalize the first letter
#     temp_sf$name_meso <- stringr::str_to_title(temp_sf$name_meso)
# 
#     # Harmonize spatial projection CRS, using SIRGAS 2000 epsg (SRID): 4674
#     temp_sf <- harmonize_projection(temp_sf)
# 
#     # strange error in Bahia 2000
#     # remove geometries with area == 0
#     temp_sf <- temp_sf[ as.numeric(st_area(temp_sf)) != 0, ]
# 
#     # strange error in Maranhao 2000
#     # meso_21 <- geobr::read_meso_region(code_meso= 21, year=2001)
#     # mapview::mapview(micro_21) + temp_sf[c(2),]
# 
#     if (year==2000 & temp_sf$code_state[1]==21) {
#       temp_sf[2, c('code_state', 'abbrev_state', 'name_state', 'code_meso', 'name_meso')] <- c(21, 'MA', 'Maranhão', 210520, 'Gerais De Balsas' )
#       temp_sf[6, c('code_state', 'abbrev_state', 'name_state', 'code_meso', 'name_meso')] <- c(21, 'MA', 'Maranhão', 210521, 'Chapadas Das Mangabeiras' )
#     }
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
# 
#     # save each state separately
#     for( c in unique(temp_sf$code_state)){ # c <- 11
# 
#       temp2 <- subset(temp_sf, code_state ==c)
#       temp2_simplified <- subset(temp_sf_simplified, code_state ==c)
# 
#       file.name <- paste0(unique(substr(temp2$code_state,1,2)),"ME",".gpkg")
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
# # apply function in parallel
# future::plan(multisession)
# future_map(.x=sub_dirs, .f=clean_meso, year=2000)
# 
# rm(list= ls())
# gc(reset = T)
# 
# 
# 
# 
# ###### Correcting number of digits of meso regions in 2010  --------------------------------
# # issue #20
# 
# 
# # Dirs
# meso_dir <- "L:////# DIRUR #//ASMEQ//geobr//data-raw//malhas_municipais//shapes_in_sf_all_years_cleaned2/meso_regiao"
# sub_dirs <- list.dirs(path =meso_dir, recursive = F)
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
# # Create function to correct number of digits of meso regions in 2010
# 
# # use data of 2013 to add code and name of meso regions in the 2010 data
# correct_meso_digits <- function(a2010_sf_meso_file){ # a2010_sf_meso_file <- sf_files_2010[1]
# 
#   # Get UF of the file
#   get_uf <- function(x){if (grepl("simplified",x)) {
#     substr(x, nchar(x)-19, nchar(x)-18)
#   } else {substr(x, nchar(x)-8, nchar(x)-7)}
#   }
#   uf <- get_uf(a2010_sf_meso_file)
# 
# 
#   # read 2010 file
#   temp2010 <- st_read(a2010_sf_meso_file)
# 
#   # read 2013 file
#   temp2013 <- sf_files_2013[ if (grepl("simplified",a2010_sf_meso_file)) {
#     (sf_files_2013 %like% paste0("/",uf)) & (sf_files_2013 %like% "simplified")
#  } else {
#     (sf_files_2013 %like% paste0("/",uf)) & !(sf_files_2013 %like% "simplified")
#   }]
#   temp2013 <- st_read(temp2013)
# 
#   # keep only code and name columns
#   table2013 <- temp2013 %>% as.data.frame()
#   table2013 <- dplyr::select(table2013, code_meso, name_meso)
# 
#   # update code_meso
#   sf2010 <- left_join(temp2010, table2013, by="name_meso")
#   sf2010 <- dplyr::select(sf2010, code_state, abbrev_state, name_state, code_meso=code_meso.y, name_meso, geom)
# 
#   # Save file
#   st_write(sf2010,a2010_sf_meso_file,append = FALSE,delete_dsn =T,delete_layer=T)
# }
# 
# # Apply function
# lapply(sf_files_2010, correct_meso_digits)
# 
# 
