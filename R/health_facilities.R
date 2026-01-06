#> DATASET: Estabelecimentos de Saúde
#> Source: Portal de Dados Abertos do Ministério da Saúde
#: ##### scale 1:5.000.000
#> Metadata:
# Título: Estabelecimentos de Saúde CNES
# Título alternativo: health facilities
# Frequência de atualização: ##########
# Forma de apresentação: Points
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Localização dos estabelecimentos registrados no Cadastro Nacional de Estabelecimentos de Saúde - CNES.
# Informações adicionais: Dados produzidos pelo Ministério da Saúde.
# Propósito: Identificação dos estabelecimentos de saúde.
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas:****
# Informação do Sistema de Referência: ##### SIRGAS 2000
#
# Observações: Anos disponíveis: ###########?

### Libraries (use any library as necessary) -----------------------------------

# library(tidyverse)
# library(lubridate)
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
# library(arrow)
# library(geoarrow)
# library(geocodebr)
# source("./R/support_harmonize_geobr.R")
# source("./R/support_fun.R")

# Download the data  -----------------------------------------------------------
download_healthfacilities <- function(year){ #no year because only most recent available. Year is set on system date, by YYYY_MM
  
  ## 0. Create temp folders and data folders -----------------------------------
  
  #folder_geobr(folder_name = "health_facilities", temp = TRUE)
  
  zip_dir <- paste0(tempdir(), "/health_facilities/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  ## 1. Get download link ------------------------------------------------------
  
  # Source:
  # "https://dados.gov.br/dados/conjuntos-dados/cnes-cadastro-nacional-de-estabelecimentos-de-saude"
  file_url <- "s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/CNES/cnes_estabelecimentos_csv.zip"
  #file_url = 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/CNES/cnes_estabelecimentos.zip'
  
  ## 2. Create direction for each download -------------------------------------
  
  #### zip folders
  in_zip <- paste0(zip_dir, "/zipped/")
  dir.create(in_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(in_zip)
  
  file_raw <- fs::file_temp(tmp_dir = in_zip,
                            ext = fs::path_ext(file_url))
  
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  ### Alternative folder
  # zip_dir <- paste0("./data_raw/", "/health_facilities/", year)
  # dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  # dir.exists(zip_dir)
  
  ## 3. Download Raw data ------------------------------------------------------
  
  httr::GET(url = file_url,
            httr::progress(),
            httr::write_disk(path = file_raw,
                             overwrite = T))
  
  ## 4. Unzip Raw data ---------------------------------------------------------
  
  file_zipped <- paste0(in_zip, basename(file_raw))
  unzip(file_zipped, exdir = out_zip)
  
  ## 5. Read file and clean collumns names -------------------------------------
  
  file_unzipped <- list.files(out_zip, full.names = TRUE)
  
  healthfacilities_raw <- fread(file = file_unzipped,
                                encoding = "UTF-8",
                                integer64 = "character") |> 
    clean_names()
  
  ## 6. Show result ------------------------------------------------------------
  
  healthfacilities_raw <- st_as_sf(healthfacilities_raw, na.fail = FALSE,
                                   coords = c("nu_longitude","nu_latitude"))
  
  glimpse(healthfacilities_raw)
  
  return(healthfacilities_raw)
}

# Clean the data  --------------------------------------------------------------
clean_healthfacilities <- function(healthfacilities_raw, year){

  ## 0. Adjust date of last update ---------------------------------------------
  
  date_month <- str_sub(year, start = 6, end = 8)
  date_year <- str_sub(year, start = 1, end = 4)
  
  ## 1. Create folder to save clean data ---------------------------------------
  
  dir_clean <- paste0("./data/health_facilities/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 2. Preparing to do adjusts ------------------------------------------------
  
  glimpse(healthfacilities_raw)
  
  states <- states_geobr() |> select(1, 2, 4, 5)
  states$code_state <- as.integer(states$code_state)
  
  glimpse(states)
  
  ## 3. Adjusts collumns names, reorder, add geocodebr -------------------------
  
  healthfacilities <- healthfacilities_raw |> 
    rename(code_cnes = 'co_cnes', # rename collumns
           code_unidade = 'co_unidade',
           code_state = 'co_uf',
           code_muni = 'co_ibge',
           num_cnpj_mantenedora = 'nu_cnpj_mantenedora',
           name_razao_social = 'no_razao_social',
           name_fantasia = 'no_fantasia',
           code_natureza_organizacao = 'co_natureza_organizacao',
           desc_natureza_organizacao = 'ds_natureza_organizacao',
           tipo_gestao = 'tp_gestao',
           code_nivel_hierarquia = 'co_nivel_hierarquia',
           desc_nivel_hierarquia = 'ds_nivel_hierarquia',
           code_esfera_administrativa = 'co_esfera_administrativa',
           desc_esfera_administrativa = 'ds_esfera_administrativa',
           code_atividade = 'co_atividade',
           tipo_unidade = 'tp_unidade',
           code_cep = 'co_cep',
           name_logradouro = 'no_logradouro',
           num_endereco = 'nu_endereco',
           name_bairro = 'no_bairro',
           num_telefone = 'nu_telefone',
           code_turno_atendimento = 'co_turno_atendimento',
           desc_turno_atendimento = 'ds_turno_atendimento',
           num_cnpj = 'nu_cnpj',
           name_email = 'no_email',
           code_natureza_jur = 'co_natureza_jur',
           code_motivo_desab = 'co_motivo_desab',
           code_ambulatorial_sus = 'co_ambulatorial_sus') |> 
    left_join(states, by = "code_state") |> # add state and region
    mutate(code_cnes = sprintf("%07d", code_cnes), # fix code_cnes to 7 digits
           date_update = date_month,
           year_update = date_year) |> 
    relocate(any_of(c("code_muni", "code_state", "abbrev_state", "code_region",
                      "name_region", "year_update", "date_update")),
             .after = code_unidade) # reorder collumns
    
  glimpse(healthfacilities)
  
  #   # fix code_muni to 7 digits
  #   muni <- geobr::read_municipality(code_muni = 'all', year = as.numeric(year_update) - 1)
  #   data.table::setDT(muni)
  #   muni[, code_muni6 := as.numeric(substring(code_muni, 1, 6))]
  #   muni <- muni[, .(code_muni6, code_muni)]
  # 
  #   dt[muni,  on = 'code_muni6', code_muni := i.code_muni]
  #   dt[, code_muni6 := NULL]
  # 

 
  # 
  #   # deal with points with missing coordinates
  #   head(dt)
  #   dt[is.na(lat) | is.na(lon),]
  #   dt[lat==0,]
  # 
  #   # identify which points should have empty geo
  #   dt[is.na(lat) | is.na(lon), empty_geo := T]
  # 
  #   dt[code_cnes=='0000930', lat]
  #   dt[code_cnes=='0000930', lon]
  # 
  #   # replace NAs with 0
  #   data.table::setnafill(dt,
  #                         type = "const",
  #                         fill = 0,
  #                         cols=c("lat","lon")
  #                         )
  # 
  

  ## 3. Apply harmonize geobr cleaning -----------------------------------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = healthfacilities,
    year = year,
    add_state = F,
    add_region = F,
    add_snake_case = F,
    #snake_colname = snake_colname,
    projection_fix = F,
    encoding_utf8 = T,
    topology_fix = F,
    remove_z_dimension = F,
    use_multipolygon = F
  )
  
  glimpse(temp_sf)
  
  ## 4. Save results  ----------------------------------------------------------
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/healthfacilities_",  date_year, date_month, ".gpkg"), delete_dsn = TRUE)
  
  # Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/healthfacilities_", date_year, date_month, ".parquet"),
    compression = 'zstd',
    compression_level = 22
  )
  
  ## 5. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
  
}

# RAPHAEL OLD CODE BELOW HERE

#' # 0. Download Raw zipped  ---------------------------------
#' 
#' 
#' 
#' #'   
#' #' 
#' #'   meta$created
#' #'   date_update <- as.Date(meta$created) |> as.character()
#' #'   date_update <- gsub("-", "", date_update)
#' #'   year_update <- substring(date_update, 1, 4)
#' #' 
#' #' 
#' #'   # date shown to geobr user
#' #'   geobr_date <- substr(date_update, 1, 6)
#' #' 
#' #' 
#' #'   # wodnload file to tempdir
#' #'   temp_local_file <- download_file(file_url = meta$url)
#' #' 
#' #'   # unzip file to tempdir
#' #'   temp_local_dir <- tempdir()
#' #'   utils::unzip(zipfile = temp_local_file, exdir = temp_local_dir)
#' #' 
#' #'   # get file name
#' #'   file_name <- utils::unzip(temp_local_file, list = TRUE)$Name
#' #'   file_full_name <- paste0(temp_local_dir,'/', file_name)
#' #' 
#' #'   # read file stored locally
#' #'   dt <- data.table::fread( file_full_name )
#' #'   head(dt)
#' #' 
#' #'   # rename columns
#' #'   names(dt) <- tolower(names(dt))
#' #'   dt <- dplyr::rename(dt,
#' #'                       code_cnes = 'co_cnes',
#' #'                       code_state = 'co_uf',
#' #'                       code_muni6 = 'co_ibge',
#' #'                       lat = 'nu_latitude',
#' #'                       lon = 'nu_longitude')
#' #' 
#' #'   # fix code_cnes to 7 digits
#' #'   dt[, code_cnes := sprintf("%07d", code_cnes)]
#' #' 
#' #'   # fix code_muni to 7 digits
#' #'   muni <- geobr::read_municipality(code_muni = 'all', year = as.numeric(year_update) - 1)
#' #'   data.table::setDT(muni)
#' #'   muni[, code_muni6 := as.numeric(substring(code_muni, 1, 6))]
#' #'   muni <- muni[, .(code_muni6, code_muni)]
#' #' 
#' #'   dt[muni,  on = 'code_muni6', code_muni := i.code_muni]
#' #'   dt[, code_muni6 := NULL]
#' #' 
#' #'   # add state and region
#' #'   dt <- add_state_info(temp_sf = dt, column = 'code_state')
#' #'   dt <- add_region_info(temp_sf = dt, column = 'code_state')
#' #' 
#' #'   # add update date columns
#' #'   dt[, date_update := as.character(date_update)]
#' #'   dt[, year_update := as.character(year_update)]
#' #' 
#' #'   # reorder columns
#' #'   data.table::setcolorder(dt,
#' #'                           c('code_cnes',
#' #'                             'code_muni',
#' #'                             'code_state', 'abbrev_state', 'name_state',
#' #'                             'code_region', 'name_region',
#' #'                             'date_update', 'year_update'))
#' #' 
#' #' 
#' #'   # deal with points with missing coordinates
#' #'   head(dt)
#' #'   dt[is.na(lat) | is.na(lon),]
#' #'   dt[lat==0,]
#' #' 
#' #'   # identify which points should have empty geo
#' #'   dt[is.na(lat) | is.na(lon), empty_geo := T]
#' #' 
#' #'   dt[code_cnes=='0000930', lat]
#' #'   dt[code_cnes=='0000930', lon]
#' #' 
#' #'   # replace NAs with 0
#' #'   data.table::setnafill(dt,
#' #'                         type = "const",
#' #'                         fill = 0,
#' #'                         cols=c("lat","lon")
#' #'                         )
#' #' 
#' #' 
#' #' 
#' #'   # Convert originl data frame into sf
#' #'   temp_sf <- sf::st_as_sf(x = dt,
#' #'                           coords = c("lon", "lat"),
#' #'                           crs = "+proj=longlat +datum=WGS84")
#' #' 
#' #' 
#' #'   # convert to point empty
#' #'   # solution from: https://gis.stackexchange.com/questions/459239/how-to-set-a-geometry-to-na-empty-for-some-features-of-an-sf-dataframe-in-r
#' #'   temp_sf$geometry[temp_sf$empty_geo == T] = sf::st_point()
#' #' 
#' #'   subset(temp_sf, code_cnes=='0000930')
#' #' 
#' #' 
#' #'   # Change CRS to SIRGAS  Geodetic reference system "SIRGAS2000" , CRS(4674).
#' #'   temp_sf <- harmonize_projection(temp_sf)
#' #' 
#' #' 
#' #'   # create folder to save the data
#' #'   dest_dir <- paste0('./data/health_facilities/', geobr_date,'/')
#' #'   dir.create(path = dest_dir, recursive = TRUE, showWarnings = FALSE)
#' #' 
#' #' 
#' #'   # Save raw file in sf format
#' #'   sf::st_write(temp_sf,
#' #'                dsn= paste0(dest_dir, 'cnes_', geobr_date,".gpkg"),
#' #'                overwrite = TRUE,
#' #'                append = FALSE,
#' #'                delete_dsn = T,
#' #'                delete_layer = T,
#                quiet = T
#                )
# 
# }
