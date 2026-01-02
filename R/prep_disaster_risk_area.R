#> DATASET: disaster_risk_areas 2010
#> Source: IBGE - ftp://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/populacao_em_areas_de_risco_no_brasil
#> Metadata:
# Titulo: disaster_risk_areas
# Titulo alternativo: Areas de risco de desastres naturais 2010
# Frequencia de atualizacao: ?
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: Utf-8
#
# Resumo: Polígonos de áreas de risco de desastres de natureza hidro-climatológicas.
# Informações adicionais: Dados produzidos conjuntamente por IBGE e CEMADEN
#
# Estado: Em desenvolvimento
# Palavras chaves descritivas:****
# Informação do Sistema de Referência: SIRGAS 2000

### Libraries (use any library as necessary) -----------------------------------

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

# Download the data  -----------------------------------------------------------
download_riskdisasterareas <- function(){ # year = 2018
  
  ## 0. Generate the correct ftp link (UPDATE YEAR HERE) -----------------------
  
  ftp_link <- paste0("https://geoftp.ibge.gov.br/organizacao_do_territorio/",
                      "tipologias_do_territorio/",
                      "populacao_em_areas_de_risco_no_brasil/base_de_dados/",
                      "PARBR2018_BATER.zip")
  
  ## 1. Create temp folder -----------------------------------------------------
  
  zip_dir <- paste0(tempdir(), "/disaster_risk_areas/")
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  ## 2. Create direction for each download -------------------------------------
  
  # zip folder
  in_zip <- paste0(zip_dir, "/zipped/")
  dir.create(in_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(in_zip)
  
  file_raw <- fs::file_temp(tmp_dir = in_zip,
                            ext = fs::path_ext(ftp_link))
  
  ## 3. Download Raw data ------------------------------------------------------
  
  httr::GET(url = ftp_link,
            httr::progress(),
            httr::write_disk(path = file_raw,
                             overwrite = T))
  
  ## 4. Unzip Raw data ---------------------------------------------------------
  
  ### unzip folder
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  unzip_geobr(zip_dir = zip_dir, in_zip = in_zip, out_zip = out_zip, is_shp = TRUE)
  
  ## 5. Bind Raw data together -------------------------------------------------
  
  shp_names <- list.files(out_zip, pattern = "\\.shp$",
                          full.names = TRUE)
  
  riskdisasterareas_list <- pbapply::pblapply(
    X = shp_names, 
    FUN = function(x){ sf::st_read(x, quiet = T, stringsAsFactors=F) }
  )
  
  riskdisasterareas_raw <- data.table::rbindlist(riskdisasterareas_list)
  
  ## 6. Show result ------------------------------------------------------------
  
  data.table::setDF(riskdisasterareas_raw)
  riskdisasterareas_raw <- sf::st_as_sf(riskdisasterareas_raw) %>% 
    clean_names()
  
  glimpse(riskdisasterareas_raw)
  
  return(riskdisasterareas_raw)
  
}

# Clean the data  --------------------------------------------------------------
clean_riskdisasterareas <- function(riskdisasterareas_raw){ # year = 2018
  
  ## 0. Create folder to save clean data ---------------------------------------
  
  dir_clean <- "./data/disaster_risk_areas/"
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. Rename collumns, reorder and adjust ------------------------------------
  
  statesgeobr <- states_geobr()
  
  glimpse(riskdisasterareas_raw)
  
  riskdisasterareas <- riskdisasterareas_raw |> 
    rename(code_state = geo_uf, code_muni = geo_mun, name_muni = municipio) |> 
    left_join(statesgeobr, by = "code_state") |> 
    select(-id, - abbrevm_state) |> 
    relocate(code_muni, name_muni, code_state, abbrev_state, name_state,
             code_region, name_region,
             geo_bater, origem , acuracia, obs, num)
  
  glimpse(riskdisasterareas)
    
  ## 3. Apply harmonize geobr cleaning -----------------------------------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = riskdisasterareas,
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
  
  ## 4. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 5. Save datasets  ---------------------------------------------------------
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/riskdisasterareas_",  year,
  #                                    ".gpkg"), delete_dsn = TRUE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean,
  #                                               "/intermediateregions_",
  #                                               year, "_simplified.gpkg"),
  #              delete_dsn = TRUE )
  
  # Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/riskdisasterareas_", ".parquet"),
    compression = 'zstd',
    compression_level = 22
  )
  
  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean,"/riskdisasterareas_", "_simplified",
                  ".parquet"),
    compression='zstd',
    compression_level = 22
  )
  
  ## 6. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}


# # OLD CODE BELOW -------------------------------------------------------------
# 
# ### Libraries (use any library as necessary) -----------------------------------
# 
# library(sf)
# library(dplyr)
# library(tidyverse)
# library(data.table)
# library(mapview)
# 
# ####### Load Support functions to use in the preprocessing of the data
# 
# source("./prep_data/prep_functions.R")
# 
# 
# 
# 
# # If the data set is updated regularly, you should create a function that will have
# # a `date` argument download the data
# 
# update <- 2010
# 
# 
# 
# 
# 
# 
# 
# ###### 0. Create Root folder to save the data -----------------
# # Root directory
# root_dir <- "L:\\# DIRUR #\\ASMEQ\\geobr\\data-raw"
# setwd(root_dir)
# 
# 
# 
# # Directory to keep raw zipped files
# dir.create("./disaster_risk_area")
# destdir_raw <- paste0("./disaster_risk_area/",update)
# dir.create(destdir_raw)
# 
# 
# # Create folders to save clean sf.rds files  -----------------
# dir.create("./disaster_risk_area/shapes_in_sf_cleaned", showWarnings = FALSE)
# destdir_clean <- paste0("./disaster_risk_area/shapes_in_sf_cleaned/",update)
# dir.create(destdir_clean)
# 
# 
# 
# #### 0. Download original data sets from source website -----------------
# 
# 
# # baixando o shape no formato .zip e dando-lhe o nome de "PARBR2018_BATER.zip"
# download.file("ftp://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/populacao_em_areas_de_risco_no_brasil/base_de_dados/PARBR2018_BATER.zip" ,
#               destfile= paste0(destdir_raw,"/PARBR2018_BATER.zip"))
# 
# 
# 
# #### 2. Unzipe shape files -----------------
# setwd(destdir_raw)
# 
# # list and unzip zipped files
# zipfiles <- list.files(pattern = ".zip")
# unzip(zipfiles)
# 
# 
# 
# 
# #### 3. Clean data set and save it in compact .rds format-----------------
# 
# 
# # lendo o shapefile
# temp_sf <- st_read("PARBR2018_BATER.shp")
# 
# 
# # renomeando as variáveis e excluindo algumas
# 
# names(temp_sf)
# temp_sf$ID <- NULL
# temp_sf$AREA_GEO <- NULL
# temp_sf <- rename(temp_sf, code_state = GEO_UF,)
# temp_sf <- rename(temp_sf, code_muni = GEO_MUN)
# temp_sf <- rename(temp_sf, name_muni = MUNICIPIO)
# temp_sf <- rename(temp_sf, geo_bater = GEO_BATER)
# temp_sf <- rename(temp_sf, origem = ORIGEM)
# temp_sf <- rename(temp_sf, acuracia = ACURACIA)
# temp_sf <- rename(temp_sf, obs = OBS)
# temp_sf <- rename(temp_sf, num = NUM)
# 
# 
# # Use UTF-8 encoding
# temp_sf$name_muni <- stringi::stri_encode(as.character(temp_sf$name_muni), "UTF-8")
# 
# 
# # store original CRS
# original_crs <- sf::st_crs(temp_sf)
# 
# # # criando a coluna das UFs
# #alterando temp_sf para poder criar abbrev_state
# temp_sf <- as.data.table(temp_sf)
# 
# # Criando a coluna das UFs
# temp_sf[ code_state== 11, abbrev_state :=	"RO" ]
# temp_sf[ code_state== 12, abbrev_state :=	"AC" ]
# temp_sf[ code_state== 13, abbrev_state :=	"AM" ]
# temp_sf[ code_state== 14, abbrev_state :=	"RR" ]
# temp_sf[ code_state== 15, abbrev_state :=	"PA" ]
# temp_sf[ code_state== 16, abbrev_state :=	"AP" ]
# temp_sf[ code_state== 17, abbrev_state :=	"TO" ]
# temp_sf[ code_state== 21, abbrev_state :=	"MA" ]
# temp_sf[ code_state== 22, abbrev_state :=	"PI" ]
# temp_sf[ code_state== 23, abbrev_state :=	"CE" ]
# temp_sf[ code_state== 24, abbrev_state :=	"RN" ]
# temp_sf[ code_state== 25, abbrev_state :=	"PB" ]
# temp_sf[ code_state== 26, abbrev_state :=	"PE" ]
# temp_sf[ code_state== 27, abbrev_state :=	"AL" ]
# temp_sf[ code_state== 28, abbrev_state :=	"SE" ]
# temp_sf[ code_state== 29, abbrev_state :=	"BA" ]
# temp_sf[ code_state== 31, abbrev_state :=	"MG" ]
# temp_sf[ code_state== 32, abbrev_state :=	"ES" ]
# temp_sf[ code_state== 33, abbrev_state :=	"RJ" ]
# temp_sf[ code_state== 35, abbrev_state :=	"SP" ]
# temp_sf[ code_state== 41, abbrev_state :=	"PR" ]
# temp_sf[ code_state== 42, abbrev_state :=	"SC" ]
# temp_sf[ code_state== 43, abbrev_state :=	"RS" ]
# temp_sf[ code_state== 50, abbrev_state :=	"MS" ]
# temp_sf[ code_state== 51, abbrev_state :=	"MT" ]
# temp_sf[ code_state== 52, abbrev_state :=	"GO" ]
# temp_sf[ code_state== 53, abbrev_state :=	"DF" ]
# head(temp_sf)
# 
# 
# 
# # Convert data.table back into sf
# temp_sf <- st_as_sf(temp_sf, crs=original_crs)
# 
# # Harmonize spatial projection CRS, using SIRGAS 2000 epsg (SRID): 4674
# temp_sf <- harmonize_projection(temp_sf)
# 
# # Make any invalid geometry valid # st_is_valid( sf)
# temp_sf <- lwgeom::st_make_valid(temp_sf)
# 
# # reorder column names
# setcolorder(temp_sf, c('geo_bater', 'origem', 'acuracia', 'obs', 'num', 'code_muni', 'name_muni', 'code_state', 'abbrev_state', 'geometry'))
# 
# 
# 
# ###### convert to MULTIPOLYGON -----------------
# temp_sf <- to_multipolygon(temp_sf)
# 
# 
# ###### 6. generate a lighter version of the dataset with simplified borders -----------------
# # skip this step if the dataset is made of points, regular spatial grids or rater data
# 
# # simplify
# temp_sf7 <- st_transform(temp_sf, crs=3857) %>%
#   sf::st_simplify(preserveTopology = T, dTolerance = 100) %>%
#   st_transform(crs=4674)
# head(temp_sf7)
# 
# 
# # Save cleaned sf in the cleaned directory
# setwd(root_dir)
# readr::write_rds(temp_sf, path= paste0(destdir_clean,"/disaster_risk_area2010.rds"), compress = "gz")
# sf::st_write(temp_sf,     dsn=  paste0(destdir_clean,"/disaster_risk_area2010.gpkg") )
# sf::st_write(temp_sf7,    dsn=  paste0(destdir_clean,"/disaster_risk_area2010 _simplified", ".gpkg"))
# 
# 
# 
# #teste push


