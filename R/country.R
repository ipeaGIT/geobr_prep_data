#> DATASET: country 2000 a 2024
#> Source: IBGE - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html?=&t=o-que-e
#> scale 1:250.000
#> Metadata: #####
# Título: País
# Título alternativo: country
# Frequência de atualização: ?????
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Polígono das fronteiras do Brasil.
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


# library(geobr)
# library(tidyverse)
# library(janitor)
# library(arrow)
# library(geoarrow)
# library(dplyr)
# library(readr)
# library(sp)
# library(sf)
# library(devtools)
# library(parallel)
# library(data.table)
# source("./R/support_harmonize_geobr.R")
# source("./R/support_fun.R")


# Download the data  ----
download_country <- function(year){ # year = 2010
  
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
  
  zip_dir <- paste0(tempdir(), "/country/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  ### Alternative folder
  # zip_dir <- paste0("./data_raw/", "/country/", year)
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
    country_list <- pbapply::pblapply(
      X = shp_names,
      FUN = function(x){ sf::st_read(x, quiet = T, stringsAsFactors= F) }
    )
    
    country_raw <- data.table::rbindlist(country_list)
  }
  
  if (year %in% c(2001, 2010:2014))  {#years with error in number of collumns
    country_raw <- readmerge_geobr(folder_path = out_zip)
  }
  
  #### After 2015
  if (length(shp_names) == 1) {
    country_raw <- st_read(shp_names, quiet = T, stringsAsFactors= F)
  }
  
  ## 6. Integrity test ----
  
  #### Before 2015
  glimpse(country_raw)
  
  #### After 2015
  if (length(shp_names) == 1) {
    table_collumns <- tibble(name_collum = colnames(country_raw),
                             type_collum = sapply(country_raw, class)) |> 
      rownames_to_column(var = "num_collumn")
    
    glimpse(table_collumns)
    glimpse(country_raw)
  }
  
  ## 7. Show result ----
  
  data.table::setDF(country_raw)
  
  country_raw <- sf::st_as_sf(country_raw) %>% 
    clean_names()
  
  return(country_raw)
  
}

# Clean the data  ----
clean_country <- function(country_raw, year){ # year = 2024
  
  ## 0. Create folder to save clean data ----
  
  dir_clean <- paste0("./data/country/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. Preparation ----
  
  country_clean <- country_raw |> 
    mutate(cod_country = 1) |> 
    select(cod_country)
  
  glimpse(country_clean)
  
  ## 2. Transform states in country -----
  
  ### Dissolve each region
  all_country <- dissolve_polygons(mysf=country_clean, group_column='cod_country')
  
  glimpse(all_country)
  
  ## 3. Apply harmonize geobr cleaning ----
  
  temp_sf <- harmonize_geobr(
    temp_sf = all_country,
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
  
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/country_",  year,
  #                                   ".gpkg"), delete_dsn = TRUE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean, "/country_",
  #                                               year, "_simplified.gpkg"),
  #              delete_dsn = TRUE )
  
  ### Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/country_", year, ".parquet"),
    compression = 'zstd',
    compression_level = 22
  )
  
  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean,"/country_", year, "_simplified", ".parquet"),
    compression='zstd',
    compression_level = 22
  )
  
  return(dir_clean)
}

####### OLD CODE BELOW ########### ----

#### Using data already in the geobr package -----------------


#### Function to create country sf file

# For an specified year, the function:
# a) reads all states sf files and pile them up
# b) make sure the have valid geometries
# c) dissolve borders to create country file
# d) create a subdirectory of that year in the country directory
# e) save as an sf file


# get_country <- function(y){
# 
#   # a) reads all states sf files and pile them up
#   # y <- 2018
#   sf_states <- read_state(year= y , code_state = "all", simplified = F)
# 
#   # store original crs
#   original_crs <- st_crs(sf_states)
# 
#   # b) make sure we have valid geometries
#   temp_sf <- sf::st_make_valid(sf_states)
#   temp_sf <- temp_sf %>% st_buffer(0)
# 
#   sf_states1 <- to_multipolygon(temp_sf)
# 
#   # c) create attribute with the number of points each polygon has
#   points_in_each_polygon = sapply(1:dim(sf_states1)[1], function(i)
#     length(st_coordinates(sf_states1$geom[i])))
# 
#   sf_states1$points_in_each_polygon <- points_in_each_polygon
#   mypols <- sf_states1 |> filter(points_in_each_polygon > 0)
# 
#   # d) convert to sp
#   sf_statesa <- mypols |> as("Spatial")
#   sf_statesa <- rgeos::gBuffer(sf_statesa, byid=TRUE, width=0) # correct eventual topology issues
# 
#   # temp_sp <- sf::as_Spatial(temp_sf)
#   # temp_sp <- rgeos::gBuffer(temp_sp, byid=TRUE, width=0)
#   # plot(sf_statesa)
# 
# 
#   # c) dissolve borders to create country file
#   result <- maptools::unionSpatialPolygons(sf_statesa, rep(TRUE, nrow(sf_statesa@data))) # dissolve
# 
#   # d) get rid of holes
#   outerRings = Filter(function(f){f@ringDir==1},result@polygons[[1]]@Polygons)
#   outerBounds = SpatialPolygons(list(Polygons(outerRings,ID=1)))
#   plot(outerBounds)
# 
#   # e) convert back to sf data
#   outerBounds <- st_as_sf(outerBounds)
#   outerBounds <- st_set_crs(outerBounds, original_crs)
# 
#   # f) get rid of holes to make sure
#   outerBounds <- outerBounds |>
#     sf::st_make_valid() |>
#     sfheaders::sf_remove_holes()
# 
#   plot(outerBounds, col='gray90')
# 
# 
#   # f) create a subdirectory of that year in the country directory
#     dest_dir <- paste0("./data/country/",y)
#     dir.create(dest_dir, showWarnings = FALSE, recursive = T)
# 
#   # g) generate a lighter version of the dataset with simplified borders
#     temp_sf_simp <- simplify_temp_sf(outerBounds)
# 
#     ###### convert to MULTIPOLYGON -----------------
#     temp_sf <- to_multipolygon(outerBounds)
#     temp_sf_simp <- to_multipolygon(temp_sf_simp)
# 
# 
#   # h) save as an sf file
#     sf::st_write(temp_sf, dsn=paste0(dest_dir, "/country_",y,".gpkg") )
#     sf::st_write(temp_sf_simp,dsn=paste0(dest_dir, "/country_",y,"_simplified", ".gpkg"))
# 
# }
# 
# 
# get_country(y=2020)
# 
# 
# # Apply function to save original data sets in rds format
# 
# # create computing clusters
#   cl <- parallel::makeCluster(detectCores())
# 
#   clusterEvalQ(cl, c(library(geobr), library(maptools), library(dplyr), library(readr), library(rgeos), library(sf)))
#   parallel::clusterExport(cl=cl, varlist= c("years","read_state"), envir=environment())
# 
# # apply function in parallel
#   parallel::parLapply(cl, years, get_country)
#   stopCluster(cl)
# 
# # rm(list= ls())
# # gc(reset = T)

