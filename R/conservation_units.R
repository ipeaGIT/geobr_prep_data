#> DATASET: conservation unit
#> Source: 
#> https://dados.gov.br/dados/conjuntos-dados/limites-oficiais-de-unidades-de-conservacao-federais
#> https://dados.mma.gov.br/dataset/unidadesdeconservacao
#> https://www.gov.br/icmbio/pt-br/assuntos/dados_geoespaciais/mapa-tematico-e-dados-geoestatisticos-das-unidades-de-conservacao-federais
#> Old source: MMA - http://mapas.mma.gov.br/i3geo/datadownload.htm
#> Metadata:
# Título: Unidades de Conservação
# Título alternativo:
# Data: Atualização ***
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: Latin1
#
# Resumo: Polígonos e Pontos das unidades de conservação brasileiras.
# Informações adicionais: Dados produzidos pelo MMA, e utilizados na elaboração do shape de biomas com a melhor base oficial disponível. Foi feito download dos dados tal como estão no segundo semestre de cada ano.
# Propósito: Identificação das unidades de conservação brasileiras.
#
# Estado: Em desenvolvimento
# Palavras chaves descritivas:****
# Informação do Sistema de Referência: SIRGAS 2000

# Download the data  -----------------------------------------------------------
download_conservationunits <- function(year){ # year = 2024
  
  ## 0. Generate the correct ftp link (UPDATE YEAR HERE) -----------------------
  
  ftp_start <- "https://dados.mma.gov.br/dataset/44b6dc8a-dc82-4a84-8d95-1b0da7c85dac/resource/"
  
  if(year == 2024) {
    date <- 202402
    ftp_link <- paste0(
      ftp_start,
      "9ec98f66-44ad-4397-8583-a1d9cc3a9835/download/shp_cnuc_2024_02.zip"
      )
    
  }
  
  if(year == 2025) {
    date <- 202503
    ftp_link <- paste0(
      ftp_start, 
      "20327e02-d4fe-4a1b-bd12-e381ab461d97/download/shp_cnuc_2025_03.zip"
      #"6ba9a557-87e8-4882-acb7-b3e0f0ea192d/download/shp_cnuc_2025_08.zip"
      
      )
  }

  # if(year == 2026) {
  #   ftp_link <- paste0(ftp_start, 
  #                      "6ba9a557-87e8-4882-acb7-b3e0f0ea192d/download/shp_cnuc_2025_08.zip"
  #                      )
  # }
  # 

  ## 1. Create temp folder -----------------------------------------------------
  
  zip_dir <- paste0(tempdir(), "/conservation_units/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  ## 2. Create direction for each download -------------------------------------
  
  file_raw <- fs::file_temp(tmp_dir = zip_dir,
                            ext = fs::path_ext(ftp_link))
  
  # filenames <- basename(ftp_link)
  
  ## 3. Download Raw data ------------------------------------------------------
  
  file_raw <- download_file_geobr(
    file_url = ftp_link,
    dest_dir = zip_dir
  )
  
  
  ## 4. Unzip Raw data ---------------------------------------------------------
  
  ### unzip folder
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  files <- unzip_geobr(zip_dir = zip_dir, out_zip = out_zip)
  
  ## 5. Check files ------------------------------------------------------------
  
  shp_names <- list.files(out_zip, pattern = "\\.shp$",
                          full.names = TRUE) |> 
    stringr::str_subset(pattern = "pontos", negate = TRUE) # Deny files with "pontos"
  
  ## 6. Bind Raw data together -------------------------------------------------
  
  conservationunits_list <- pbapply::pblapply(
    X = shp_names, 
    FUN = function(x){ sf::st_read(x, quiet = T, stringsAsFactors= F) }
  )
  
  conservationunits_raw <- dplyr::bind_rows(conservationunits_list)
  
  conservationunits_raw$year <- year
  conservationunits_raw$date <- date
  
  return(conservationunits_raw)
}

# Clean the data  --------------------------------------------------------------
# conservationunits_raw <- tar_read(conservationunits_raw, 2)
clean_conservationunits <- function(conservationunits_raw){
  
  ## 0. Create folder to save clean data ---------------------------------------
  yyyy <- conservationunits_raw$year[1]
  yyyymm <- conservationunits_raw$date[1]
  dir_clean <- paste0("./data/conservation_units/", yyyy)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. Rename columns and remove collumns -------------------------------------
  
  conservationunits <- conservationunits_raw |> 
    janitor::clean_names()
 
  if (yyyy >= 2024) {
  conservationunits_raw <- conservationunits_raw |> 
    dplyr::select(
           code_conservation_unit = cd_cnuc,
           name_conservation_uni = nome_uc,
           name_state = uf,
           name_muni = municipio,
           code_wdpa = wdpa_pid,
           creation_year = cria_ano,
           legislation = cria_ato,
           outro_ato,
           pl_manejo,
           conselho_gestor = co_gestor,
           quality = quali_pol, 
           ppgr,
           government_level = esfera,
           org_gestor,
           group_manejo = grupo,
           category = categoria,
           category_iucn = cat_iucn,
           area_ha_total = ha_total,
           area_ha_legal = ha_ato, 
           area_ha_amazonia = amazonia,
           area_ha_caatinga = caatinga, 
           area_ha_cerrado = cerrado, 
           area_ha_matlantica = matlantica, 
           area_ha_pampa = pampa, 
           area_ha_pantanal = pantanal, 
           area_ha_marinho = marinho, 
           limite, 
           year,
           date,
           geometry
     )
  }
  
  ## 2. Apply harmonize geobr cleaning -----------------------------------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = conservationunits,
    year = yyyy,
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

  ## 2b. Validate before saving
  stopifnot(!is.na(sf::st_crs(temp_sf)))
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")

  ## 3. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 4. Save datasets  ---------------------------------------------------------
  
  # Save in parquet
  write_geobr_parquet(
    sf_obj = temp_sf,
    path = paste0(dir_clean, "/conservationunits_", yyyymm, ".parquet"))

  write_geobr_parquet(
    sf_obj = temp_sf_simplified,
    path = paste0(dir_clean,"/conservationunits_", yyyymm, "_simplified.parquet")
    )
  
  
  ## 5. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}

################ RAFAEL OLD CODE BELOW HERE ############# ---------------------
# 
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
# update <- 201909
# 

# #### 1. Download original data sets from MMA website -----------------
# 
# # Download and read into CSV at the same time
# ftp <- 'http://mapas.mma.gov.br/ms_tmp/ucstodas.shp'
# 
# download.file(url = ftp,
#               destfile = paste0(destdir_raw,"/","ucstodas.shp"),mode = "wb") #mode = "wb" resolve o problema na hora de baixar o arquivo
# 
# ftp <- 'http://mapas.mma.gov.br/ms_tmp/ucstodas.shx'
# download.file(url = ftp,
#               destfile = paste0(destdir_raw,"/","ucstodas.shx"),mode = "wb")
# 
# ftp <- 'http://mapas.mma.gov.br/ms_tmp/ucstodas.dbf'
# download.file(url = ftp,
#               destfile = paste0(destdir_raw,"/","ucstodas.dbf"),mode = "wb")
# 
# 
# 
# 
# 
# 
# #### 2. Unzipe shape files -----------------
# # unecessary
# 
# 
# 
# 
# #### 3. Clean data set and save it in compact .rds format-----------------
# 
# # Root directory
# setwd('./conservation_units')
# 
# 
# # list all csv files
# shape <- list.files(path=paste0("./",update), full.names = T, pattern = ".shp$") # $ para indicar que o nome termina com .shp pois existe outro arquivo com .shp no nome
# 
# # read data
# temp_sf <- st_read(shape, quiet = F, stringsAsFactors=F, options = "ENCODING=latin1") #Encoding usado pelo IBGE (ISO-8859-1) usa-se latin1 para ler acentos
# head(temp_sf)
# 
# # add download date column
# temp_sf$date <- update
# 
# # Rename columns
# temp_sf <- dplyr::rename(temp_sf,
#                          code_conservation_unit = ID_UC0,
#                          name_conservation_unit = NOME_UC1,
#                          id_wcm = ID_WCMC2,
#                          category = CATEGORI3,
#                          group = GRUPO4,
#                          government_level = ESFERA5,
#                          creation_year = ANO_CRIA6,
#                          gid7 = GID7,
#                          quality = QUALIDAD8,
#                          code_u111 = CODIGO_U11,
#                          legislation = ATO_LEGA9,
#                          name_organization = NOME_ORG12,
#                          dt_ultim10 = DT_ULTIM10)
# head(temp_sf)
# 
 