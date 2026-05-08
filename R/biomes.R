#> DATASET: biomes 2004, 2019
#> Source: IBGE - https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/biomas/
#: scale 1:5.000.000
#> Metadata:
# Título: Biomas
# Título alternativo: Biomes
# Frequência de atualização: Ocasional
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Poligonos e Pontos do biomas brasileiros.
# Informações adicionais: Dados produzidos pelo IBGE, e utilizados na elaboracao do shape de biomas com a melhor base oficial disponivel.
# Propósito: Identifição dos biomas brasileiros.
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas:****
# Informação do Sistema de Referência: SIRGAS 2000
#
# Observações: Anos disponíveis: 2004 e 2019*
# *O ano de 2019 é referente aos biomas terrestres do IBGE do sistema costeiro de 2024



###### Download the data  -----------------
download_biomes <- function(year){ # year = 2019

  #### 0. Get the correct ftp link (UPDATE HERE IN CASE OF NEW YEAR IN THE DATA)
  
  zip_dir <- paste0(tempdir(), "/biomes/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  # build url
  if(year == 2006) {
    ftp <- 'https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/biomas/vetores/Biomas_5000mil.zip'
  }
  
  if(year == 2019) {
    ftp <- 'https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/biomas/vetores/Biomas_250mil.zip'
    ftp_costeiro <- 'https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/biomas/vetores/Sistema_Costeiro_Marinho_250mil.zip'
    
    file_raw_costeiro <- fs::file_temp(tmp_dir = zip_dir,
                                       ext = fs::path_ext(ftp_costeiro))
  }
  
  if(year == 2025) {
    ftp <- 'https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/biomas/vetores/2025_Biomas-e-Sistema-Costeiro-Marinho-do-Brasil-1-250000_shp.zip'
  }
  
  # create local file
  file_raw <- fs::file_temp(tmp_dir = zip_dir,
                            ext = fs::path_ext(ftp))
  
  #### 1. Download original data sets from source website
  download.file(url = ftp,
                destfile = file_raw)
  
  if(year == 2019) {
    download.file(url = ftp_costeiro,
                  destfile = file_raw_costeiro)
  }


  #### 2. Unzip shape files
  
  zipfiles <- list.files(path = zip_dir, pattern = basename(file_raw), full.names = T)
  lapply(zipfiles, unzip, exdir = zip_dir)
  
  if(year == 2019) {
    zipfiles_costeiro <- list.files(path = zip_dir, pattern = basename(file_raw_costeiro), full.names = T)
    lapply(zipfiles_costeiro, unzip, exdir = zip_dir)
  }


  #### 3. Read shapefile
  
  if (year == 2006){
  
    biomes_raw <- sf::st_read(
      dsn = zip_dir, 
      layer = "Biomas5000",
      options = "ENCODING=latin1",
      stringsAsFactors = F, 
      quiet = TRUE
      )
    }

  # For 2019, must join earth biomes with coastal system
  if (year == 2019){
  
    raw_costeiro <- sf::st_read(dsn = zip_dir, layer = "Sistema_Costeiro_Marinho",
                            options = "ENCODING = latin1",
                            stringsAsFactors = F, quiet = TRUE)

    
    raw_terrestre <- sf::st_read(dsn = zip_dir, layer = "lm_bioma_250",
                          options = "ENCODING = latin1",
                          stringsAsFactors = F, quiet = TRUE)
    
    raw_costeiro <- raw_costeiro |>
      dplyr::mutate(Bioma = "Sistema Costeiro", CD_Bioma = NA) |>
      dplyr::select(-S_COSTEIRO)
  
    biomes_raw <- dplyr::bind_rows(raw_terrestre, raw_costeiro)
  }
  
  if (year == 2025){
    
    biomes_raw <- sf::st_read(
      dsn = zip_dir, 
      # layer = "Biomas5000",
      stringsAsFactors = F, 
      quiet = TRUE
    )
    
  }
  
  biomes_raw <- biomes_raw |> janitor::clean_names()
  biomes_raw$year <- year
    
  return(biomes_raw)
  
  }



# Clean the data ----------------------------------

# year <- tar_read("years_biomes")[1]
# biomes_raw <- tar_read("biomes_raw",branches = 1)

clean_biomes <- function(biomes_raw) {
  
  # 0. Create folder to save clean data
  yyyy <- biomes_raw$year[1]

  dir_clean <- paste0("./data/biomes/", yyyy)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  


  ## 2. standardize colnames  ---------------------------------------------------
  biomes <- rename_cols_geobr(biomes_raw, dicionario_biomes) |> 
    dplyr::select(
      dplyr::any_of(c("code_biome", "name_biome")),
      year, geometry
      )
  
  # 1. harmonize geobr data
  temp_sf <- harmonize_geobr(
    temp_sf = biomes, 
    year = yyyy,
    add_state = F, 
    add_region = F, 
    add_snake_case = T, 
    snake_colname = "name_biome",
    projection_fix = T, 
    encoding_utf8 = T, 
    topology_fix = T,
    remove_z_dimension = T, 
    use_multipolygon = T
    )

  # sort rows
  biomes <- biomes |> 
    dplyr::arrange(name_biome)
    
  # 3. generate a lighter version of the dataset with simplified borders
  temp_sf_simplified <- simplify_temp_sf(temp_sf)
  
  
  # 4. Save file ---------------------------
    
  write_geobr_parquet(
    sf_obj = temp_sf,
    path = paste0(dir_clean, "/", "biomes_", yyyy, ".parquet")
    )

  write_geobr_parquet(
    sf_obj = temp_sf_simplified,
    path = paste0(dir_clean, "/", "biomes_", yyyy, "_simplified.parquet"))
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}


