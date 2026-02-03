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
  
  if(year == 2004) {
    ftp <- 'https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/biomas/vetores/Biomas_5000mil.zip'
  }
  
  if(year == 2019) {
    ftp <- 'https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/biomas/vetores/Biomas_250mil.zip'
    ftp_costeiro <- 'https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/biomas/vetores/Sistema_Costeiro_Marinho_250mil.zip'
    
    file_raw_costeiro <- fs::file_temp(tmp_dir = zip_dir,
                                       ext = fs::path_ext(ftp_costeiro))
    
  }
  
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
  
  if (year == 2004){
  
    biomes_raw <- st_read(dsn = zip_dir, layer = "Biomas5000",
                        options = "ENCODING = latin1",
                        stringsAsFactors = F, quiet = TRUE) %>% 
    mutate(across(where(is.character),
                  ~ iconv (.x, from = "latin1", to = "UTF-8")))
  }



  # For 2019, must join earth biomes with coastal system
  if (year == 2019){
  
    raw_costeiro <- st_read(dsn = zip_dir, layer = "Sistema_Costeiro_Marinho",
                            options = "ENCODING = latin1",
                            stringsAsFactors = F, quiet = TRUE)

    
    raw_terrestre <- st_read(dsn = zip_dir, layer = "lm_bioma_250",
                          options = "ENCODING = latin1",
                          stringsAsFactors = F, quiet = TRUE)
    
    raw_costeiro <- raw_costeiro %>%
      mutate(Bioma = "Sistema Costeiro", CD_Bioma = NA) %>%
      select(-S_COSTEIRO)
  
    biomes_raw <- rbind(raw_terrestre, raw_costeiro)
  }
  
  biomes_raw$year <- year
    
  return(biomes_raw)
  
  }



# Clean the data ----------------------------------

# year <- tar_read("years_biomes")[1]
# biomes_raw <- tar_read("biomes_raw",branches = 1)

clean_biomes <- function(biomes_raw, year) {
  
  # 0. Create folder to save clean data

  dir_clean <- paste0("./data/biomes/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  

  # define colnames
  snake_colname <- switch(
    as.character(year),
    "2004" = "NOM_BIOMA",
    "2019" = "Bioma",
    NA_character_
  )
  
  id_colname <- switch(
    as.character(year),
    "2004" = "ID1",
    "2019" = "CD_Bioma",
    NA_character_
  )
  
  # 2. Rename and reorder columns
  biomes_raw <- biomes_raw |>
    select('name_biome' = all_of(snake_colname), # 666 tidyselect pedindo pra mudar: tem que vir all_of() ou any_of() antes de snake selection
           'code_biome' = all_of(id_colname), # 666 tidyselect pedindo pra mudar: ""
           year, 
           geometry
    ) |> 
    arrange(name_biome)
  
  # 1. harmonize geobr data
  temp_sf <- harmonize_geobr(
    temp_sf = biomes_raw, 
    year = year,
    add_state = F, 
    add_region = F, 
    add_snake_case = T, 
    snake_colname = snake_colname,
    projection_fix = T, 
    encoding_utf8 = T, 
    topology_fix = T,
    remove_z_dimension = T, 
    use_multipolygon = T
    )

    
    
  # 3. generate a lighter version of the dataset with simplified borders
  # skip this step if the dataset is made of points, regular spatial grids or rater data

    temp_sf_simplified <- simplify_temp_sf(temp_sf)
  
  
  # 4. Save file
  # sf::st_write(temp_sf, dsn = paste0(dir_clean, "/", "biomes_", year, ".gpkg"), append=FALSE)
  # sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean, "/", "biomes_", year, "_simplified", ".gpkg"), append=FALSE)

  # Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/", "biomes_", year, ".parquet"),
    compression='zstd',
    compression_level = 7
  )

  arrow::write_parquet(
    x = temp_sf_simplified,
    sink = paste0(dir_clean, "/", "biomes_", year, "_simplified", ".parquet"),
    compression='zstd',
    compression_level = 7
  )
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}


