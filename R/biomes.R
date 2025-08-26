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
# Proposito: Identificao dos biomas brasileiros.
#
# Estado: Em desenvolvimento
# Palavras chaves descritivas:****
# Informacao do Sistema de Referencia: SIRGAS 2000
#
# Observações: Anos disponíveis: 2004 e 2019*
# *O ano de 2019 é referente aos biomas terrestres do IBGE do sistema costeiro de 2024



###### Download the data  -----------------
download_biomes <- function(year){ # year = 2019

#### 0. Get the correct ftp link (UPDATE HERE IN CASE OF NEW YEAR IN THE DATA):  ----
  
  if(year == 2004) {
    ftp <- 'https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/biomas/vetores/Biomas_5000mil.zip'
  }
  
  if(year == 2019) {
    ftp <- 'https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/biomas/vetores/Biomas_250mil.zip'
    ftp_costeiro <- 'https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/biomas/vetores/Sistema_Costeiro_Marinho_250mil.zip'
    
    file_raw_costeiro <- fs::file_temp(ext = fs::path_ext(ftp_costeiro))
    
  }
  
  file_raw <- fs::file_temp(ext = fs::path_ext(ftp))
  tmp_dir <- tempdir()
  
  #### 1. Download original data sets from source website -----------------
  
  download.file(url = ftp,
                destfile = file_raw)
  
  if(year == 2019) {
    download.file(url = ftp_costeiro,
                  destfile = file_raw_costeiro)
  }


  #### 2. Unzip shape files -----------------
  
  zipfiles <- list.files(path = tmp_dir, pattern = basename(file_raw), full.names = T)
  lapply(zipfiles, unzip, exdir = tmp_dir)
  
  if(year == 2019) {
    zipfiles_costeiro <- list.files(path = tmp_dir, pattern = basename(file_raw_costeiro), full.names = T)
    lapply(zipfiles_costeiro, unzip, exdir = tmp_dir)
  }


  #### 3. Read shapefile -----------------
  
  if (year == 2004){
  
    raw_biomes <- st_read(dsn = tmp_dir, layer = "Biomas5000",
                        options = "ENCODING = latin1",
                        stringsAsFactors = F, quiet = TRUE) %>% 
    mutate(across(where(is.character),
                  ~ iconv (.x, from = "latin1", to = "UTF-8")))
  }



  # For 2019, must join earth biomes with coastal system
  if (year == 2019){
  
    raw_costeiro <- st_read(dsn = tmp_dir, layer = "Sistema_Costeiro_Marinho",
                            options = "ENCODING = latin1",
                            stringsAsFactors = F, quiet = TRUE)

    
    raw_terrestre <- st_read(dsn = tmp_dir, layer = "lm_bioma_250",
                          options = "ENCODING = latin1",
                          stringsAsFactors = F, quiet = TRUE)
    
    raw_costeiro <- raw_costeiro %>%
      mutate(Bioma = "Sistema Costeiro", CD_Bioma = NA) %>%
      select(-S_COSTEIRO)
  
    raw_biomes <- rbind(raw_terrestre, raw_costeiro)
    }
    
  return(raw_biomes)
  
  }



######## Clean the data ----

clean_biomes <- function(raw_biomes, year) {
  
#### 0. Create folder to save clean sf.rds files -----------------

  dir_clean <- paste0("./data/biomes/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  

#### 1. Check geometry and make valid -----------------

st_is_valid(raw_biomes)
st_is_valid(raw_biomes, reason = TRUE)
  
raw_biomes <- sf::st_make_valid(raw_biomes)

st_is_valid(raw_biomes, reason = TRUE)


# save to open in mapshaper to check if the file is working
sf::st_write(raw_biomes, dsn = paste0(dir_clean, "/", "biomes_", year, ".kml"), year = TRUE)
  
  
temp_sf <- st_make_valid(raw_biomes)

st_is_valid(temp_sf)

####### Se for diferente de tudo TRUE, criar função para mostrar ERROR



#### 2. CORRECT SHAPE And Make valid geometry -----------------
  
temp_sf2 <- dissolve_polygons(temp_sf, 'Bioma')

#### 3. Rename columns an reorder -------------------------

if ( year == 2004){
names(temp_sf) <- c('code_biome', 'name_biome', 'n_biome' , 'geometry', "year")
}

if ( year == 2019){
names(temp_sf) <- c('name_biome', 'code_biome', 'geometry', "year")
}

#########################

temp_sf2 <- temp_sf |>
  group_by(Bioma, CD_Bioma) |>
  summarise(geometry = st_union(geometry))

temp_sf3 <- temp_sf |>
  mutate(geometry = s2::as_s2_geography(geometry)) |>
  group_by(Bioma, CD_Bioma) |>
  summarise(geometry = s2::s2_union_agg(geometry)) |>
  mutate(geometry = st_as_sfc(geometry))





##### 4. Rename columns -------------------------
# 
# if ( year == 2004){
#   temp_sf <- dplyr::rename(temp_sf, code_biome = COD_BIOMA, name_biome = NOM_BIOMA)
# 
#   
# }
# 
# 
# if ( year == 2019){
# 
#   # rename columns and pile files up
#   temp_sf <- dplyr::rename(temp_sf, code_biome = CD_Bioma, name_biome = Bioma)
#   temp_sf$year <- year
# 
# 
#   temp_sf_costeiro$name_biome <- "Sistema Costeiro"
#   temp_sf_costeiro$code_biome <- NA
#   temp_sf_costeiro$year <- year
#   temp_sf_costeiro$S_COSTEIRO <- NULL
# 
# # reorder columns
# setcolorder(temp_sf, neworder= c('name_biome', 'code_biome', 'year', 'geometry'))
# setcolorder(temp_sf_costeiro, neworder= c('name_biome', 'code_biome', 'year', 'geometry'))
# 
# 
# }
# 

# make valid geometry
temp_sf <- st_make_valid(temp_sf)
st_is_valid(temp_sf, reason = TRUE)



##### 5. Check projection, UTF, topology, etc -------------------------


# Harmonize spatial projection CRS, using SIRGAS 2000 epsg (SRID): 4674
temp_sf <- harmonize_projection(temp_sf)
st_is_valid(temp_sf)


# 
# # Use UTF-8 encoding in all character columns
# temp_sf <- temp_sf %>%
#   mutate_if(is.factor, function(x){ x %>% as.character() %>%
#       stringi::stri_encode("UTF-8") } )
# temp_sf <- temp_sf %>%
#   mutate_if(is.factor, function(x){ x %>% as.character() %>%
#       stringi::stri_encode("UTF-8") } )


###### convert to MULTIPOLYGON -----------------
temp_sf <- to_multipolygon(temp_sf)

###### 6. generate a lighter version of the dataset with simplified borders -----------------
# skip this step if the dataset is made of points, regular spatial grids or rater data

# simplify
temp_sf_simplified <- st_transform(temp_sf, crs = 3857) %>%
  sf::st_simplify(preserveTopology = T, dTolerance = 100) %>%
  st_transform(crs = 4674)
head(temp_sf_simplified)


###### 8. Clean data set and save it in geopackage format-----------------

##### Save file -------------------------

# Save original and simplified datasets
saveRDS(temp_sf, file = paste0(dir_clean, "/", "biomes_", year, ".rds"))
sf::st_write(temp_sf, dsn = paste0(dir_clean, "/", "biomes_", year, ".gpkg"), year = TRUE)
sf::st_write(temp_sf_simplified, dsn = paste0(dir_clean, "/", "biomes_", year, "_simplified", ".gpkg"), append = TRUE)

}


