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



###### Download the data  -----------------
download_biomes <- function(year){ # year = 2019

#### 0. Get the correct ftp link (UPDATE HERE IN CASE OF NEW YEAR IN THIS DATA):  ----
  
  if(year == 2004) {
    ftp <- 'https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/biomas/vetores/Biomas_5000mil.zip'
  }
  
  if(year == 2019) {
    ftp <- 'https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/biomas/vetores/Biomas_250mil.zip'
  }
  
file_raw <- fs::file_temp(ext = fs::path_ext(ftp))
tmp_dir <- tempdir()

#### 1. Download original data sets from source website -----------------

download.file(url = ftp,
              destfile = file_raw)

#### 2. Unzip shape files -----------------

zipfiles <- list.files(path = tmp_dir, pattern = basename(file_raw), full.names = T)
lapply(zipfiles, unzip, exdir = tmp_dir)

#Outras opções de montar o zip                
#paste0(dest,"/","Biomas_250mil_costeiro.zip")))


#### 3. Read shapefile -----------------

if (year == 2004){

  raw_biomes <- st_read(dsn = tmp_dir, layer = "Biomas5000",
                      options = "ENCODING = latin1",
                      stringsAsFactors = F, quiet = TRUE) %>% 
  mutate(across(where(is.character),
                ~ iconv (.x, from = "latin1", to = "UTF-8")))
}

if (year == 2019){

  raw_biomes <- st_read(dsn = tmp_dir, layer = "lm_bioma_250",
                        options = "ENCODING = latin1",
                        stringsAsFactors = F, quiet = TRUE) %>% 
    mutate(across(where(is.character),
                  ~ iconv (.x, from = "latin1", to = "UTF-8")))
  }

#### 4. Deliver the raw_biomes (LIMPAR AQUI) -----------------

# # O que fazer com o shape costeiro?
# ftp_costeiro <- 'https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/biomas/vetores/Sistema_Costeiro_Marinho_250mil.zip'

 
# download.file(url = ftp_costeiro,
#               destfile = paste0(destdir_raw,
#                                 "/","Biomas_250mil_costeiro.zip"))

return(raw_biomes)

}



######## Clean the data ----

clean_biomes <- function(raw_biomes) {
  

#### 1. Clean data set and save it in compact .rds format-----------------

# list all csv files
shape <- list.files(path = tmp_dir,
                    full.names = T,
                    pattern = ".shp$")

# read data
if ( year == 2004){
  
  
  
  temp_sf <- st_read(shape, quiet = F, stringsAsFactors=F, options = "ENCODING = latin1") #Encoding usado pelo IBGE (ISO-8859-1) usa-se latin1 para ler acentos
  }

if ( year == 2019){
  temp_sf <- st_read(shape[1], quiet = F, stringsAsFactors=F)
  temp_sf_costeiro <- st_read(shape[2], quiet = F, stringsAsFactors=F)

  # make valid geometry
  temp_sf_costeiro <- st_make_valid(temp_sf_costeiro)
  # st_is_valid(temp_sf_costeiro, reason = TRUE)

}

# make valid geometry
temp_sf <- st_make_valid(temp_sf)
# st_is_valid(temp_sf, reason = TRUE)


temp_sf <- st_read(shape[1], quiet = F, stringsAsFactors=F)

temp_sf2 <- temp_sf |>
  fgroup_by(Bioma, CD_Bioma) |>
  fsummarise(geometry = st_union(geometry))

temp_sf3 <- temp_sf |>
  fmutate(geometry = s2::as_s2_geography(geometry)) |>
  fgroup_by(Bioma, CD_Bioma) |>
  fsummarise(geometry = s2::s2_union_agg(geometry)) |>
  fmutate(geometry = st_as_sfc(geometry))





##### 4. Rename columns -------------------------

if ( year == 2004){
  temp_sf <- dplyr::rename(temp_sf, code_biome = COD_BIOMA, name_biome = NOM_BIOMA)

  # Create columns with date and with state codes
  temp_sf$year <- year

  head(temp_sf)
}


if ( year == 2019){

  # rename columns and pile files up
  temp_sf <- dplyr::rename(temp_sf, code_biome = CD_Bioma, name_biome = Bioma)
  temp_sf$year <- year


  temp_sf_costeiro$name_biome <- "Sistema Costeiro"
  temp_sf_costeiro$code_biome <- NA
  temp_sf_costeiro$year <- year
  temp_sf_costeiro$S_COSTEIRO <- NULL

# reorder columns
setcolorder(temp_sf, neworder= c('name_biome', 'code_biome', 'year', 'geometry'))
setcolorder(temp_sf_costeiro, neworder= c('name_biome', 'code_biome', 'year', 'geometry'))

# pille them up
temp_sf <- rbind(temp_sf, temp_sf_costeiro)
}


# make valid geometry
temp_sf <- st_make_valid(temp_sf)
# st_is_valid(temp_sf, reason = TRUE)



##### 5. Check projection, UTF, topology, etc -------------------------


# Harmonize spatial projection CRS, using SIRGAS 2000 epsg (SRID): 4674
temp_sf <- harmonize_projection(temp_sf)


# Make any invalid geometry valid # st_is_valid( sf)
temp_sf <- lwgeom::st_make_valid(temp_sf)


# Use UTF-8 encoding in all character columns
temp_sf <- temp_sf %>%
  mutate_if(is.factor, function(x){ x %>% as.character() %>%
      stringi::stri_encode("UTF-8") } )
temp_sf <- temp_sf %>%
  mutate_if(is.factor, function(x){ x %>% as.character() %>%
      stringi::stri_encode("UTF-8") } )



###### convert to MULTIPOLYGON -----------------
temp_sf <- to_multipolygon(temp_sf)

###### 6. generate a lighter version of the dataset with simplified borders -----------------
# skip this step if the dataset is made of points, regular spatial grids or rater data

# simplify
temp_sf_simplified <- st_transform(temp_sf, crs=3857) %>%
  sf::st_simplify(preserveTopology = T, dTolerance = 100) %>%
  st_transform(crs=4674)
head(temp_sf_simplified)



###### 8. Clean data set and save it in geopackage format-----------------
setwd(root_dir)


##### Save file -------------------------

# Save original and simplified datasets
readr::write_rds(temp_sf, path= paste0("./shapes_in_sf_cleaned/",year,"/biomes_", year,".rds"), compress = "gz")
sf::st_write(temp_sf, dsn= paste0("./shapes_in_sf_cleaned/",year,"/biomes_", year,".gpkg"), year = TRUE)
sf::st_write(temp_sf_simplified, dsn= paste0("./shapes_in_sf_cleaned/",year,"/biomes_", year," _simplified", ".gpkg"), update = TRUE)

}


