#> DATASET: Brazilian legal amazon
#> Source: MMA - http://mapas.mma.gov.br/i3geo/datadownload.htm
#> Metadata:
# Título: Amazônia Legal
# Título alternativo: Amazonia legal
# Frequência de atualização: Nunca
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Polígonos e Pontos da Amazônia Legal brasileira.
# Informações adicionais: Dados produzidos pelo MMA com base legal que consta no código florestal (lei 12.651).
# Propósito: Identificaçãao da Amazônia legal.
#
# Estado: Em desenvolvimento
# Palavras chaves descritivas:****
# Informação do Sistema de Referência: SIRGAS 2000


##### Download the data  -----------------
download_amazonialegal <- function(){ # i've removed the year argument,
  # because the file is atomic. Only have had one update.


  # Download and read into CSV at the same time ----
  ftp_shp <- 'http://mapas.mma.gov.br/ms_tmp/amazlegal.shp'
  ftp_shx <- 'http://mapas.mma.gov.br/ms_tmp/amazlegal.shx'
  ftp_dbf <- 'http://mapas.mma.gov.br/ms_tmp/amazlegal.dbf'
  ftp <- c(ftp_shp,ftp_shx,ftp_dbf)


  # Directions do download the file ------
  temp_dir <- fs::path_temp()

  for(i in 1:length(ftp)){
    
    file_name <- basename(ftp[i])
    
    temp_download <-  httr::GET(
      url = ftp[i], 
      httr::write_disk(path = paste0(temp_dir,"/",file_name),
                       overwrite = T)
    )
   }
  
  # Save in the temp directory ----
  shp_file <- basename(ftp_shp)
  shp_dir <- paste0(temp_dir,"/",shp_file)
  
  # read data
  temp_sf <- sf::st_read(
    shp_dir, 
    quiet = F, 
    stringsAsFactors=F
    )
  
  return(temp_sf)
  
  }


##### Clean the data

###### 1. create clean directory -----------------


clean_amazonialegal <- function(amazonialegal_raw){
 
  #create directory
   dir_clean <- "./data/amazonia_legal/"
   dir.create(dir_clean, showWarnings = FALSE)
   

###### 2. rename column names -----------------

# Rename columns
   amazonialegal_raw$GID0 <- NULL
   amazonialegal_raw$ID1 <- NULL


###### 3. ensure the data uses spatial projection SIRGAS 2000 epsg (SRID): 4674-----------------

temp_sf3 <- harmonize_projection(amazonialegal_raw)

# Harmonize spatial projection CRS, using SIRGAS 2000 epsg (SRID): 4674

st_crs(temp_sf3)$epsg
st_crs(temp_sf3)$input
st_crs(temp_sf3)$proj4string
st_crs(st_crs(temp_sf3)$wkt) == st_crs(temp_sf3)




###### 4. ensure every string column is as.character with UTF-8 encoding -----------------

# not necessary here



###### 5. remove Z dimension of spatial data-----------------

# remove Z dimension of spatial data
temp_sf5 <- temp_sf3 %>% st_sf() %>% st_zm( drop = T, what = "ZM")



###### 6. fix eventual topology issues in the data-----------------

##### Make any invalid geometry valid # st_is_valid( sf)
temp_sf6 <- fix_topology(temp_sf5)


###### convert to MULTIPOLYGON -----------------
temp_sf6 <- to_multipolygon(temp_sf6)


###### 7. generate a lighter version of the dataset with simplified borders -----------------
# skip this step if the dataset is made of points, regular spatial grids or rater data

# simplify
temp_sf7 <- simplify_temp_sf(temp_sf6)
head(temp_sf7)


###### 8. Clean data set and save it in geopackage format-----------------

#save original and simplified datasets
sf::st_write(temp_sf6, append = FALSE, dsn = paste0(dir_clean, "amazonialegal", ".gpkg") )
sf::st_write(temp_sf7, append = FALSE, dsn = paste0(dir_clean, "amazonialegal","_simplified", ".gpkg"))

return(dir_clean)
}









