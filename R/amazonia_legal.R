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
# Palavras-chaves descritivas:****
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

   
###### 3. Apply geobr cleaning -----------------

   temp_sf <- harmonize_geobr(
     temp_sf = amazonialegal_raw, 
     add_state = F, 
     add_region = F, 
     add_snake_case = F, 
     #snake_colname = snake_colname,
     projection_fix = T,
     encoding_utf8 = F, 
     topology_fix = T,
     remove_z_dimension = T,
     use_multipolygon = T
   )

glimpse(temp_sf)
   
###### 4. generate a lighter version of the dataset with simplified borders -----------------

# simplify
temp_sf2 <- simplify_temp_sf(temp_sf)
head(temp_sf2)


###### 5. Clean data set and save it in geopackage format-----------------

#save original and simplified datasets
# sf::st_write(temp_sf, append = FALSE, dsn = paste0(dir_clean, "amazonialegal", ".gpkg") )
# sf::st_write(temp_sf2, append = FALSE, dsn = paste0(dir_clean, "amazonialegal","_simplified", ".gpkg"))

arrow::write_parquet(
  x = temp_sf, 
  sink = paste0(dir_clean, "amazonialegal", ".parquet"),
  compression='zstd',
  compression_level = 22
  )

arrow::write_parquet(
  x = temp_sf2, 
  sink = paste0(dir_clean, "amazonialegal","_simplified", ".parquet"),
  compression='zstd',
  compression_level = 22
)

return(dir_clean)
}

