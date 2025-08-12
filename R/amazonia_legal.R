#> DATASET: legal amazon
#> Source: MMA - http://mapas.mma.gov.br/i3geo/datadownload.htm
#> Metadata:
# Título: Amazônia Legal
# Título alternativo: Amazonia legal
# Frequência de atualização: ?
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
download_amazonialegal <- function(year){ # year = 2022

# If the data set is updated regularly, you should create a function that will have
# a `date` argument download the data

year <- 2012

###### 0. Create directories to downlod and save the data -----------------

# Directory to keep raw zipped files and cleaned files

dir.create("./data_raw/amazonia_legal", showWarnings = FALSE)
dir.create("./data/amazonia_legal", showWarnings = FALSE)
dir_raw <- paste0("./data_raw/amazonia_legal/", year)
dir.create(dir_raw, recursive = T, , showWarnings = FALSE)


###### 1. download the raw data from the original website source -----------------

# Download and read into CSV at the same time
ftp_shp <- 'http://mapas.mma.gov.br/ms_tmp/amazlegal.shp'
ftp_shx <- 'http://mapas.mma.gov.br/ms_tmp/amazlegal.shx'
ftp_dbf <- 'http://mapas.mma.gov.br/ms_tmp/amazlegal.dbf'
ftp <- c(ftp_shp,ftp_shx,ftp_dbf)
aux_ft <- c("shp","shx","dbf")

# Directions do download the file
file_raw <-fs::file_temp(ext = fs::path_ext(ftp))


for(i in 1:length(ftp)){
  
  # download.file(url = ftp[i],
  #               destfile = paste0(destdir_raw,"/","amazonia_legal.",aux_ft[i]) )
  httr::GET(url=ftp[i],
            httr::write_disk(path=paste0(file_raw,"/","amazonia_legal.",aux_ft[i]), overwrite = F))
  
}


#oldversion
# for(i in 1:length(ftp)){
# 
#     # download.file(url = ftp[i],
#     #               destfile = paste0(destdir_raw,"/","amazonia_legal.",aux_ft[i]) )
#   httr::GET(url=ftp[i],
#             httr::write_disk(path=paste0(destdir_raw,"/","amazonia_legal.",aux_ft[i]), overwrite = F))
# 
#   }





###### 2. rename column names -----------------
setwd(file_raw)

# read data
temp_sf <- sf::st_read("./amazonia_legal.shp", quiet = F, stringsAsFactors=F)


# Rename columns
temp_sf$GID0 <- NULL
temp_sf$ID1 <- NULL



###### 3. ensure the data uses spatial projection SIRGAS 2000 epsg (SRID): 4674-----------------

temp_sf3 <- harmonize_projection(temp_sf)

# Harmonize spatial projection CRS, using SIRGAS 2000 epsg (SRID): 4674
temp_sf <- harmonize_projection(temp_sf)

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

# Make any invalid geometry valid # st_is_valid( sf)
temp_sf6 <- lwgeom::st_make_valid(temp_sf5)


###### convert to MULTIPOLYGON -----------------
temp_sf6 <- to_multipolygon(temp_sf6)


###### 7. generate a lighter version of the dataset with simplified borders -----------------
# skip this step if the dataset is made of points, regular spatial grids or rater data

# simplify
temp_sf7 <- simplify_temp_sf(temp_sf6)
head(temp_sf7)



###### 8. Clean data set and save it in geopackage format-----------------
setwd(root_dir)

# save original and simplified datasets
sf::st_write(temp_sf6, dsn= paste0(destdir_clean, "/amazonia_legal_", update, ".gpkg") )
sf::st_write(temp_sf7, dsn= paste0(destdir_clean, "/amazonia_legal_", update," _simplified", ".gpkg"))


