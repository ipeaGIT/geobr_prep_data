#> DATASET: statistical grid 2010, 2022
#> Source: ###### IBGE - ftp://geoftp.ibge.gov.br/recortes_para_fins_estatisticos/grade_estatistica/censo_2010/
#########: scale 1:5.000.000
#> Metadata: #####
# Título: Grade Estatística
# Título alternativo: #####Statistical Grid 2010 Census
# Frequência de atualização: Ocasionalmente
#
# Forma de apresentação: #####Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: ##### Poligonos e Pontos do biomas brasileiros.
# Informações adicionais: Dados produzidos pelo IBGE, e utilizados na elaboracao do shape da base estatística com a melhor base oficial disponível.
# Propósito: Disponibilização da grade estatística do Brasil.
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas: ****
# Informação do Sistema de Referência: #####SIRGAS 2000
#
# Observações: 
# Anos disponíveis: 2010, 2022


####### Download the data  -----------------
 download_statsgrid <- function(){ # year = 2010

### USE temporariamente
   library(targets)
   library(tidyverse)
   library(data.table)
   library(mirai)
   library(RCurl)
   source("./R/support_harmonize_geobr.R")
   source("./R/support_fun.R")

  ###### 0. Get the correct url and file names -----------------
   
   if(year == 2010) {
     url = "ftp://geoftp.ibge.gov.br/recortes_para_fins_estatisticos/grade_estatistica/censo_2010/"
   }
   
   if(year == 2022) {
     url = "https://geoftp.ibge.gov.br/recortes_para_fins_estatisticos/grade_estatistica/censo_2022/grade_estatistica/"
   }
   
   
  ###### 1. Create temp folder -----------------
   
   file_raw <- fs::file_temp(ext = fs::path_ext(url))
   tmp_dir <- tempdir()
   
  #### Generate file names
   filenames = getURL(url, ftp.use.epsv = FALSE, dirlistonly = TRUE)
   filenames <- strsplit(filenames, "\r\n")
   filenames = unlist(filenames)
   
   # Filter only the zip ones
   filenames <- subset(filenames, grepl(filenames, pattern = ".zip"), value = TRUE)
   
   
   #list_folders(url) #não funciona
   
  #### Create direction for each download
     
  list_files_download <- paste0(url, filenames)
  
  ###### 2. Download Raw data -----------------
  
  # Download zipped files
    for (name_file in filenames) {
      download.file(paste(url, name_file, sep = ""), paste(tmp_dir, name_file, sep = "\\"))
   print("Done")
    }

  ###### 3. Unzip Raw data -----------------
  
  # directory of zips
  zip_names <- list.files(tmp_dir, pattern = "\\.zip", full.names = TRUE)
  
  # zip folder
  out_zip <- paste0(tmp_dir, "/unzipped")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)

  pbapply::pblapply(
    X = zip_names, 
    FUN = function(x){ unzip(zipfile = x, exdir = out_zip) }
      )
  
  # #### 666666 paralelizacao
  # # numero de cores
  # mirai::daemons(28)
  # 
  # mirai::mirai_map(
  #   .x = zip_names, 
  #   .f = function(x){ unzip(zipfile = x, exdir = out_zip) },
  #   .progress = T
  # )
  # mirai::daemons(0)
  




  ###### 4. Bind Raw data together -----------------
  
  shp_names <- list.files(out_zip, pattern = "\\.shp$", full.names = TRUE)
  # shp_names <- shp_names[1:2]
  
  # paralelizar ?
  statsgrid_list <- pbapply::pblapply(
    X = shp_names, 
    FUN = function(x){ sf::st_read(x, quiet = T, stringsAsFactors=F) }
    )
  
  statsgrid_raw <- data.table::rbindlist(statsgrid_list)
  data.table::setDF(statsgrid_raw)
  statsgrid_raw <- sf::st_as_sf(statsgrid_raw)


  return(statsgrid_raw)
}
   


# Clean the data ----------------------------------
  clean_statsgrid <- function(year, statsgrid_raw) {

  # 0. Create folder to save clean data
    
  dir_clean <- paste0("./data/statistical_grid/", year)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  
  # 1. harmonize geobr data
  # 4. Save file
  
  return(dir_clean)
  }

# 
# # list all shape files
#   all_shapes <- list.files(full.names = T, recursive = T, pattern = ".shp")
#   all_shapes <- all_shapes[ !(all_shapes %like% ".xml")]
# 
# 
# 
# shp_to_sf_rds <- function(x){
# 

  
# # drop unecessary columns
#   shape$Shape_Leng <- NULL
#   shape$Shape_Area <- NULL
# 
# 
#   ###### convert to MULTIPOLYGON -----------------
#   temp_sf <- to_multipolygon(temp_sf)
# 
# 
#   ###### 6. generate a lighter version of the dataset with simplified borders -----------------
#   # skip this step if the dataset is made of points, regular spatial grids or rater data
# 
#   # # simplify
#   # shape_simplified <- st_transform(shape, crs=3857) %>%
#   #   sf::st_simplify(preserveTopology = T, dTolerance = 100) %>%
#   #   st_transform(crs=4674)
#   # head(shape)
# 
# # get file name
#   file_name <- paste0(substr(x, 11, 12), "grid.rds")
# 
# # save in .rds
#   readr::write_rds(x=shape, path = paste0("../shapes_in_sf_all_years_cleaned/2010/", substr(x, 11, 12), "grid.rds"), compress="gz" )
#   sf::st_write(temp_sf,  dsn= paste0("../shapes_in_sf_all_years_cleaned/2010/", substr(x, 11, 12), "grid.gpkg") )
#   # sf::st_write(temp_sf7, dsn= paste0("../shapes_in_sf_all_years_cleaned/2010/", substr(x, 11, 12), "grid_simplified", ".gpkg"))
#   }
# 
# 
# # Apply function to save original data sets in rds format
# 
# # create computing clusters
#   cl <- parallel::makeCluster(detectCores())
# 
#   clusterEvalQ(cl, c(library(data.table), library(readr), library(sf)))
#   parallel::clusterExport(cl=cl, varlist= c("all_shapes"), envir=environment())
# 
#   # apply function in parallel
#   parallel::parLapply(cl, all_shapes, shp_to_sf_rds)
#   stopCluster(cl)
# 
#   rm(list= ls())
#   gc(reset = T)
# 
# 
# # # DO NOT run
# #   # remove all unzipped shape files
# #     # list all unzipped shapes
# #       f <- list.files(path = root_dir, full.names = T, recursive = T, pattern = ".shx|.shp|.prj|.dbf|.cpg|.xml|.sbx|.sbn")
# #       file.remove(f)
# 
# 
# ###### 4. Prepare table with correspondence between grid ID and code_state -----------------
# 
# 
# grid_state_correspondence_table <- structure(list(name_uf = c("Acre", "Acre", "Acre", "Acre", "Amazonas",
#                                   "Amazonas", "Amazonas", "Amazonas", "Amazonas", "Amazonas", "Amazonas",
#                                   "Amazonas", "Amazonas", "Amazonas", "Amazonas", "Amazonas", "Roraima",
#                                   "Roraima", "Roraima", "Roraima", "Roraima", "Roraima", "Amap\u00e1",
#                                   "Amap\u00e1", "Amap\u00e1", "Amap\u00e1", "Par\u00e1", "Par\u00e1", "Par\u00e1", "Par\u00e1", "Par\u00e1",
#                                   "Par\u00e1", "Par\u00e1", "Par\u00e1", "Par\u00e1", "Par\u00e1", "Par\u00e1", "Par\u00e1", "Par\u00e1",
#                                   "Maranh\u00e3o", "Maranh\u00e3o", "Maranh\u00e3o", "Maranh\u00e3o", "Maranh\u00e3o", "Maranh\u00e3o",
#                                   "Maranh\u00e3o", "Piau\u00ed", "Piau\u00ed", "Piau\u00ed", "Piau\u00ed", "Piau\u00ed", "Piau\u00ed",
#                                   "Cear\u00e1", "Cear\u00e1", "Cear\u00e1", "Rio Grande do Norte", "Rio Grande do Norte",
#                                   "Para\u00edba", "Para\u00edba", "Pernambuco", "Pernambuco", "Pernambuco",
#                                   "Pernambuco", "Pernambuco", "Alagoas", "Alagoas", "Sergipe",
#                                   "Sergipe", "Bahia", "Bahia", "Bahia", "Bahia", "Bahia", "Esp\u00edrito Santo",
#                                   "Esp\u00edrito Santo", "Esp\u00edrito Santo", "Rio de Janeiro", "Rio de Janeiro",
#                                   "Rio de Janeiro", "Rio de Janeiro", "S\u00e3o Paulo", "S\u00e3o Paulo",
#                                   "S\u00e3o Paulo", "S\u00e3o Paulo", "S\u00e3o Paulo", "Paran\u00e1", "Paran\u00e1", "Santa Catarina",
#                                   "Santa Catarina", "Santa Catarina", "Santa Catarina", "Rio Grande do Sul",
#                                   "Rio Grande do Sul", "Rio Grande do Sul", "Rio Grande do Sul",
#                                   "Mato Grosso do Sul", "Mato Grosso do Sul", "Mato Grosso do Sul",
#                                   "Mato Grosso do Sul", "Mato Grosso do Sul", "Mato Grosso do Sul",
#                                   "Mato Grosso do Sul", "Minas Gerais", "Minas Gerais", "Minas Gerais",
#                                   "Minas Gerais", "Minas Gerais", "Minas Gerais", "Minas Gerais",
#                                   "Minas Gerais", "Goi\u00e1s", "Goi\u00e1s", "Goi\u00e1s", "Goi\u00e1s", "Goi\u00e1s",
#                                   "Goi\u00e1s", "Goi\u00e1s", "Distrito Federal", "Tocantins", "Tocantins",
#                                   "Tocantins", "Tocantins", "Tocantins", "Mato Grosso", "Mato Grosso",
#                                   "Mato Grosso", "Mato Grosso", "Mato Grosso", "Mato Grosso", "Mato Grosso",
#                                   "Mato Grosso", "Mato Grosso", "Mato Grosso", "Rond\u00f4nia", "Rond\u00f4nia",
#                                   "Rond\u00f4nia", "Rond\u00f4nia", "Rond\u00f4nia", "Rond\u00f4nia"), code_state = c("AC",
# "AC", "AC", "AC", "AM", "AM", "AM", "AM", "AM", "AM", "AM", "AM",
# "AM", "AM", "AM", "AM", "RR", "RR", "RR", "RR", "RR", "RR", "AP",
# "AP", "AP", "AP", "PA", "PA", "PA", "PA", "PA", "PA", "PA", "PA",
# "PA", "PA", "PA", "PA", "PA", "MA", "MA", "MA", "MA", "MA", "MA",
# "MA", "PI", "PI", "PI", "PI", "PI", "PI", "CE", "CE", "CE", "RN",
# "RN", "PB", "PB", "PE", "PE", "PE", "PE", "PE", "AL", "AL", "SE",
# "SE", "BA", "BA", "BA", "BA", "BA", "ES", "ES", "ES", "RJ", "RJ",
# "RJ", "RJ", "SP", "SP", "SP", "SP", "SP", "PR", "PR", "SC", "SC",
# "SC", "SC", "RS", "RS", "RS", "RS", "MS", "MS", "MS", "MS", "MS",
# "MS", "MS", "MG", "MG", "MG", "MG", "MG", "MG", "MG", "MG", "GO",
# "GO", "GO", "GO", "GO", "GO", "GO", "DF", "TO", "TO", "TO", "TO",
# "TO", "MT", "MT", "MT", "MT", "MT", "MT", "MT", "MT", "MT", "MT",
# "RO", "RO", "RO", "RO", "RO", "RO"), code_grid = c("ID_50", "ID_51",
# "ID_60", "ID_61", "ID_51", "ID_60", "ID_61", "ID_62", "ID_63",
# "ID_70", "ID_71", "ID_72", "ID_73", "ID_80", "ID_81", "ID_82",
# "ID_72", "ID_81", "ID_82", "ID_83", "ID_92", "ID_93", "ID_74",
# "ID_75", "ID_84", "ID_85", "ID_53", "ID_54", "ID_55", "ID_63",
# "ID_64", "ID_65", "ID_73", "ID_74", "ID_75", "ID_76", "ID_83",
# "ID_84", "ID_85", "ID_55", "ID_56", "ID_65", "ID_66", "ID_75",
# "ID_76", "ID_77", "ID_56", "ID_57", "ID_66", "ID_67", "ID_76",
# "ID_77", "ID_67", "ID_68", "ID_77", "ID_67", "ID_68", "ID_67",
# "ID_68", "ID_57", "ID_58", "ID_67", "ID_68", "ID_69", "ID_57",
# "ID_58", "ID_57", "ID_58", "ID_37", "ID_46", "ID_47", "ID_56",
# "ID_57", "ID_36", "ID_37", "ID_39", "ID_26", "ID_27", "ID_36",
# "ID_37", "ID_24", "ID_25", "ID_26", "ID_34", "ID_35", "ID_24",
# "ID_25", "ID_14", "ID_15", "ID_24", "ID_25", "ID_4", "ID_13",
# "ID_14", "ID_15", "ID_23", "ID_24", "ID_33", "ID_34", "ID_35",
# "ID_43", "ID_44", "ID_25", "ID_26", "ID_35", "ID_36", "ID_37",
# "ID_45", "ID_46", "ID_47", "ID_34", "ID_35", "ID_44", "ID_45",
# "ID_46", "ID_55", "ID_56", "ID_45", "ID_45", "ID_55", "ID_56",
# "ID_65", "ID_66", "ID_33", "ID_34", "ID_43", "ID_44", "ID_45",
# "ID_52", "ID_53", "ID_54", "ID_55", "ID_63", "ID_42", "ID_43",
# "ID_51", "ID_52", "ID_53", "ID_62")), .Names = c("name_state", "abbrev_state",
# "code_grid"), row.names = c(NA, -139L), class = "data.frame")
# 
# # # Use UTF-8 encoding in all character columns
# #   options(encoding = "UTF-8")
# #
# #   grid_state_correspondence_table <- grid_state_correspondence_table %>%
# #     mutate_if(is.character, function(x){
# #       x  %>% stringi::stri_encode(to="UTF-8") } )
# 
# 
# # sort data alphabetically
# grid_state_correspondence_table <- grid_state_correspondence_table[order(grid_state_correspondence_table$name_state),]
# 
# # save table
#   save(grid_state_correspondence_table, file = "./data/grid_state_correspondence_table.RData", compress = T)
#   #
# 
# 
