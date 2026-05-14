#> DATASET: regions 2000 a 2024
#> Source: IBGE - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html?=&t=o-que-e
#> scale 1:250.000
#> Metadata: #####
# Título: Regiões
# Título alternativo: regions
# Frequência de atualização: ?????
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Polígonos das regiões brasileiras.
# Informações adicionais: Dados de estados produzidos pelo IBGE, e utilizados na 
# elaboração do shape das regiões com a melhor base oficial disponível.
# Propósito: Disponibilização das fronteiras regionais do Brasil.
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas: ****
# Informação do Sistema de Referência: SIRGAS 2000
#
# Observações: 
# Anos disponíveis: 2000 a 2024


# unify all clean files of state  --------------------------------------------------------------
# state_files <- tar_read(states_clean)
# hist_state_files <- tar_read(hist_states_clean)
get_all_states_clean <- function(state_files, hist_state_files){
  
  # single vector with all file paths
  all_files <- c(state_files, hist_state_files)
  all_files <- unique(all_files)
  
  # drop simplified geometries
  all_state_files <- all_files[!grepl("simplified", all_files)] |> sort()
  
  return(all_state_files)
}

# Clean the data  --------------------------------------------------------------

# all_state_files <- tar_read("all_states_clean")

clean_regions <- function(all_state_files){
  
  dir_root <- paste0("./data/regions/")
  dir.create(dir_root, recursive = T, showWarnings = FALSE)
  

  # create function that creates regions by merging states and saves files
  get_regions_from_states <- function(filepath){ # filepath <- all_state_files[1]

    # Anos historicos (1940-1991) podem ter code_state/code_region como bool no
    # parquet (efeito do base::ifelse com todas as linhas matching a condicao em
    # clean_hist_states). Coerce para numeric ANTES do filter pra evitar
    # arrow::write_parquet falhar com 'NotImplemented: MakeBuilder for
    # geoarrow.wkb' apos o dissolve. Drop litigio que e bool e nao usado aqui.
    temp <- read_geoparquet(filepath)
    
    temp <- temp |> 
      dplyr::mutate(
        code_region = as.numeric(code_region),
        code_state  = as.numeric(code_state)
      ) |>
      dplyr::select(-dplyr::any_of("litigio")) |>
      dplyr::filter(!is.na(code_region), code_region >= 1)

    ## 0. Create folder to save clean data
    yyyy <- temp$year[1]
    dir_clean <- paste0("./data/regions/", yyyy)
    dir.create(dir_clean, recursive = T, showWarnings = FALSE)
    message(yyyy)

    # dissolve polygons
    all_regions <- dissolve_polygons_no_split(
      mysf = temp,
      group_column = "code_region"
    )
    
    # add name_region 
    all_regions <- all_regions |> 
      dplyr::mutate(
        year = yyyy,
        name_region = case_when(
        code_region==1 ~"Norte",
        code_region==2 ~"Nordeste",
        code_region==3 ~"Sudeste",
        code_region==4 ~"Sul",
        code_region==5 ~"Centro-Oeste",
        .default = NA
      )) |> 
      dplyr::filter(!is.na(name_region))

      # reorder columns
      all_regions <- all_regions |>
      dplyr::select(
        code_region,
        name_region,
        year,
        geometry
      ) |> unique()

      # head(all_regions)
      # plot(all_regions)
      
      # sort by key columns
      temp_sf <- all_regions |> 
        dplyr::arrange(code_region)
      
      
  ## 5. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 6. Save datasets  ---------------------------------------------------------
  
  write_geobr_parquet(
    sf_obj = temp_sf,
    path = paste0(dir_clean, "/regions_", yyyy, ".parquet"))

  write_geobr_parquet(
    sf_obj = temp_sf_simplified,
    path = paste0(dir_clean,"/regions_", yyyy, "_simplified", ".parquet"))
  
  return(yyyy)
  }
  
  # apply function to all years
  success_list <- lapply(
    X = all_state_files, 
    FUN = function(x){get_regions_from_states(x)}
      )
  
  if (isFALSE(length(success_list) == length(success_list))) {
    stop("error in Regions clean")
    }
  
  ## 7. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_root, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}

 
###### OLD CODE BELOW ####################
# 
# ###### 0. Create Root folder to save the data
# root_dir <- "L:\\# DIRUR #\\ASMEQ\\geobr\\data-raw"
# setwd(root_dir)
# dir.create("./regions")
# 
# 
# #### This function loads Brazilian stantes for an specified year {geobr::read_states} and
# #### and generates the sf boundaries of region
# prep_region <- function(year){
# 
#   y <- year
# 
#   # create year folder to save clean data
#   destdir <- paste0("./regions/",y)
#   dir.create(destdir)
# 
#   # a) reads all states sf files and pile them up
#   sf_states <- geobr::read_state(code_state = "all", year = y, simplified = F)
# 
#   # remove wrong-coded regions
#   sf_states <- subset(sf_states, code_region %in% c(1:5))
# 
# 
# # store original crs
#   original_crs <- st_crs(sf_states)
# 
#   # b) make sure we have valid geometries
#   temp_sf <- sf::st_make_valid(sf_states)
#   temp_sf <- temp_sf %>% st_buffer(0)
# 
#   sf_states1 <- to_multipolygon(temp_sf)
# 
# 
# ## Dissolve each region
# all_regions <- dissolve_polygons(mysf=temp_sf, group_column='code_region')
# 
# 
# ### add region names
# all_regions <- add_region_info(temp_sf = all_regions, column = 'code_region')
# all_regions <- select(all_regions, c('code_region', 'name_region', 'geometry'))
# 
# 
# 
# 
# ###### 7. generate a lighter version of the dataset with simplified borders -----------------
#   # skip this step if the dataset is made of points, regular spatial grids or rater data
# 
#   # simplify
#   temp_sf7 <- simplify_temp_sf(all_regions)
# 
# ###### convert to MULTIPOLYGON
# all_regions <- to_multipolygon(all_regions)
# temp_sf7 <- to_multipolygon(temp_sf7)
# 
#   # Save cleaned sf in the cleaned directory
#   sf::st_write(all_regions, dsn= paste0(destdir,"/regions_",y,".gpkg"))
#   sf::st_write(temp_sf7, dsn= paste0(destdir,"/regions_",y,"_simplified", ".gpkg"))
# 
# }
# 
# 
# 
# # Aplica para diferentes anos
# my_years <- c(2000, 2001, 2010, 2013, 2014, 2015, 2016, 2017, 2018)
# 
# prep_region(2020)
# 
# # Parallel processing using future.apply
# future::plan(future::multiprocess)
# future.apply::future_lapply(X =my_years, FUN=prep_region, future.packages=c('readr', 'sp', 'sf', 'dplyr', 'geobr'))
# 
