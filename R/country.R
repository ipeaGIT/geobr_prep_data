#> DATASET: country 2000 a 2024
#> Source: IBGE - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html?=&t=o-que-e
#> scale 1:250.000
#> Metadata: #####
# Título: País
# Título alternativo: country
# Frequência de atualização: ?????
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Polígono das fronteiras do Brasil.
# Informações adicionais: Dados de estados produzidos pelo IBGE, e utilizados na elaboração do shape das regiões com a melhor base oficial disponível.
# Propósito: Disponibilização das fronteiras regionais do Brasil.
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas: ****
# Informação do Sistema de Referência: SIRGAS 2000
#


# Clean the data  --------------------------------------------------------------
# regions_files <- tar_read(regions_clean)
clean_country <- function(regions_files){
  
  dir_root <- paste0("./data/country/")
  dir.create(dir_root, recursive = T, showWarnings = FALSE)
  
  # drop simplified geometries
  regions_files <- regions_files[!grepl("simplified", regions_files)]
  
  # create function that creates regions by merging states and saves files
  get_country_from_states <- function(filepath){ # filepath <- regions_files[1]
    
    temp <- arrow::open_dataset(filepath) |> 
      sf::st_as_sf()
    
    ## 0. Create folder to save clean data
    yyyy <- temp$year[1]
    dir_clean <- paste0("./data/country/", yyyy)
    dir.create(dir_clean, recursive = T, showWarnings = FALSE)
    message(yyyy)
    
    # dissolve polygons
    temp_sf <- dissolve_polygons_no_split(
      mysf = temp,
      group_column = "year"
    )  
    
    temp_sf <- temp_sf |> 
      dplyr::select(year) |> 
      unique()
    
    # head(temp_sf)
    # plot(temp_sf)
    
    ## 5. lighter version --------------------------------------------------------
    temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
    
    ## 6. Save datasets  ---------------------------------------------------------
    
    write_geobr_parquet(
      temp_sf,
      paste0(dir_clean, "/country_", yyyy, ".parquet"))
    
    write_geobr_parquet(
      temp_sf_simplified,
      paste0(dir_clean,"/country_", yyyy, "_simplified", ".parquet"))
    
    return(yyyy)
  }
  
  # apply function to all years
  
  success_list <- lapply(
    X = regions_files, 
    FUN = function(x){get_country_from_states(x)}
  )
  
  if (isFALSE(length(success_list) == length(success_list))) {
    stop("error in country clean")
  }
  
  ## 7. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_root, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
  
}
