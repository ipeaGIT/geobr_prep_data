
f <- list.files("./data/amazonia_legal/", full.names = T,recursive = T)

check_names <- function(file_path){
  
  message(basename(file_path))
  arrow::open_dataset(file_path) |> 
    names()
  
  # b <- arrow::open_dataset(file_path) |> 
  #   sf::st_as_sf()
}

lapply(X= f, FUN = check_names)
