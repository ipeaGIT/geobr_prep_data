
f <- list.files("./data/states/", full.names = T,recursive = T, pattern = ".parquet")

check_names <- function(file_path){
  
  # file_path = f[1]
  message(basename(file_path))
  arrow::open_dataset(file_path) |> 
    names()
  
  # b <- arrow::open_dataset(file_path) |>
  #   sf::st_as_sf()
}

lapply(X= f, FUN = check_names) 

x <- lapply(X = f, FUN = check_names) |> 
 dplyr::bind_rows()

table(x$)