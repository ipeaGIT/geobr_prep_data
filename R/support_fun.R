
###### detect year from file name  -----------------
detect_year_from_string <- function(string){
  
  yyyy <- regmatches(string, gregexpr("\\d{4}", string))[[1]]
  yyyy <- unique(yyyy)
  yyyy <- yyyy[ yyyy != '2500' ]
  return(yyyy)
}


###### list ftp folders -----------------

# function to list ftp folders from their original sub-dir
list_folders <- function(ftp){

  h <- rvest::read_html(ftp)
  elements <- rvest::html_elements(h, "a")
  folders <- rvest::html_attr(elements, "href")
  return(folders)

}



###### Download file to tempdir -----------------

download_file <- function(file_url,
                          dest_dir = NULL,
                          showProgress = TRUE){

  if(is.null(dest_dir)) { dest_dir <- tempdir()}

  if (!(dir.exists(dest_dir))) {stop("'dest_dir' does not exist")}

  # file_ext <- tools::file_ext(file_url)
  # dest_file <- tempfile(fileext = paste0('.', file_ext))
  dest_file <- paste0(dest_dir,'/', basename(file_url))

  # download data
    httr::GET(url=file_url,
              if(showProgress==T){ httr::progress()},
              httr::write_disk(dest_file, overwrite = T),
              config = httr::config(ssl_verifypeer = FALSE)
    )

    return(dest_file)
}

# ###### Unzip data -----------------
#
# # function to Unzip files in their original sub-dir
# # unzip_fun <- function(f, head_dir){
# #   unzip(f, exdir = file.path(head_dir, substr(f, 3, 6)))
# # }
# unzip_fun <- function(f){
#   # f <- files_1st_batch[1]
#   t<-strsplit(f, "/")
#   t<-t[[1]][length(t[[1]])]
#   t<- nchar(t)
#   unzip(f, exdir = file.path(head_dir, substr(f, 3, nchar(f)-t) ))
# }




#####fixing municipality repetition---------

# https://github.com/ipeaGIT/geobr/blob/49534a6b19dc765e43e4c2f4404342f4fd0fdb4e/r-package/prep_data/prep_state_muni_regions.R#L987








# remove state repetition ----------------------
remove_state_repetition <- function(temp_sf){
  
  # know cases: Maranhao in 2000 and ES in 2001
  if (nrow(temp_sf)>27 | any(year==2001 & temp_sf$abbrev_state=='ES')   ) {
    
    # get colnames and summarize
    vars <- names(temp_sf)[-length(names(temp_sf))]
    temp_sf <- temp_sf |> group_by_at(vars) |> summarise()
    temp_sf <- temp_sf |> filter(!code_state=="0")
    temp_sf <- unique(temp_sf)
    return(temp_sf)
    
  } else { return(temp_sf) }
}

# 1. read raw zipped file in temporary dir  ---------------------------------
#' input: tempfile of raw data, temp dir of raw data, dest dir to save raw data
#' unzip and read raw data
#' output: save raw data in .rds format in the data_raw dir
muni_saveraw <- function(tempf, temp_dir, dest_dir) {
  
  ## 1.1 Unzip original data
  utils::unzip(zipfile = tempf, exdir = temp_dir, overwrite = TRUE, unzip = "unzip")
  
  ## 1.2. read shape file
  
  # List shape file
  shape_file <- list.files(path = temp_dir, full.names = T, recursive = T, pattern = ".shp$")
  shape_file <- shape_file <- shape_file[ data.table::like(shape_file, 'MU|mu2500|Municipios|municipios') ]
  
  # detect year from file name
  year <- detect_year_from_string(tempf)
  year <- year[year != '2500']
  year <- year[year != '0807']
  year <- year[year != '1701']
  year <- year[1]
  
  # Encoding for different years
  if (year %like% "2000") {
    temp_sf <- sf::st_read(shape_file, quiet = T, stringsAsFactors=F, options = "ENCODING=IBM437")
  }
  
  if (year %like% "2001|2005|2007|2010"){
    temp_sf <- sf::st_read(shape_file, quiet = T, stringsAsFactors=F, options = "ENCODING=WINDOWS-1252")
  }
  
  if (year >= 2013) {
    temp_sf <- sf::st_read(shape_file, quiet = T, stringsAsFactors=F, options = "ENCODING=UTF8")
  }
  
  
  ## 1.3. Save original data in compact .rds format
  
  # file name
  file_name <- gsub(".shp$", ".rds", basename(shape_file), ignore.case = T)
  
  # save in .rds
  saveRDS(temp_sf, file = paste0(dest_dir,"/", file_name), compress = TRUE)
  
  # # return path to raw file
  # muni_raw_path <- paste0(dest_dir,"/", file_name)
  # return(muni_raw_path)
}





###### Simplify temp_sf -----------------

simplify_temp_sf <- function(temp_sf, tolerance=100){
  
  # reproject to utm
  temp_gpkg_simplified <- sf::st_transform(temp_sf, crs=3857)
  
  # simplify with tolerance
  temp_gpkg_simplified <- sf::st_simplify(temp_gpkg_simplified,
                                          preserveTopology = T,
                                          dTolerance = tolerance)
  
  # reproject to utm
  temp_gpkg_simplified <- sf::st_transform(temp_gpkg_simplified, crs=4674)
  
  # Make any invalid geometry valid # st_is_valid( sf)
  temp_gpkg_simplified <- fix_topology(temp_gpkg_simplified)
  
  return(temp_gpkg_simplified)
}
