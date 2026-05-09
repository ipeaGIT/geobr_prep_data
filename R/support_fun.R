
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
download_file_geobr <- function(file_url,
                                dest_dir = NULL,
                                showProgress = TRUE,
                                timeout = 500,
                                max_active = 5) {
  
  if (is.null(dest_dir)) {
    dest_dir <- tempdir()
  }
  
  if (!dir.exists(dest_dir)) {
    stop("'dest_dir' does not exist")
  }
  
  if (!is.character(file_url) || length(file_url) == 0L) {
    stop("'file_url' must be a non-empty character vector")
  }
  
  dest_files <- file.path(dest_dir, basename(file_url))
  
  existing <- file.exists(dest_files)
  if (any(existing)) {
    unlink(dest_files[existing])
  }
  
  reqs <- lapply(file_url, function(url) {
    httr2::request(url) |>
      httr2::req_options(
        timeout = timeout,
        ssl_verifypeer = 0L
      )
  })
  
  if (length(reqs) == 1L) {
    httr2::req_perform(reqs[[1]], path = dest_files[[1]])
    return(dest_files)
  }
  
  resps <- httr2::req_perform_parallel(
    reqs,
    paths = dest_files,
    max_active = max_active,
    on_error = "continue"
  )
  
  ok <- vapply(
    resps,
    function(x) !inherits(x, "httr2_failure"),
    logical(1)
  )
  
  if (!all(ok)) {
    warning(
      "Some downloads failed:\n",
      paste(file_url[!ok], collapse = "\n")
    )
  }
  
  dest_files[ok]
}

# download_file_geobr <- function(file_url,
#                           dest_dir = NULL,
#                           showProgress = TRUE,
#                           timeout = 500) {
#   
#   if (is.null(dest_dir)) dest_dir <- tempdir()
#   
#   if (!dir.exists(dest_dir)) {
#     stop("'dest_dir' does not exist")
#   }
#   
#   dest_file <- file.path(dest_dir, basename(file_url))
#   
#   if (file.exists(dest_file)) unlink(dest_file)
#   
#   req <- httr2::request(file_url) |>
#     httr2::req_options(
#       timeout = timeout,
#       ssl_verifypeer = 0L
#     )
#   
#   if (isTRUE(showProgress)) {
#     req <- req |> httr2::req_progress()
#   }
#   
#   httr2::req_perform(req, path = dest_file)
#   
#   dest_file
# }


# Unzip geobr function ---------------------------------------------------------

unzip_geobr <- function(zip_dir, out_zip = NULL) {
  
  if (is.null(out_zip)) {
    out_zip <- file.path(zip_dir, "unzipped")
  }
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  
  zip_names <- list.files(zip_dir, pattern = "\\.zip$", full.names = TRUE)
  
  purrr::walk(zip_names, function(z) {
    ok <- tryCatch({
      unzip(z, exdir = out_zip)
      TRUE
    }, error = function(e) FALSE)
    
    if (!ok) {
      system2(
        "unzip",
        args = c("-oq", shQuote(z), "-d", shQuote(out_zip)),
        stdout = FALSE,
        stderr = FALSE
      )
    }
  })
  
  files <- list.files(path = out_zip, full.names = T, recursive = T)
  files <- unique(files)
  
  return(files)
}


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
  
  # Make any invalid geometry valid # sf::st_is_valid( sf)
  temp_gpkg_simplified <- fix_topology(temp_gpkg_simplified)
  
  return(temp_gpkg_simplified)
}





# detect UTM zone from legacy ESRI .prj file  ---------------------------
# Reads an ESRI-style .prj file from IBGE 2000 (which sf cannot parse into
# a proper CRS) and extracts the UTM zone number. Returns integer or NA.
detect_utm_zone_from_prj <- function(prj_file){

  if (!file.exists(prj_file)) return(NA_integer_)
  txt <- readLines(prj_file, warn = FALSE)
  txt <- paste(txt, collapse = " ")
  m <- regmatches(txt, regexpr("ZONE\\s+\\d+", txt, ignore.case = TRUE))
  if (length(m) == 0) return(NA_integer_)
  as.integer(sub("(?i)ZONE\\s+", "", m[1], perl = TRUE))
}


# code coulumns to numeric ---------------------------

code_cols_to_numeric <- function(df){
  df <- df |>
    dplyr::mutate(
      dplyr::across(
        dplyr::starts_with("code_"),
        # dplyr::any_of(c("code_muni", "code_state", "code_region",
        #               "code_meso", "code_micro", "code_immediate", 
        #               "code_intermediate", "code_quilombo",
        #               "code_tract", "code_weighting", "code_neighborhood", 
        #               "code_district", "code_subdistrict", 
        #               "code_favela",
        #               "code_school",
        #               "code_biome",
        #               "code_conservation_unit",
        #               "code_indigenous_land", "code_adm_unit",
        #               "code_health_region", "code_health_macroregion",
        #               "code_water_basin", "code_water_basin_macro"
        #               )
                      # ),
        as.numeric
      )
    )
  
  return(df)
}



# detect encoding of a csv file
detect_csv_encoding <- function(file_path){
  
  out <- list()
  
  out$readr <- readr::guess_encoding(file_path)
  
  temp <- stringi::stri_enc_detect(
    readLines(
    file_path, 
    warn = FALSE, 
    encoding = "bytes")
    )
  
 df_stringi <- lapply(
   X = 1:length(temp), 
   FUN = function(i, mylist = temp){
     mylist[[i]] |>
       dplyr::filter(Language=='pt')
           }
         ) |> 
   data.table::rbindlist() |> 
   dplyr::arrange(-Confidence) |> 
   unique() |> 
   head()
  
  
  out$stringi <- df_stringi
  return(out)
}

