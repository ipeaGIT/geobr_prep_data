#> DATASET: municipality 2000 a 2024
#> Source: IBGE - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html?=&t=o-que-e
#> scale 1:250.000
#> Metadata: #####
# Título: Municípios
# Título alternativo: municipality
# Frequência de atualização: ?????
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Poligonos dos municípios brasileiros.
# Informações adicionais: Dados produzidos pelo IBGE, e utilizados na elaboração do shape dos municípios com a melhor base oficial disponível.
# Propósito: Disponibilização das fronteiras municipais do Brasil.
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas: ****
# Informação do Sistema de Referência: SIRGAS 2000
#
# Observações: 
# Anos disponíveis: 

# Download the data  -----------------------------------------------------------

# year <- tar_read(years_municipality, branches = 1)[1]

download_municipality <- function(year){ # year = 2010
  
  ## 0. Generate the correct ftp link (UPDATE YEAR HERE) -----------------------
  
  url_start <- paste0("https://geoftp.ibge.gov.br/organizacao_do_territorio/",
                      "malhas_territoriais/malhas_municipais/municipio_")
  
  ### create states tibble
  states <- states_geobr()
  
  ### Year with parts of URL splitted in states --------------------------------
  if(year %in% c(2000, 2001, 2010:2014)) {
    
    #2000 ou 2010
    if(year %in% c(2000, 2010)) {
      ftp_link <- paste0(url_start, year, "/", states$abbrevm_state, "/",
                         states$abbrevm_state, "_municipios.zip")
    }
    
    #2001
    if(year == 2001) {
      ftp_link <- paste0(url_start, year, "/", states$abbrevm_state, "/",
                         states$code_state, "mu2500g.zip")
    }
    
    #2013 e 2014
    if(year %in% c(2013:2014)) {
      ftp_link <- paste0(url_start, year, "/", states$abbrev_state, "/",
                         states$abbrevm_state, "_municipios.zip")
    }
    
    filenames <- basename(ftp_link)
    
    names(ftp_link) <- filenames
  }
  
  ### Years with br URL --------------------------------------------------------
  #2005 
  if(year == 2005) {
    options(timeout = 600)
    
    ftp_link <- paste0(url_start, year, "/escala_2500mil/proj_geografica/arcview_shp/brasil/55mu2500gc.zip")
    }
  
  # 2007
  if(year == 2007) {
    ftp_link <- paste0(url_start, year, "//escala_2500mil/proj_geografica_sirgas2000/brasil/55mu2500gsr.zip")
  }
  
  # 2015 until 2018
  if(year %in% c(2015:2018)) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR/br_municipios.zip")
  }
  
  # 2019
  if(year %in% c(2019)) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR/br_municipios_20200807.zip")
  }
  
  # 2020 until 2022
  if(year %in% c(2020:2022)) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR/BR_Municipios_", year, ".zip")
  }
  
  # After 2023
  if(year >= 2023) {
    ftp_link <- paste0(url_start, year, "/Brasil/BR_Municipios_", year, ".zip")
  }
  
  ## 1. Create temp folder -----------------------------------------------------
  
  zip_dir <- paste0(tempdir(), "/municipality/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  

  ## 2. Create direction for each download -------------------------------------
  
  file_raw <- fs::file_temp(tmp_dir = zip_dir,
                            ext = fs::path_ext(ftp_link))
  
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  ## 3. Download Raw data ------------------------------------------------------
  
    file_raw <- download_file_geobr(
      file_url = ftp_link,
      dest_dir = zip_dir
    )
  
  
  ## 4. Unzip Raw data ---------------------------------------------------------
  
  files <- unzip_geobr(zip_dir = zip_dir,
              out_zip = out_zip
              )
  
  ## 5. Set corret encoding ----------------------------------------------------
  
  if (year == 2000) { #years without number of collumns errors
    encode <- "ENCODING=IBM437"
  }
  
  if (year %in% c(2001, 2005, 2007, 2010)) {
    encode <- "ENCODING=WINDOWS-1252"
  }
  
  if (year >= 2013) {
    encode =  "ENCODING=UTF8"
  }
  
  ## 6. Bind Raw data together -------------------------------------------------

  municipality_raw <- readmerge_geobr(
    folder_path = out_zip,
    encoding = encode
    )
  
  ## 7. Show result ------------------------------------------------------------

  municipality_raw <- municipality_raw |>
    janitor::clean_names()
  
  municipality_raw$year <- year
  
  return(municipality_raw)
}

# Clean the data  --------------------------------------------------------------
# municipality_raw <- tar_read(municipality_raw, branches = 5) # 2010
# municipality_raw <- tar_read(municipality_raw, branches = 2) # 2001
# head(municipality_raw)
clean_municipality <- function(municipality_raw){
  
  ## 0. Create folder to save clean data ---------------------------------------
  yyyy <- municipality_raw$year[1]
  dir_clean <- paste0("./data/municipality/", yyyy)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 2. standardize colnames  ---------------------------------------------------
  municipalities <- rename_cols_geobr(municipality_raw, dicionario_municipality) |> 
    dplyr::select(code_muni, name_muni, year) |> 
    mutate(code_state = substr(code_muni, 1, 2)) |> 
    filter(code_muni != 0)
  
  # dissolve borders to clean geometry and remove repetition
  # we don't use the dissolve_polygons_no_split() function here because it removes holes
  # and some municipalities do have holes that must be kept
  col_names <- names(municipalities)
  col_names <- col_names[!grepl("geometry", col_names)]
  
  municipalities <- harmonize_projection(municipalities)
  
  municipalities <- duckspatial::ddbs_union_agg(
    x =  municipalities,
    by = col_names
  ) |>
    duckspatial::ddbs_collect()

  
  ## 3. Apply harmonize geobr cleaning -----------------------------------------

  temp_sf <- harmonize_geobr(
    temp_sf = municipalities,
    year = yyyy,
    add_state = T,
    state_column = "code_state",
    add_region = T,
    region_column = "code_state",
    add_snake_case = T,
    snake_colname = "name_muni",
    projection_fix = T,
    encoding_utf8 = T,
    topology_fix = T,
    remove_z_dimension = T,
    use_multipolygon = T
  )

  ## 3c. Enforce column order (geometry always last)
  temp_sf <- temp_sf |>
    dplyr::select(code_muni, name_muni,
                  code_state, abbrev_state, name_state,
                  code_region, name_region,
                  year, geometry)


  # sort by key columns
  temp_sf <- temp_sf |> 
    dplyr::arrange(code_state, code_muni)
  
  
  ## 3d. Validate before saving
  stopifnot(is.numeric(temp_sf$code_muni))
  stopifnot(is.numeric(temp_sf$code_state))
  stopifnot(is.numeric(temp_sf$code_region))
  stopifnot(is.character(temp_sf$abbrev_state))
  stopifnot(all(nchar(temp_sf$abbrev_state) == 2))
  stopifnot(!is.na(sf::st_crs(temp_sf)))
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")

  
  
  ## 4. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 6. Save datasets  ---------------------------------------------------------
  
  ### Save in parquet
  write_geobr_parquet(
    sf_obj = temp_sf,
    path = paste0(dir_clean, "/municipalities_", yyyy, ".parquet")
    )

  write_geobr_parquet(
    sf_obj = temp_sf_simplified,
    path = paste0(dir_clean,"/municipalities_", yyyy, "_simplified", ".parquet")
    )
  
  ## 7. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean,
                      pattern = ".parquet",
                      recursive = TRUE,
                      full.names = TRUE)
  
  return(files)
}
