#> DATASET: indigenous Lands
#> Source: FUNAI - https://mapas2.funai.gov.br/portal_mapas/
#> Metadata:
# Titulo: Terras Indigenas
# Titulo alternativo: Terras Indigenas
# Data: Atualizacao mensal/anual
#
# Forma de apresentacao: Shape
# Linguagem: Pt-BR
# Character set: Utf-8
#
# Resumo: Poligonos das terras indigenas brasileiras.
# Informacoes adicionais: Dados produzidos pela FUNAI.
# Proposito: Identificacao das terras indigenas brasileiras.
#
# Estado: Completado
# Informacao do Sistema de Referencia: SIRGAS 2000


# Download the data  -----------------------------------------------------------
download_indigenousland <- function(year){ # year = 2024

  ## 0. Set up the download links (UPDATE YEAR) --------------------------------

  # base <- "https://mapas2.funai.gov.br/portal_mapas/"
  # a <- list_folders(base)
  
  encoding = "ENCODING=UTF-8"
  
  if (year == 2016) {
  date <- 20160728
  ftp_link <- "https://mapas2.funai.gov.br/portal_mapas/shapes_old/Backup/ti_sirgas_antigo4.zip"
  }
  
  if (year == 2017) {
  date <- 20170710
  ftp_link <- "https://mapas2.funai.gov.br/portal_mapas/shapes_old/Backup/ti_sirgas_antigo33.zip"
  }
  
  if (year == 2018) {
  encoding = "ENCODING=latin1"
  date <- 20181106
  ftp_link <- "https://mapas2.funai.gov.br/portal_mapas/shapes_old/Backup/ti_sirgas(2).zip"
  }

  if (year == 2019) { 
  date <- 20191025
  ftp_link <- "https://mapas2.funai.gov.br/portal_mapas/shapes_old/Backup/ti_sirgas_25_10_2019_2.zip"
  }

  if (year == 2020) {
  date <- 20200316
  ftp_link <- "https://mapas2.funai.gov.br/portal_mapas/shapes_old/Backup/ti_sirgas.zip(18).backup"
  }
  
  if (year == 2022) {
  date <- 20220223
  ftp_link <- "https://mapas2.funai.gov.br/portal_mapas/shapes_old/Backup/ti_sirgas(53).zip"
  }
  
  if (year == 2024) {
  date <- 20240823
  ftp_link <- "https://mapas2.funai.gov.br/portal_mapas/shapes_old/ti_sirgas_20240823.zip"  
  }
  
  if (year == 2025) {
  date <- 20250625
  ftp_link <- "https://mapas2.funai.gov.br/portal_mapas/shapes_old/ti_sirgas_20250625.zip"
  }
  
  
  ## 1. Create temp folders for download ---------------------------------------

  zip_dir <- paste0(tempdir(), "/indigenous_land/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)

  # unzipped folder
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)

  ## 2. Download Raw data ------------------------------------------------------

  file_raw <- download_file_geobr(
    file_url = ftp_link, 
    dest_dir = zip_dir
  )
  
  
  ## 3. Unzip Raw data ---------------------------------------------------------

  files <- unzip_geobr(zip_dir = dirname(file_raw), out_zip = out_zip)
  
  files_shp <- files[grep(".shp$", files)]
  if (length(files_shp)>1) {
    stop(paste("Mais de um arquivo shape file para terras indigenas no ano", year)) 
    }
  
  ## 4. Read shapefile ---------------------------------------------------------
  
  indigenousland_raw <- readmerge_geobr(
    folder_path = out_zip, 
    encoding = encoding
    )
  
  # nao sei pq precisa disso em 2022 
  if(year==2022) {
    indigenousland_raw <- sf::st_read(files_shp)
  }
    
  indigenousland_raw <- indigenousland_raw |> 
    janitor::clean_names()

  indigenousland_raw$year <- year
  indigenousland_raw$date <- date
  
  return(indigenousland_raw)
}

# Clean the data ---------------------------------------------------------------
# indigenousland_raw <- tar_read(indigenousland_raw, 7)
# head(indigenousland_raw)
clean_indigenousland <- function(indigenousland_raw){

  ## 0. Create folder to save clean data ---------------------------------------
  yyyy <- indigenousland_raw$year[1]
  dir_clean <- paste0("./data/indigenous_land/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  ## 1. Rename columns to geobr standard ---------------------------------------
  
  temp_sf <- rename_cols_geobr(indigenousland_raw, dicionario_indigenousland) |> 
    dplyr::select( - dplyr::any_of( "gid") )
  
  # complement state info
  temp_sf <- dplyr::left_join(
    x = temp_sf,
    y =  states_geobr(),
    by = "abbrev_state"
    )
  
  ## 4. Apply harmonize geobr cleaning -----------------------------------------

  temp_sf <- harmonize_geobr(
    temp_sf        = temp_sf,
    year           = yyyy,
    add_state      = TRUE,
    state_column   = "code_state",
    add_region     = TRUE,
    region_column  = "code_state",
    add_snake_case = TRUE,
    snake_colname  = c("name_indigenous_land", "name_ethnic_group",
                       "name_muni", "coordination_region",
                       "name_adm_unit"),
    projection_fix = TRUE,
    encoding_utf8  = TRUE,
    topology_fix   = TRUE,
    remove_z_dimension = TRUE,
    use_multipolygon   = TRUE
  )

  # improvements
  # use spatial join to determine code_muni and code_state, the ones with largest area overlap
  
  ## 5. Reorder columns (geometry ALWAYS last) ---------------------------------
  temp_sf <- temp_sf |>
    dplyr::select(
      dplyr::any_of(c(
      'code_indigenous_land',
      'name_indigenous_land',
      'name_ethnicity',
      'name_municipality',
      'code_state',
      'abbrev_state',
      'name_state',
      'code_region',
      'name_region',
      'area_ha',
      'fase_ti',
      'modalidade',
      'reestudo',
      'coordination_region',
      'faixa_fron',
      'code_adm_unit',
      'name_adm_unit',
      'abbrev_adm_unit',
      'dominio',
      'year',
      'date',
      'geometry')
    ))

  # sort by key columns
  temp_sf <- temp_sf |> 
    dplyr::arrange(code_state, code_indigenous_land)
  
  
  ## 6. Validate before saving -------------------------------------------------

  stopifnot(!is.na(sf::st_crs(temp_sf)))
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")
  stopifnot(is.numeric(temp_sf$code_indigenous_land))
  stopifnot(is.numeric(temp_sf$code_state))
  stopifnot(is.character(temp_sf$abbrev_state))
  stopifnot(is.character(temp_sf$name_state))

  ## 7. Lighter version --------------------------------------------------------

  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)

  ## 8. Save datasets ----------------------------------------------------------

  write_geobr_parquet(
    temp_sf,
    paste0(dir_clean, "/indigenouslands_", yyyy, ".parquet")
    )

  write_geobr_parquet(
    temp_sf_simplified,
    paste0(dir_clean, "/indigenouslands_", yyyy, "_simplified.parquet")
    )

  ## 9. Return file paths for targets ------------------------------------------

  files <- list.files(path = dir_clean,
                      pattern = ".parquet",
                      recursive = TRUE,
                      full.names = TRUE)

  return(files)
}
