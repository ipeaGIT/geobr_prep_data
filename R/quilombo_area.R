#> DATASET: Quilombo Areas (Áreas Quilombolas)
#> Source: INCRA - https://certificacao.incra.gov.br/csv_shp/export_shp.py
#> Metadata:
# Titulo: Areas Quilombolas
# Titulo alternativo: Quilombo Areas
# Frequencia de atualizacao: Irregular (atualizado conforme novas certificacoes)
# Forma de apresentacao: Polygons
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Poligonos das areas quilombolas brasileiras certificadas pelo INCRA.
# Informacoes adicionais: Dados produzidos pelo INCRA (Instituto Nacional de
# Colonizacao e Reforma Agraria).
#
# Estado: Ativo
# Informacao do Sistema de Referencia: SIRGAS 2000

### Libraries (use any library as necessary) -----------------------------------

# library(sf)
# library(dplyr)
# library(httr)
# library(data.table)
# source("./R/support_harmonize_geobr.R")
# source("./R/support_fun.R")

# Download the data  -----------------------------------------------------------
# year <- format(Sys.Date(), "%Y%m")
download_quilombo <- function(year) {

  ftp <- "https://certificacao.incra.gov.br/csv_shp/zip/%C3%81reas%20de%20Quilombolas.zip"

  #  ftp <- "https://certificacao.incra.gov.br/csv_shp/zip/"
  #a <-   list_folders(ftp)
  # a

  dest_dir <- paste0(tempdir(), "/quilombo_area")
  dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)

  ## Download
  zip_file <- download_file_geobr(
    file_url = ftp, 
    dest_dir = dest_dir
  )
  
  
  ## Unzip
  out_dir <- paste0(dest_dir, "/unzipped")
  dir.create(out_dir, showWarnings = FALSE)
  utils::unzip(zip_file, exdir = out_dir)

  ## Read shapefile
  shp_file <- list.files(out_dir, pattern = "\\.shp$",
                         recursive = TRUE, full.names = TRUE)
  if (length(shp_file) == 0) stop("Shapefile nao encontrado no ZIP quilombolas")

  raw <- sf::st_read(shp_file[1], quiet = TRUE, stringsAsFactors = FALSE,
                     options = "ENCODING=WINDOWS-1252")
  
  raw$date_update <- year
  
  
  message("Quilombolas lidas: ", nrow(raw), " registros")
  return(raw)
}

# Clean the data  --------------------------------------------------------------
# quilombo_raw <- tar_read(quilombo_raw)
clean_quilombo <- function(quilombo_raw) {

  dir_clean <- "./data/quilombo_area"
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  date_update <- quilombo_raw$date_update[1]
  year_update <- lubridate::year(Sys.Date())
  
  ## 1. Rename columns
  temp_sf <- quilombo_raw |>
    dplyr::select(
      code_quilombo = cd_quilomb,
      code_sr       = cd_sr,
      n_process     = nr_process,
      name_quilombo = nm_comunid,
      name_muni     = nm_municip,
      abbrev_state  = cd_uf,
      date_recog    = dt_publica,
      date_decree_pr = dt_public1,
      date_decree   = dt_decreto,
      date_titulacao = dt_titulac,
      code_sipra    = cd_sipra,
      area_ha       = nr_area_ha,
      n_families    = nr_familia,
      geo_scale     = nr_escalao,
      perimeter     = perimetro_,
      gov_level     = esfera,
      stage         = fase,
      responsible_unit   = responsave,
      geometry
    )

  ## 2. Add code_state from abbrev_state
  states_ref <- states_geobr()
  state_lookup <- stats::setNames(states_ref$code_state, states_ref$abbrev_state)
  temp_sf$code_state <- as.numeric(state_lookup[temp_sf$abbrev_state])

  ## 3. Ensure numeric types
  temp_sf$code_quilombo <- as.numeric(temp_sf$code_quilombo)
  temp_sf$n_process     <- as.numeric(temp_sf$n_process)
  temp_sf$n_families      <- as.numeric(temp_sf$n_families)
  temp_sf$geo_scale       <- as.numeric(temp_sf$geo_scale)
  temp_sf$perimeter     <- as.numeric(temp_sf$perimeter)
  temp_sf$area_ha     <- as.numeric(temp_sf$area_ha)
  
  ## 4. Standardize code_sr format (SR-XX)
  temp_sf$code_sr <- ifelse(
    grepl("^SR", temp_sf$code_sr), temp_sf$code_sr,
    ifelse(grepl("^[0-9]", temp_sf$code_sr),
           paste0("SR-", temp_sf$code_sr), temp_sf$code_sr))

  ## 5. Standardize date columns to ISO format (YYYY-MM-DD)
  date_cols <- c("date_recog", "date_decree_pr", "date_decree", "date_titulacao")
  
  for (col in date_cols) {
    vals <- temp_sf[[col]]
    parsed <- dplyr::case_when(
      grepl("^\\d{4}-", vals) ~ as.Date(vals, format = "%Y-%m-%d"),
      grepl("/", vals)        ~ as.Date(vals, format = "%d/%m/%Y"),
      grepl("\\.", vals)      ~ as.Date(vals, format = "%d.%m.%Y"),
      TRUE                    ~ as.Date(NA)
    )
    temp_sf[[col]] <- as.character(parsed)
  }

  ## 6. Harmonize (projection, topology, multipolygon)
  temp_sf <- harmonize_geobr(
    temp_sf            = temp_sf,
    year               = year_update,
    add_state          = FALSE,
    add_region         = FALSE,
    add_snake_case     = TRUE,
    snake_colname      = c("name_quilombo", "name_muni"),
    projection_fix     = TRUE,
    encoding_utf8      = TRUE,
    topology_fix       = TRUE,
    remove_z_dimension = TRUE,
    use_multipolygon   = FALSE
  )
  temp_sf <- sf::st_cast(temp_sf, "MULTIPOLYGON")

  ## 7. Reorder columns (geometry ALWAYS last)
  
  temp_sf$year_update <-year_update
  temp_sf$date_update <- date_update 
    
  # temp_sf <- temp_sf |>
  #   dplyr::select(
  #     code_quilombo, name_quilombo,
  #     code_sr, n_process,
  #     name_muni, abbrev_state, code_state,
  #     date_recog, date_decree_pr, date_decree, date_titulacao,
  #     n_family, code_sipra, n_scale, perimeter,
  #     sphere, phase, responsible,
  #     date_update, year_update, geometry
  #   )

  # sort by key columns
  temp_sf <- temp_sf |> 
    dplyr::arrange(code_state, code_quilombo)
  
  ## 8. Validation
  stopifnot(!is.na(sf::st_crs(temp_sf)))
  stopifnot(sf::st_crs(temp_sf)$epsg == 4674)
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))

  ## 8. Simplify
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)

  ## 9. Save
  write_geobr_parquet(
    sf_obj = temp_sf,
    path = paste0(dir_clean, paste0("/quilombolalands_",date_update,".parquet"))
    )
  
  write_geobr_parquet(
    sf_obj = temp_sf_simplified,
    path = paste0(dir_clean, paste0("/quilombolalands_",date_update,"_simplified.parquet"))
  )

  files <- list.files(path = dir_clean, pattern = "\\.parquet$",
                      recursive = TRUE, full.names = TRUE)
  return(files)
}
