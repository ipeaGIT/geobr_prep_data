#> DATASET: Localidades (Localities)
#> Source: IBGE - Cadastro de Localidades Selecionadas (2010)
#>         IBGE - Localidades do Brasil (2022)
#> Metadata:
# Titulo: Localidades do Brasil
# Titulo alternativo: Localities of Brazil
# Frequencia de atualizacao: Censal
# Forma de apresentacao: Points
# Linguagem: Pt-BR
# Character set: WINDOWS-1252 (2010), UTF-8 (2022)
#
# Resumo: Localizacao pontual de todas as localidades brasileiras
#         (cidades, vilas, povoados, lugarejo, etc.)
# Informacoes adicionais: Dados produzidos pelo IBGE.
# Proposito: Identificacao da localizacao de localidades brasileiras.
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas: locality, localidade, village, city, town
# Informacao do Sistema de Referencia: SIRGAS 2000

### Libraries (use any library as necessary) -----------------------------------

# library(sf)
# library(dplyr)
# library(arrow)
# library(geoarrow)
# library(stringi)
# source("./R/support_harmonize_geobr.R")
# source("./R/support_fun.R")

# Download the data  -----------------------------------------------------------
download_locality <- function(year){

  ## 0. Set up the download links -----------------------------------------------

  if (year == 2010) {
    # 2010: individual shp files (not zipped)
    base_url <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/localidades/cadastro_de_localidades_selecionadas_2010/Shapefile_SHP/"
    shp_files <- c(
      "BR_Localidades_2010_v1.shp",
      "BR_Localidades_2010_v1.dbf",
      "BR_Localidades_2010_v1.prj",
      "BR_Localidades_2010_v1.shx"
    )
  } else if (year == 2022) {
    # 2022: zipped shapefile
    ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/localidades/Localidades_do_Brasil/2022/Localidades_Brasil_shp.zip"
  } else {
    stop(paste("Ano", year, "nao suportado para localidades"))
  }

  ## 1. Create temp directories -------------------------------------------------

  zip_dir <- paste0(tempdir(), "/localidades/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)

  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)

  ## 2. Download data -----------------------------------------------------------

  if (year == 2010) {
    # Download individual files (not zipped)
    for (f in shp_files) {
      dest <- paste0(out_zip, f)
      download.file(
        url = paste0(base_url, f),
        destfile = dest,
        mode = "wb",
        quiet = FALSE
      )
    }
  }

  if (year == 2022) {
    in_zip <- paste0(zip_dir, "/zipped/")
    dir.create(in_zip, showWarnings = FALSE, recursive = TRUE)

    file_raw <- paste0(in_zip, "localidades_2022.zip")
    download.file(
      url = ftp_link,
      destfile = file_raw,
      mode = "wb",
      quiet = FALSE
    )

    utils::unzip(file_raw, exdir = out_zip, junkpaths = TRUE)
  }

  ## 3. Read shapefile ----------------------------------------------------------

  encode <- if (year == 2010) "WINDOWS-1252" else "UTF-8"

  # Find the .shp file
  shp_path <- list.files(out_zip, pattern = "\\.shp$",
                         full.names = TRUE, recursive = TRUE)

  if (length(shp_path) == 0) {
    stop("Nenhum arquivo .shp encontrado em ", out_zip)
  }

  raw <- sf::st_read(
    shp_path[1],
    quiet = TRUE,
    stringsAsFactors = FALSE,
    options = paste0("ENCODING=", encode)
  )

  raw$year <- year
  return(raw)
}

# Clean the data ---------------------------------------------------------------
clean_locality <- function(raw){

  ## 0. Create clean directory --------------------------------------------------
  yyyy <- raw$year[1]
  dir_clean <- paste0("./data/localidades/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  ## 1. Inspect and standardize column names ------------------------------------

  # Lowercase all column names for consistent handling
  names(raw) <- tolower(names(raw))

  ## 2. NO filter — keep ALL localities (cidades, vilas, povoados, etc.) --------

  temp_sf <- raw

  ## 3. Extract locality type column --------------------------------------------

  # nm_categor contains the locality type (CIDADE, VILA, POVOADO, etc.)
  if ("nm_categor" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, type_locality = nm_categor)
  } else if ("nm_categ" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, type_locality = nm_categ)
  } else {
    warning("Coluna de categoria de localidade nao encontrada em ", yyyy)
    temp_sf$type_locality <- NA_character_
  }

  ## 4. Rename columns to geobr standard ----------------------------------------

  # Map code_muni column
  if ("cd_geocodm" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, code_muni = cd_geocodm)
  } else if ("geocodigo" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, code_muni = geocodigo)
  } else if ("cd_mun" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, code_muni = cd_mun)
  } else if ("cd_geocmu" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, code_muni = cd_geocmu)
  }

  # Map name_muni column
  if ("nm_mun" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, name_muni = nm_mun)
  } else if ("nm_municip" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, name_muni = nm_municip)
  } else if ("nm_mun_2022" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, name_muni = nm_mun_2022)
  } else if ("nome" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, name_muni = nome)
  }

  # Map locality name column
  if ("nm_localid" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, name_locality = nm_localid)
  } else if ("nm_local" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, name_locality = nm_local)
  } else if ("nm_localidade" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, name_locality = nm_localidade)
  }


  ## 6. Derive code_state from code_muni ----------------------------------------

  temp_sf$code_state <- substr(temp_sf$code_muni, 1, 2)

  ## 7. Select only needed columns before harmonize -----------------------------

  # Keep type_locality and name_locality as extra columns
  cols_to_keep <- c("code_muni", "name_muni", "code_state")

  if ("name_locality" %in% names(temp_sf)) {
    cols_to_keep <- c(cols_to_keep, "name_locality")
  }

  cols_to_keep <- c(cols_to_keep, "type_locality", "geometry")

  temp_sf <- temp_sf |>
    dplyr::select(dplyr::all_of(cols_to_keep))

  ## 8. Apply harmonize_geobr ---------------------------------------------------
  # Points: no topology fix, no multipolygon

  temp_sf <- harmonize_geobr(
    temp_sf        = temp_sf,
    year           = yyyy,
    add_state      = TRUE,
    state_column   = "code_state",
    add_region     = TRUE,
    region_column  = "code_state",
    add_snake_case = TRUE,
    snake_colname  = c("name_muni", "name_locality"),
    projection_fix = TRUE,
    encoding_utf8  = TRUE,
    topology_fix   = FALSE,
    remove_z_dimension = TRUE,
    use_multipolygon   = FALSE
  )

  ## 9. Reorder columns (geometry always last) ----------------------------------

  cols_final <- c("code_muni", "name_muni")

  if ("name_locality" %in% names(temp_sf)) {
    cols_final <- c(cols_final, "name_locality")
  }

  cols_final <- c(cols_final, "type_locality",
                  "code_state", "abbrev_state", "name_state",
                  "code_region", "name_region",
                  "year", "geometry")

  temp_sf <- temp_sf |>
    dplyr::select(dplyr::all_of(cols_final))

  ## 10. Validate ---------------------------------------------------------------

  stopifnot(is.numeric(temp_sf$code_muni))
  stopifnot(is.numeric(temp_sf$code_state))
  stopifnot(is.character(temp_sf$name_muni))
  stopifnot(is.character(temp_sf$type_locality))
  stopifnot(is.character(temp_sf$abbrev_state))
  stopifnot(is.character(temp_sf$name_state))
  stopifnot(is.numeric(temp_sf$code_region))
  stopifnot(is.character(temp_sf$name_region))
  stopifnot(is.numeric(temp_sf$year))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")
  stopifnot(!is.na(sf::st_crs(temp_sf)))
  stopifnot(!any(is.na(temp_sf$code_state)))

  ## 11. Save parquet (no simplified version for points) ------------------------

  write_geobr_parquet(
    temp_sf,
    paste0(dir_clean, "/localidades_", yyyy, ".parquet"))

  ## 12. Return file paths ------------------------------------------------------

  files <- list.files(
    path = dir_clean,
    pattern = ".parquet",
    recursive = TRUE,
    full.names = TRUE
  )

  return(files)
}
