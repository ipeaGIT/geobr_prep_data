#> DATASET: Escolas — Catálogo de Escolas (INEP Oracle BI)
#> Source: INEP Oracle BI — https://anonymousdata.inep.gov.br/analytics/
#>   Dashboard: Catálogo de Escolas > Lista das Escolas > Exportar (CSV)
#> Metadata:
# Titulo: Escolas (fonte Oracle BI / Catálogo de Escolas)
# Titulo alternativo: schools_bi
# Frequencia de atualizacao: Anual (Censo Escolar)
# Forma de apresentacao: Points (com lat/lon oficiais do INEP)
# Linguagem: Pt-BR
# Character set: UTF-8 com BOM
#
# Resumo: Dados do Catálogo de Escolas do INEP exportados via Oracle BI.
# Contém lat/lon oficiais coletadas pelo INEP. Cobertura ~85% das escolas.
#
# INSTRUCOES DE COLETA MANUAL:
# 1. Acessar: https://anonymousdata.inep.gov.br/analytics/saw.dll?Dashboard
#    &PortalPath=/shared/Censo da Educação Básica/_portal/Catálogo de Escolas
# 2. Na página "Pré-Lista", clicar "Aplicar" (sem filtros = Brasil todo)
# 3. Na página "Lista das Escolas", clicar link "Exportar" (centro da página)
# 4. Salvar CSV em: data-raw/schools_bi/YYYYMM_schools_bi.csv
#    (ex: 202604_schools_bi.csv para abril/2026)
#
# ESTRATEGIA: Complementar ao R/schools.R (microdados + geocodebr).
#   schools.R     = microdados INEP + geocodebr (coordenadas estimadas via CNEFE)
#   schools_bi.R  = CSV do Oracle BI (coordenadas oficiais do INEP)
#
# Estado: Ativo
# Informacao do Sistema de Referencia: SIRGAS 2000 (EPSG:4674)

### Libraries (use any library as necessary) -----------------------------------

# library(sf)
# library(dplyr)
# library(readr)
# library(arrow)
# source("./R/support_harmonize_geobr.R")
# source("./R/support_fun.R")

# Directory where manually downloaded CSVs are stored
SCHOOLS_BI_RAW_DIR <- "./data-raw/schools_bi"


# Download the data (read from pre-downloaded CSV) ----------------------------
download_schools_bi <- function(csv_file) {

  if (!file.exists(csv_file)) {
    stop("CSV nao encontrado: ", csv_file, "\n",
         "Baixe manualmente do Oracle BI e salve em ", SCHOOLS_BI_RAW_DIR)
  }

  message("Lendo CSV do Oracle BI: ", basename(csv_file))
  raw <- readr::read_csv(csv_file, show_col_types = FALSE,
                         locale = readr::locale(encoding = "UTF-8"))
  message("Escolas lidas: ", nrow(raw), " registros")

  return(raw)
}


# Clean the data  --------------------------------------------------------------
clean_schools_bi <- function(schools_bi_raw, year) {

  dir_clean <- paste0("./data/schools_bi/", year)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  temp_df <- schools_bi_raw

  ## 1. Column mapping: Oracle BI CSV → geobr standard
  temp_df <- temp_df |>
    dplyr::transmute(
      code_school       = as.numeric(`Código INEP`),
      name_school       = Escola,
      education_level   = `Etapas e Modalidade de Ensino Oferecidas`,
      admin_category    = `Categoria Administrativa`,
      address           = Endereço,
      phone_number      = Telefone,
      government_level  = `Dependência Administrativa`,
      location_type     = Localização,
      situation         = `Restrição de Atendimento`,
      school_size       = `Porte da Escola`,
      name_muni         = Município,
      abbrev_state      = UF,
      lat               = Latitude,
      lon               = Longitude
    )

  ## 2. Add code_state from abbrev_state
  states_ref <- states_geobr()
  states_ref$code_state <- as.numeric(states_ref$code_state)
  state_lookup <- stats::setNames(states_ref$code_state, states_ref$abbrev_state)
  temp_df$code_state <- as.numeric(state_lookup[temp_df$abbrev_state])

  ## 3. Add name_state and region info
  region_lookup <- stats::setNames(states_ref$code_region, states_ref$abbrev_state)
  name_state_lookup <- stats::setNames(states_ref$name_state, states_ref$abbrev_state)
  name_region_lookup <- stats::setNames(states_ref$name_region, states_ref$abbrev_state)
  code_region_lookup <- stats::setNames(as.numeric(states_ref$code_region), states_ref$abbrev_state)

  temp_df$name_state   <- as.character(name_state_lookup[temp_df$abbrev_state])
  temp_df$code_region  <- as.numeric(code_region_lookup[temp_df$abbrev_state])
  temp_df$name_region  <- as.character(name_region_lookup[temp_df$abbrev_state])

  ## 4. Apply snake_case_names for Title Case formatting
  temp_df <- snake_case_names(temp_df, colname = c("name_school", "name_muni",
                                                    "name_state", "name_region"))

  ## 5. Add year
  temp_df$year <- as.numeric(year)

  ## 6. Create sf POINT geometry from lat/lon
  ##    INEP coordinates are WGS84 (4326), transform to SIRGAS 2000 (4674)
  valid_coords <- !is.na(temp_df$lat) & !is.na(temp_df$lon) &
                  temp_df$lat != 0 & temp_df$lon != 0

  n_valid <- sum(valid_coords)
  n_total <- nrow(temp_df)
  message(sprintf("Escolas com coordenadas: %d/%d (%.1f%%)",
                  n_valid, n_total, 100 * n_valid / n_total))

  ## Schools with coordinates
  df_good <- temp_df[valid_coords, ]
  sf_good <- sf::st_as_sf(df_good, coords = c("lon", "lat"),
                           crs = 4326, na.fail = FALSE)
  sf_good <- sf::st_transform(sf_good, 4674)

  ## Schools without coordinates → POINT EMPTY
  df_bad <- temp_df[!valid_coords, ]
  if (nrow(df_bad) > 0) {
    empty_geom <- sf::st_sfc(
      lapply(seq_len(nrow(df_bad)), function(i) sf::st_point()),
      crs = 4674
    )
    df_bad$lat <- NULL
    df_bad$lon <- NULL
    sf_bad <- sf::st_sf(df_bad, geometry = empty_geom)
    temp_sf <- dplyr::bind_rows(sf_good, sf_bad)
  } else {
    temp_sf <- sf_good
  }

  ## Remove lat/lon columns (now in geometry)
  temp_sf$lat <- NULL
  temp_sf$lon <- NULL

  ## 7. Reorder columns (geometry ALWAYS last)
  temp_sf <- temp_sf |>
    dplyr::select(
      code_school, name_school,
      education_level, admin_category,
      address, phone_number, government_level,
      location_type, situation, school_size,
      name_muni,
      code_state, abbrev_state, name_state,
      code_region, name_region,
      year, geometry
    )

  ## 8. Validation
  stopifnot(!is.na(sf::st_crs(temp_sf)))
  stopifnot(sf::st_crs(temp_sf)$epsg == 4674)
  stopifnot(is.numeric(temp_sf$code_school))
  stopifnot(is.numeric(temp_sf$code_state))
  stopifnot(is.character(temp_sf$abbrev_state))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")

  ## 9. Save
  sfarrow::st_write_parquet(temp_sf,
    dsn = paste0(dir_clean, "/schools_bi_", year, ".parquet"))

  files <- list.files(path = dir_clean, pattern = "\\.parquet$",
                      recursive = TRUE, full.names = TRUE)
  return(files)
}
