#> DATASET: Arranjos Populacionais e Concentracoes Urbanas
#> Source: IBGE
# - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/divisao-regional/15782-arranjos-populacionais-e-concentracoes-urbanas-do-brasil.html
# - https://geoftp.ibge.gov.br/organizacao_do_territorio/divisao_regional/arranjos_populacionais
#> Scale: 1:25.000
#> Format: MDB (MS Access) with multiple spatial layers
#> CRS: SIRGAS 2000
#> Year: 2015 (static, based on Census 2010 data)
#> Encoding: UTF-8
#>
#> The ZIP contains a single MDB file: ArranjosPopulacionais_2ed.mdb
#> Key layers:
#>   - ComposicaoRecortes_01_PorMunicipio (5567 municipalities, 44 columns)
#>   - ArranjosPopulacionais_01 (294 dissolved arrangements)
#>   - ConcentracoesUrbanas_ConsideradasAnalise_01 (185 dissolved concentrations)
#>
#> For pop_arrangements: use ComposicaoRecortes_01_PorMunicipio, subset rows
#>   where CodArranjoPop is not NA (1243 municipalities)
#> For urban_concentrations: use ConcentracoesUrbanas_ConsideradasAnalise_01
#>   (185 pre-dissolved concentration polygons)


###############################################################################
# POP ARRANGEMENTS (#26) — Arranjos Populacionais
###############################################################################

# Download the data  -----------------------------------------------------------
download_poparrangements <- function(year){

  ## 0. URL -------------------------------------------------------------------
  if (year ==2010){
    file_url <- paste0(
      "https://geoftp.ibge.gov.br/organizacao_do_territorio/",
      "divisao_regional/arranjos_populacionais/base_de_dados_2ed/",
      "ArranjosPopulacionais_mbd_2ed.zip"
    )
  }

  ## 1. Create temp folders ---------------------------------------------------
  zip_dir <- paste0(tempdir(), "/pop_arrangements/")
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)

  out_zip <- paste0(zip_dir, "unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)

  ## 2. Download --------------------------------------------------------------
  zip_file <- paste0(zip_dir, "ArranjosPopulacionais.zip")
  httr::GET(url = file_url, httr::progress(),
            httr::write_disk(zip_file, overwrite = TRUE),
            httr::timeout(300))
  
  if (!file.exists(zip_file) || file.size(zip_file) < 1000) {
    stop("Download falhou para pop_arrangements. URL: ", file_url)
  }

  ## 3. Unzip -----------------------------------------------------------------
  utils::unzip(zip_file, exdir = out_zip)

  ## 4. Find the MDB file -----------------------------------------------------
  mdb_file <- list.files(out_zip, pattern = "\\.mdb$",
                         full.names = TRUE, recursive = TRUE)

  if (length(mdb_file) == 0) {
    stop("No .mdb file found in the unzipped directory")
  }

  # sf::st_layers(mdb_file)
  
  ## 5. Read the per-municipality layer (contains all codes) ------------------
  raw <- sf::st_read(
    mdb_file[1],
    layer = "ComposicaoRecortes_01_PorMunicipio",
    quiet = TRUE,
    stringsAsFactors = FALSE
  )

  raw$year <- year
  
  return(raw)
}

# Clean the data  --------------------------------------------------------------
clean_poparrangements <- function(poparrangements_raw){

  ## 0. Create clean directory ------------------------------------------------
  dir_clean <- "./data/pop_arrangements/"
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  year <- 2015

  ## 1. Rename columns to geobr standard -------------------------------------
  temp_sf <- poparrangements_raw |>
    dplyr::rename(
      code_muni                = CodMunic,
      name_muni                = NomMunic,
      pop_total_2010           = PopTot2010,
      pop_urban_2010           = Pess2010Urbano,
      pop_rural_2010           = Pess2010Rural,
      code_pop_arrangement     = CodArranjoPop,
      name_pop_arrangement     = NomeArranjoPop
    )

  ## 2. Replace empty strings and MDB null markers with NA -------------------
  temp_sf <- temp_sf |>
    dplyr::mutate(
      code_pop_arrangement = dplyr::case_when(
        code_pop_arrangement %in% c("", "<Null>") ~ NA_character_,
        TRUE ~ code_pop_arrangement
      ),
      name_pop_arrangement = dplyr::case_when(
        name_pop_arrangement %in% c("", "<Null>") ~ NA_character_,
        TRUE ~ name_pop_arrangement
      )
    )

  ## 3. Subset: only municipalities belonging to a pop arrangement -----------
  temp_sf <- temp_sf |>
    dplyr::filter(!is.na(code_pop_arrangement))

  ## 4. Derive code_state from code_muni -------------------------------------
  temp_sf$code_muni <- as.numeric(temp_sf$code_muni)
  temp_sf$code_state <- substr(as.character(temp_sf$code_muni), 1, 2) |>
    as.numeric()

  ## 5. Convert pop columns to numeric ---------------------------------------
  temp_sf$pop_total_2010  <- as.numeric(temp_sf$pop_total_2010)
  temp_sf$pop_urban_2010  <- as.numeric(temp_sf$pop_urban_2010)
  temp_sf$pop_rural_2010  <- as.numeric(temp_sf$pop_rural_2010)
  temp_sf$code_pop_arrangement <- as.numeric(temp_sf$code_pop_arrangement)

  ## 6. Harmonize (state info, projection, topology, etc.) -------------------
  temp_sf <- harmonize_geobr(
    temp_sf            = temp_sf,
    year               = year,
    add_state          = TRUE,
    state_column       = "code_state",
    add_region         = TRUE,
    region_column      = "code_state",
    add_snake_case     = TRUE,
    snake_colname      = c("name_muni", "name_pop_arrangement"),
    projection_fix     = TRUE,
    encoding_utf8      = TRUE,
    topology_fix       = TRUE,
    remove_z_dimension = TRUE,
    use_multipolygon   = FALSE
  )

  ## 7. Post-harmonize: ungroup, ensure sf, set CRS if missing ---------------
  temp_sf <- dplyr::ungroup(temp_sf)
  if (!inherits(temp_sf, "sf")) {
    temp_sf <- sf::st_as_sf(temp_sf)
  }
  if (is.na(sf::st_crs(temp_sf))) {
    sf::st_crs(temp_sf) <- 4674
  }

  ## 8. Cast to MULTIPOLYGON -------------------------------------------------
  temp_sf <- sf::st_cast(temp_sf, "MULTIPOLYGON")

  ## 9. Select and reorder columns -------------------------------------------
  temp_sf <- temp_sf |>
    dplyr::select(
      code_pop_arrangement, name_pop_arrangement,
      code_muni, name_muni,
      pop_total_2010, pop_urban_2010, pop_rural_2010,
      code_state, abbrev_state, name_state,
      code_region, name_region,
      year, geometry
    )

  ## 10. Validate -------------------------------------------------------------
  stopifnot(sf::st_crs(temp_sf)$epsg == 4674)
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")
  stopifnot(is.numeric(temp_sf$code_state))
  stopifnot(is.numeric(temp_sf$code_muni))
  stopifnot(is.numeric(temp_sf$code_pop_arrangement))
  stopifnot(is.character(temp_sf$abbrev_state))

  ## 11. Simplified version ---------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)

  ## 12. Save parquet ---------------------------------------------------------
  write_geobr_parquet(
    temp_sf,
    paste0(dir_clean, "/poparrangements_", yyyy, ".parquet")
    
    )

  write_geobr_parquet(
    temp_sf_simplified,
    paste0(dir_clean,"/poparrangements_", yyyy, "_simplified", ".parquet")
    )

  ## 13. Return file paths ----------------------------------------------------
  files <- list.files(path = dir_clean, pattern = "\\.parquet$",
                      recursive = TRUE, full.names = TRUE)
  return(files)
}


