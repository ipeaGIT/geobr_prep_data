#> DATASET: Municipal Seats (Sedes Municipais)
#> Source: IBGE - Cadastro de Localidades Selecionadas (2010)
#>         IBGE - Localidades do Brasil (2022)
#> Metadata:
# Titulo: Sedes Municipais
# Titulo alternativo: Municipal Seats / City Seats
# Frequencia de atualizacao: Censal
# Forma de apresentacao: Points
# Linguagem: Pt-BR
# Character set: WINDOWS-1252 (2010), UTF-8 (2022)
#
# Resumo: Localizacao pontual das sedes dos municipios brasileiros.
# Informacoes adicionais: Dados produzidos pelo IBGE.
# Proposito: Identificacao da localizacao das sedes municipais.
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas: municipal seat, sede municipal, city seat
# Informacao do Sistema de Referencia: SIRGAS 2000


# Download the data  -----------------------------------------------------------
download_muniseats <- function(year){

  ## 0. Set up the download links -----------------------------------------------

  if (year == 2010) {
    base_url <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/localidades/cadastro_de_localidades_selecionadas_2010/Shapefile_SHP/"
    shp_files <- list_folders(base_url)
    shp_files <- shp_files[ shp_files %like% "BR_Localidades_2010_v1"]

  } else if (year == 2022) {
    # 2022: zipped shapefile
    ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/localidades/Localidades_do_Brasil/2022/Localidades_Brasil_gpkg.zip"
  } else {
    stop(paste("Ano", year, "nao suportado para municipal_seat"))
  }

  ## 1. Create temp directories -------------------------------------------------

  zip_dir <- paste0(tempdir(), "/municipal_seat/", year)
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

    file_raw <- paste0(out_zip, "localidades_2022.zip")
    download.file(
      url = ftp_link,
      destfile = file_raw,
      mode = "wb",
      quiet = FALSE
    )

    utils::unzip(file_raw, exdir = out_zip, junkpaths = TRUE)
  }

  ## 3. Read shapefile ----------------------------------------------------------

  encode <- ifelse(year == 2010, "WINDOWS-1252", "UTF-8")

  # Find the .shp file
  shp_path <- list.files(out_zip, pattern = "\\.(shp|gpkg)$",
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

  raw <- raw |> 
    janitor::clean_names()
  
  raw$year <- year
  return(raw)
}

# Clean the data ---------------------------------------------------------------
clean_muniseats <- function(raw){

  ## 0. Create clean directory --------------------------------------------------
  yyyy <- raw$year[1]
  dir_clean <- paste0("./data/municipal_seat/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)


  ## 2. Filter municipal seats only ---------------------------------------------

  # 2010: filter by nm_categor == "CIDADE" (seats only, not vilas/other)
  # 2022: filter by cd_nivel == "CI" or nm_categor == "CIDADE"
  if (yyyy == 2010) {
    temp_sf <- subset(raw, nm_categor == "CIDADE")
  }
  
  if (yyyy == 2022) {
    temp_sf <- subset(raw, sct_localidade %in% c("Sede Municipal", "Capital Federal"))
    }

  ## 3. Rename columns to geobr standard ----------------------------------------
  
  # Map column names from IBGE to geobr standard
  if ("cd_geocodm" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, code_muni = cd_geocodm)
  } else if ("geocodigo" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, code_muni = geocodigo)
  } else if ("cd_mun" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, code_muni = cd_mun)
  } else if ("cd_geocmu" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, code_muni = cd_geocmu)
  }

  if ("nm_mun" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, name_muni = nm_mun)
  } else if ("nm_municip" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, name_muni = nm_municip)
  } else if ("nm_mun_2022" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, name_muni = nm_mun_2022)
  } else if ("nome" %in% names(temp_sf)) {
    temp_sf <- dplyr::rename(temp_sf, name_muni = nome)
  }


  ## 5. Derive code_state from code_muni ----------------------------------------

  temp_sf$code_state <- substr(temp_sf$code_muni, 1, 2)
  

  ## 6. Select only needed columns before harmonize -----------------------------

  if ("geom" %in% names(temp_sf)) {
    temp_sf <- temp_sf |>
    dplyr::rename(geometry = geom) |>
    sf::st_set_geometry("geometry")
  }
  
  temp_sf <- temp_sf |>
    dplyr::select(code_muni, name_muni, code_state, geometry)

  ## 7. Apply harmonize_geobr ---------------------------------------------------
  # Points: no topology fix, no multipolygon

  temp_sf <- harmonize_geobr(
    temp_sf        = temp_sf,
    year           = yyyy,
    add_state      = TRUE,
    state_column   = "code_state",
    add_region     = TRUE,
    region_column  = "code_state",
    add_snake_case = TRUE,
    snake_colname  = "name_muni",
    projection_fix = TRUE,
    encoding_utf8  = TRUE,
    topology_fix   = FALSE,
    remove_z_dimension = TRUE,
    use_multipolygon   = FALSE
  )
  
  
  ## 8. Reorder columns (geometry always last) ----------------------------------

  temp_sf <- temp_sf |>
    dplyr::select(code_muni, name_muni,
                  code_state, abbrev_state, name_state,
                  code_region, name_region,
                  year, geometry)

  # sort by key columns
  temp_sf <- temp_sf |> 
    dplyr::arrange(code_state, code_muni)
  
  ## 9. Validate ----------------------------------------------------------------

  stopifnot(is.numeric(temp_sf$code_muni))
  stopifnot(is.numeric(temp_sf$code_state))
  stopifnot(is.character(temp_sf$name_muni))
  stopifnot(is.character(temp_sf$abbrev_state))
  stopifnot(is.character(temp_sf$name_state))
  stopifnot(is.numeric(temp_sf$code_region))
  stopifnot(is.character(temp_sf$name_region))
  stopifnot(is.numeric(temp_sf$year))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")
  stopifnot(!is.na(sf::st_crs(temp_sf)))
  stopifnot(!any(is.na(temp_sf$code_state)))

  ## 10. Save parquet (no simplified version for points) -------------------------

  write_geobr_parquet(
    temp_sf,
    paste0(dir_clean, "/municipalseats_", yyyy, ".parquet"))

  ## 11. Return file paths ------------------------------------------------------

  files <- list.files(
    path = dir_clean,
    pattern = ".parquet",
    recursive = TRUE,
    full.names = TRUE
  )

  return(files)
}
