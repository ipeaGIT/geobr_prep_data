#> DATASET: river basins (Divisao Hidrografica Nacional - DHN250)
#> Source: IBGE - https://geoftp.ibge.gov.br/informacoes_ambientais/estudos_ambientais/bacias_e_divisoes_hidrograficas_do_brasil/
#> Scale: 1:250.000
#> Metadata:
# Titulo: Divisao Hidrografica Nacional
# Titulo alternativo: river basins / water basins
# Frequencia de atualizacao: Ocasional
#
# Forma de apresentacao: Shapefile
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Poligonos das Regioes Hidrograficas do Brasil em 3 niveis
#   hierarquicos (macro, meso, micro), conforme classificacao da ANA/IBGE.
# Informacoes adicionais: Dados produzidos pelo IBGE em parceria com a ANA
#   (Agencia Nacional de Aguas). Os vetores estao disponíveis na escala
#   1:250.000 (DHN250).
# Proposito: Disponibilizacao das divisoes hidrograficas do Brasil.
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas: hidrografia, bacias, regioes hidrograficas
# Informacao do Sistema de Referencia: SIRGAS 2000 (EPSG:4674)
#
# Observacoes:
#   - Anos disponiveis: 2021
#   - 3 niveis: macro (12 regioes), meso (54 regioes), micro (302 regioes)
#   - Dados organizados em:
#     macro_RH.zip → cd_macroRH, nm_macroRH
#     meso_RH.zip  → cd_mesoRH, nm_mesoRH, cd_macroRH, nm_macroRH
#     micro_RH.zip → cd_microRH, nm_microRH, cd_mesoRH, nm_mesoRH, cd_macroRH, nm_macroRH

### Libraries (use any library as necessary) -----------------------------------

# library(RCurl)
# library(arrow)
# library(geoarrow)
# library(stringr)
# library(sf)
# library(purrr)
# library(janitor)
# library(dplyr)
# library(readr)
# library(data.table)
# library(magrittr)
# library(devtools)
# library(lwgeom)
# library(stringi)
# library(tidyverse)
# library(mirai)
# library(rvest)
# source("./R/support_harmonize_geobr.R")
# source("./R/support_fun.R")

# Download the data  -----------------------------------------------------------
download_riverbasins <- function(year) { # year = 2021

  ## 0. Set up URLs (UPDATE YEAR HERE) -----------------------------------------

  if (year == 2021) {
    base_url <- paste0(
      "https://geoftp.ibge.gov.br/informacoes_ambientais/",
      "estudos_ambientais/bacias_e_divisoes_hidrograficas_do_brasil/",
      "2021/Divisao_Hidrografica_Nacional_DHN250/vetores/"
    )
  } else {
    stop(paste("Year", year, "not supported for river_basins"))
  }

  ## 1. Create temp directories ------------------------------------------------

  zip_dir <- paste0(tempdir(), "/river_basins/", year)
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, recursive = TRUE, showWarnings = FALSE)

  ## 2. Download all 3 levels --------------------------------------------------

  levels <- c("macro_RH", "meso_RH", "micro_RH")
  raw_list <- list()

  for (lvl in levels) {

    ftp_link <- paste0(base_url, lvl, ".zip")

    lvl_zip <- paste0(zip_dir, lvl, ".zip")
    lvl_dir <- paste0(out_zip, lvl, "/")
    dir.create(lvl_dir, recursive = TRUE, showWarnings = FALSE)

    # zip_file <- download_file_geobr(
    #   file_url = ftp, 
    #   dest_dir = dest_dir
    # )
    
    download.file(url = ftp_link, destfile = lvl_zip, mode = "wb", quiet = FALSE)
    unzip(lvl_zip, exdir = lvl_dir)

    raw_list[[lvl]] <- sf::st_read(lvl_dir, quiet = TRUE, stringsAsFactors = FALSE)
  }

  # add year
  raw_list <- lapply(
    X = raw_list,
    FUN = function(df){
      df <- df |> 
        dplyr::mutate(year = year)
    }
    )
  ## 3. Return as list of 3 sf objects -----------------------------------------

  return(raw_list)
}


# Clean the data  --------------------------------------------------------------
clean_riverbasins <- function(raw) { # year = 2021

  ## 0. Create clean directory -------------------------------------------------

  yyyy <- raw[[1]]$year
  dir_clean <- paste0("./data/river_basins/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  ## 1. Extract and standardize each level -------------------------------------

  # --- MACRO (12 regioes hidrograficas) ---
  macro <- raw[["macro_RH"]] |>
    dplyr::rename(
      code_water_basin = cd_macroRH,
      name_water_basin = nm_macroRH,
      area_km2         = area
    ) |>
    dplyr::mutate(
      level = "macro",
      code_water_basin = code_water_basin
    ) |>
    dplyr::select(code_water_basin, name_water_basin, level, area_km2, geometry)

  # --- MESO (54 regioes) ---
  meso <- raw[["meso_RH"]] |>
    dplyr::rename(
      code_water_basin       = cd_mesoRH,
      name_water_basin       = nm_mesoRH,
      code_water_basin_macro = cd_macroRH,
      name_water_basin_macro = nm_macroRH,
      area_km2               = area
    ) |>
    dplyr::mutate(
      level = "meso",
      code_water_basin       = code_water_basin,
      code_water_basin_macro = code_water_basin_macro
    ) |>
    dplyr::select(code_water_basin, name_water_basin,
                  code_water_basin_macro, name_water_basin_macro,
                  level, area_km2, geometry)

  # --- MICRO (302 regioes) ---
  micro <- raw[["micro_RH"]] |>
    dplyr::rename(
      code_water_basin       = cd_microRH,
      name_water_basin       = nm_microRH,
      code_water_basin_meso  = cd_mesoRH,
      name_water_basin_meso  = nm_mesoRH,
      code_water_basin_macro = cd_macroRH,
      name_water_basin_macro = nm_macroRH,
      area_km2               = area
    ) |>
    dplyr::mutate(
      level = "micro",
      code_water_basin       = code_water_basin,
      code_water_basin_meso  = code_water_basin_meso,
      code_water_basin_macro = code_water_basin_macro
    ) |>
    dplyr::select(code_water_basin, name_water_basin,
                  code_water_basin_meso, name_water_basin_meso,
                  code_water_basin_macro, name_water_basin_macro,
                  level, area_km2, geometry)

  ## 2. Harmonize each level ---------------------------------------------------

  harmonize_level <- function(temp_sf) {
    temp_sf <- harmonize_geobr(
      temp_sf            = temp_sf,
      year               = yyyy,
      add_state          = FALSE,
      add_region         = FALSE,
      add_snake_case     = TRUE,
      snake_colname      = "name_water_basin",
      projection_fix     = TRUE,
      encoding_utf8      = TRUE,
      topology_fix       = TRUE,
      remove_z_dimension = TRUE,
      use_multipolygon   = FALSE
    )

    # Ensure sf and CRS
    temp_sf <- dplyr::ungroup(temp_sf)
    if (!inherits(temp_sf, "sf")) {
      temp_sf <- sf::st_as_sf(temp_sf)
    }
    if (is.na(sf::st_crs(temp_sf))) {
      sf::st_crs(temp_sf) <- 4674
    }

    # Cast to MULTIPOLYGON
    temp_sf <- sf::st_cast(temp_sf, "MULTIPOLYGON")

    return(temp_sf)
  }

  macro <- harmonize_level(macro)
  meso  <- harmonize_level(meso)
  micro <- harmonize_level(micro)

  # Also snake_case the parent-level name columns in meso/micro
  meso  <- snake_case_names(meso,  colname = "name_water_basin_macro")
  micro <- snake_case_names(micro, colname = c("name_water_basin_meso",
                                                "name_water_basin_macro"))

  ## 3. Reorder columns (geometry last) ----------------------------------------

  macro <- macro |>
    dplyr::select(code_water_basin, name_water_basin,
                  level, area_km2, year, geometry)

  meso <- meso |>
    dplyr::select(code_water_basin, name_water_basin,
                  code_water_basin_macro, name_water_basin_macro,
                  level, area_km2, year, geometry)

  micro <- micro |>
    dplyr::select(code_water_basin, name_water_basin,
                  code_water_basin_meso, name_water_basin_meso,
                  code_water_basin_macro, name_water_basin_macro,
                  level, area_km2, year, geometry)

  ## 4. Validate ---------------------------------------------------------------

  validate_level <- function(sf_obj, label) {
    stopifnot(sf::st_crs(sf_obj)$epsg == 4674)
    stopifnot(all(sf::st_geometry_type(sf_obj) == "MULTIPOLYGON"))
    stopifnot(names(sf_obj)[ncol(sf_obj)] == "geometry")
    stopifnot(is.numeric(sf_obj$code_water_basin))
    stopifnot(is.character(sf_obj$name_water_basin))
    stopifnot(is.numeric(sf_obj$year))
  }

  validate_level(macro, "macro")
  validate_level(meso,  "meso")
  validate_level(micro, "micro")

  ## 5. Simplify ---------------------------------------------------------------

  macro_simplified <- simplify_temp_sf(macro, tolerance = 500)
  meso_simplified  <- simplify_temp_sf(meso,  tolerance = 100)
  micro_simplified <- simplify_temp_sf(micro, tolerance = 100)

  ## 6. Save Parquet files -----------------------------------------------------

  save_level <- function(sf_obj, sf_simp, level_name) {
    write_geobr_parquet(
      sf_obj,
      paste0(dir_clean, "/river_basins_", level_name, "_", yyyy, ".parquet"))
    write_geobr_parquet(
      sf_simp,
      paste0(dir_clean, "/river_basins_", level_name, "_", yyyy, "_simplified.parquet"))
  }

  save_level(macro, macro_simplified, "macro")
  save_level(meso,  meso_simplified,  "meso")
  save_level(micro, micro_simplified, "micro")

  ## 7. Return file paths ------------------------------------------------------

  files <- list.files(
    path = dir_clean,
    pattern = "\\.parquet$",
    recursive = TRUE,
    full.names = TRUE
  )

  return(files)
}
