# Unit test: end-to-end real para 3 anos representativos (1970, 2010, 2024)
# Inclui download + clean + join com municipality/hist_muni
suppressMessages(library(dplyr))
suppressMessages(library(tidyr))
suppressMessages(library(readxl))
suppressMessages(library(arrow))
suppressMessages(library(geoarrow))   # required by read_geoparquet fallback
suppressMessages(library(sfarrow))
suppressMessages(library(sf))
suppressMessages(library(lwgeom))

source("R/support_fun.R")
source("R/support_harmonize_geobr.R")
source("R/metro_area.R")

# Listas de file paths para passar como argumentos (mimetiza o que tar_target produz)
muni_paths <- list.files("data/municipality/", pattern = "\\.parquet$",
                         recursive = TRUE, full.names = TRUE)
hist_paths <- list.files("data/historical_empire/", pattern = "\\.parquet$",
                         recursive = TRUE, full.names = TRUE)

# Função para testar um ano
test_year <- function(year, expected_n_rms_min, expected_n_munis_min) {
  cat("--- Testing year", year, "---\n")
  raw <- download_metro_area(year)
  files <- clean_metro_area(raw, muni_paths, hist_paths)

  parquet_main <- files[grepl(paste0(year, "/metro_area_", year, "\\.parquet$"), files)]
  parquet_main <- parquet_main[!grepl("simplified", parquet_main)]

  ds <- read_geoparquet(parquet_main)

  cat("  nrow:", nrow(ds), "\n")
  cat("  n_rms:", length(unique(ds$name_metro)), "\n")
  cat("  n_munis:", length(unique(ds$code_muni)), "\n")
  cat("  CRS:", sf::st_crs(ds)$epsg, "\n")
  cat("  geom types:", paste(unique(as.character(sf::st_geometry_type(ds))), collapse=","), "\n")
  cat("  last col:", names(ds)[ncol(ds)], "\n")

  stopifnot(sf::st_crs(ds)$epsg == 4674)
  stopifnot(all(sf::st_geometry_type(ds) == "MULTIPOLYGON"))
  stopifnot(names(ds)[ncol(ds)] == "geometry")
  stopifnot(is.numeric(ds$code_muni))
  stopifnot(is.numeric(ds$code_state))
  stopifnot(is.character(ds$abbrev_state))
  stopifnot(all(nchar(ds$abbrev_state) == 2))
  stopifnot(length(unique(ds$name_metro)) >= expected_n_rms_min)
  stopifnot(length(unique(ds$code_muni)) >= expected_n_munis_min)
}

# 1970 (hardcoded; 9 RMs, 113 munis; 1 unmatched ja resolvido pelo merge Guanabara)
test_year(1970, expected_n_rms_min = 9, expected_n_munis_min = 100)

# 2010 (junho, snapshot único; ~42 RMs, ~696 munis)
test_year(2010, expected_n_rms_min = 40, expected_n_munis_min = 600)

# 2024 (snapshot novo schema 16-col; ~80 RMs, ~1398 munis)
test_year(2024, expected_n_rms_min = 75, expected_n_munis_min = 1300)

cat("PASS: unit_metro_area_e2e_3years\n")
