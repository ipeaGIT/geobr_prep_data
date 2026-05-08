library(sf)
library(dplyr)
library(arrow)
library(geoarrow)
setwd("d:/Dropbox/Artigos/geobr_prep_data")
source("R/support_fun.R")
source("R/support_harmonize_geobr.R")
source("R/census_tract.R")

test_ufs <- c("11", "16", "27", "42")
sf::sf_use_s2(FALSE)

rur <- read_geoparquet("data/census_tract/2000/census_tracts_2000_rural.parquet")
rur_test <- rur[substr(as.character(rur$code_state), 1, 2) %in% test_ufs, ]
rur_agg <- dplyr::filter(rur_test, geom_source == "rural_aggregated")
tracts_2010 <- read_geoparquet("data/census_tract/2010/census_tracts_2010.parquet")
tracts_2010$code_tract <- as.character(tracts_2010$code_tract)
censobr_all <- arrow::read_parquet("data-raw/census_tract/2000/censobr_tracts_2000.parquet",
                                    as_data_frame = TRUE)
censobr_codes <- unique(as.character(censobr_all$code_tract))
dir_compat <- "data-raw/census_tract/compat"
compat_cache <- list()

cat("=== Counting NULL partitions and lost valid_codes ===\n\n")

n_null <- 0
n_ok <- 0
n_still_agg <- 0
total_valid_in <- 0
total_valid_out <- 0

for (i in seq_len(nrow(rur_agg))) {
  row <- rur_agg[i, ]
  range_code <- row$original_id
  if (is.na(range_code)) { n_still_agg <- n_still_agg + 1; next }

  codes <- expand_range(range_code)
  codes <- codes[codes %in% censobr_codes]
  if (length(codes) == 0) { n_still_agg <- n_still_agg + 1; next }

  uf <- substr(range_code, 1, 2)
  csv_file <- file.path(dir_compat, paste0("compat_", uf, "_C2000_", uf, ".csv"))
  if (!file.exists(csv_file)) { n_still_agg <- n_still_agg + 1; next }

  if (is.null(compat_cache[[uf]])) compat_cache[[uf]] <- read_compat_uf(uf, dir_compat)
  t2010_uf <- tracts_2010[substr(tracts_2010$code_tract, 1, 2) == uf, ]

  total_valid_in <- total_valid_in + length(codes)

  parts <- partition_range_combined(
    range_poly = row, range_id = range_code,
    valid_codes = codes, tracts_2010_uf = t2010_uf,
    compat = compat_cache[[uf]]
  )

  if (is.null(parts) || nrow(parts) == 0) {
    n_null <- n_null + 1
    if (i <= 20 || n_null <= 5) {
      cat(sprintf("  NULL: range %s (%d valid_codes)\n", range_code, length(codes)))
    }
  } else {
    n_ok <- n_ok + 1
    total_valid_out <- total_valid_out + nrow(parts)
    if (nrow(parts) != length(codes)) {
      cat(sprintf("  MISMATCH: range %s: %d valid → %d output\n",
                  range_code, length(codes), nrow(parts)))
    }
  }
}

cat(sprintf("\n=== Summary ===\n"))
cat(sprintf("Ranges total: %d\n", nrow(rur_agg)))
cat(sprintf("  Partitioned OK: %d\n", n_ok))
cat(sprintf("  Returned NULL: %d\n", n_null))
cat(sprintf("  Still aggregated (no data): %d\n", n_still_agg))
cat(sprintf("Valid codes in: %d\n", total_valid_in))
cat(sprintf("Valid codes out: %d\n", total_valid_out))
cat(sprintf("Lost codes: %d\n", total_valid_in - total_valid_out))
