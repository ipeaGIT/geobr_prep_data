library(sf)
library(dplyr)
library(arrow)
library(geoarrow)
setwd("d:/Dropbox/Artigos/geobr_prep_data")
source("R/support_fun.R")
source("R/support_harmonize_geobr.R")
source("R/census_tract.R")

# Test unified logic for 4 small UFs: RO(11), AP(16), AL(27), SC(42)
test_ufs <- c("11", "16", "27", "42")

cat("=== Test unified partition for UFs:", paste(test_ufs, collapse=", "), "===\n\n")

# 1. Read inputs (same as unified does)
cat("Reading parquets...\n")
urb_clean <- read_geoparquet("data/census_tract/2000/census_tracts_2000_urbano.parquet")
rur_clean <- read_geoparquet("data/census_tract/2000/census_tracts_2000_rural.parquet")
tracts_2010 <- read_geoparquet("data/census_tract/2010/census_tracts_2010.parquet")
tracts_2010$code_tract <- as.character(tracts_2010$code_tract)

# Filter to test UFs only
urb_test <- urb_clean[substr(as.character(urb_clean$code_state), 1, 2) %in% test_ufs, ]
rur_test <- rur_clean[substr(as.character(rur_clean$code_state), 1, 2) %in% test_ufs, ]
cat(sprintf("Test subset: %d urbano, %d rural\n", nrow(urb_test), nrow(rur_test)))

rur_indiv <- dplyr::filter(rur_test, geom_source == "rural")
rur_agg <- dplyr::filter(rur_test, geom_source == "rural_aggregated")
cat(sprintf("Rural: %d individual, %d aggregated\n", nrow(rur_indiv), nrow(rur_agg)))

# 2. Load censobr ground truth
censobr_path <- "data-raw/census_tract/2000/censobr_tracts_2000.parquet"
censobr_codes <- arrow::read_parquet(censobr_path,
                                      col_select = "code_tract",
                                      as_data_frame = TRUE)
censobr_codes <- as.character(censobr_codes$code_tract) |> unique()
censobr_test <- censobr_codes[substr(censobr_codes, 1, 2) %in% test_ufs]
cat(sprintf("Censobr ground truth for test UFs: %d code_tracts\n\n", length(censobr_test)))

# 3. Partition each range
sf::sf_use_s2(FALSE)
dir_compat <- "data-raw/census_tract/compat"
compat_cache <- list()
partitioned_list <- list()
still_aggregated <- list()

for (i in seq_len(nrow(rur_agg))) {
  row <- rur_agg[i, ]
  range_code <- row$original_id
  if (is.na(range_code)) {
    still_aggregated[[length(still_aggregated) + 1]] <- row
    next
  }

  codes <- expand_range(range_code)
  # Fix B: filter by censobr
  codes <- codes[codes %in% censobr_codes]
  if (length(codes) == 0) {
    still_aggregated[[length(still_aggregated) + 1]] <- row
    next
  }

  uf <- substr(range_code, 1, 2)
  csv_file <- file.path(dir_compat, paste0("compat_", uf, "_C2000_", uf, ".csv"))
  if (!file.exists(csv_file)) {
    still_aggregated[[length(still_aggregated) + 1]] <- row
    next
  }

  if (is.null(compat_cache[[uf]])) {
    compat_cache[[uf]] <- read_compat_uf(uf, dir_compat)
  }

  t2010_uf <- tracts_2010[substr(as.character(tracts_2010$code_tract), 1, 2) == uf, ]
  parts <- partition_range_combined(
    range_poly = row, range_id = range_code,
    valid_codes = codes, tracts_2010_uf = t2010_uf,
    compat = compat_cache[[uf]]
  )

  if (!is.null(parts) && nrow(parts) > 0) {
    # New partition_range_combined returns 1 row per valid_code, no NAs
    parts_filled <- fill_gaps_voronoi(parts, row)

    # Copy metadata
    for (col in c("code_muni", "name_muni", "code_state", "abbrev_state",
                   "name_state", "code_region", "name_region", "year",
                   "code_neighborhood", "name_neighborhood",
                   "code_district", "name_district",
                   "code_subdistrict", "name_subdistrict", "zone")) {
      if (col %in% names(row)) parts_filled[[col]] <- as.vector(row[[col]])
    }
    parts_filled$geom_source <- "compat_partitioned"
    parts_filled$original_id <- range_code
    parts_filled$n_tracts_inside <- NA_real_
    parts_filled$code_tract <- as.numeric(parts_filled$code_tract)

    if (nrow(parts_filled) > 0) {
      partitioned_list[[length(partitioned_list) + 1]] <- parts_filled
    }
  } else {
    still_aggregated[[length(still_aggregated) + 1]] <- row
  }

  if (i %% 100 == 0) cat(sprintf("  range %d/%d\n", i, nrow(rur_agg)))
}

cat(sprintf("\n=== Partition results ===\n"))
n_part <- sum(sapply(partitioned_list, nrow))
n_agg <- length(still_aggregated)
cat(sprintf("Partitioned features: %d\n", n_part))
cat(sprintf("Still aggregated: %d\n", n_agg))
cat(sprintf("(new design: unresolved codes get Voronoi seeds)\n"))

# Combine
if (length(partitioned_list) > 0) {
  partitioned_sf <- dplyr::bind_rows(partitioned_list)
  # Rural priority
  n_before <- nrow(partitioned_sf)
  partitioned_sf <- partitioned_sf |>
    dplyr::filter(!code_tract %in% rur_indiv$code_tract)
  cat(sprintf("After rural priority: %d -> %d\n", n_before, nrow(partitioned_sf)))
} else {
  partitioned_sf <- rur_agg[0, ]
}

if (length(still_aggregated) > 0) {
  still_agg_sf <- dplyr::bind_rows(still_aggregated)
} else {
  still_agg_sf <- rur_agg[0, ]
}

rur_processed <- dplyr::bind_rows(rur_indiv, partitioned_sf, still_agg_sf)

# Merge urban > rural
rur_only <- dplyr::filter(rur_processed, !code_tract %in% urb_test$code_tract)
uni <- dplyr::bind_rows(urb_test, rur_only)

cat(sprintf("\n=== FINAL RESULTS ===\n"))
cat(sprintf("Unified features: %d\n", nrow(uni)))
cat(sprintf("Censobr expected: %d\n", length(censobr_test)))
cat(sprintf("Difference: %d\n", nrow(uni) - length(censobr_test)))
cat(sprintf("code_tract NAs: %d\n", sum(is.na(uni$code_tract))))
cat(sprintf("code_tract duplicates: %d\n", sum(duplicated(uni$code_tract[!is.na(uni$code_tract)]))))
cat(sprintf("geom_source:\n"))
print(table(uni$geom_source, useNA = "ifany"))
cat(sprintf("\nmatch_method:\n"))
print(table(uni$match_method, useNA = "ifany"))
