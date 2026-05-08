library(sf)
library(dplyr)
library(arrow)
library(geoarrow)
setwd("d:/Dropbox/Artigos/geobr_prep_data")
source("R/support_fun.R")
source("R/support_harmonize_geobr.R")
source("R/census_tract.R")

test_ufs <- c("11", "16", "27", "42")

cat("=== Verify: are the 96 'missing' codes actually in urbano? ===\n\n")

censobr_all <- arrow::read_parquet("data-raw/census_tract/2000/censobr_tracts_2000.parquet",
                                    as_data_frame = TRUE)
censobr_all$code_tract <- as.character(censobr_all$code_tract)
censobr_test <- unique(censobr_all$code_tract[substr(censobr_all$code_tract, 1, 2) %in% test_ufs])

urb <- read_geoparquet("data/census_tract/2000/census_tracts_2000_urbano.parquet")
rur <- read_geoparquet("data/census_tract/2000/census_tracts_2000_rural.parquet")
urb_test <- urb[substr(as.character(urb$code_state), 1, 2) %in% test_ufs, ]
rur_test <- rur[substr(as.character(rur$code_state), 1, 2) %in% test_ufs, ]

# Build the same unified as test_unified_small.R
rur_indiv <- dplyr::filter(rur_test, geom_source == "rural")

# All compat partitioned codes (from the test: 2651 codes)
# Simulate by getting all codes from ranges
rur_agg <- dplyr::filter(rur_test, geom_source == "rural_aggregated")
range_codes <- character()
for (i in seq_len(nrow(rur_agg))) {
  rid <- rur_agg$original_id[i]
  if (is.na(rid)) next
  codes <- expand_range(rid)
  valid <- codes[codes %in% censobr_all$code_tract]
  range_codes <- c(range_codes, valid)
}
range_codes <- unique(range_codes)

# All codes in unified = urbano + rur_indiv + range_partitioned - overlaps
urb_codes <- unique(as.character(urb_test$code_tract))
rur_indiv_codes <- unique(as.character(rur_indiv$code_tract))

# The unified merge removes:
# 1. compat codes that duplicate rur_indiv
compat_after_rural <- range_codes[!range_codes %in% rur_indiv_codes]
# 2. rural codes that duplicate urbano
all_rural_codes <- unique(c(rur_indiv_codes, compat_after_rural))
rural_after_urb <- all_rural_codes[!all_rural_codes %in% urb_codes]

unified_codes <- unique(c(urb_codes, rural_after_urb))

cat("Censobr test:", length(censobr_test), "\n")
cat("Unified codes:", length(unified_codes), "\n")
cat("Difference:", length(censobr_test) - length(unified_codes), "\n\n")

missing_from_unified <- setdiff(censobr_test, unified_codes)
cat("Missing from unified:", length(missing_from_unified), "\n")

if (length(missing_from_unified) > 0) {
  # Are they in urbano?
  in_urb <- missing_from_unified[missing_from_unified %in% urb_codes]
  cat("  Of those, in urbano:", length(in_urb), "\n")
  # Are they in rur_indiv?
  in_rur <- missing_from_unified[missing_from_unified %in% rur_indiv_codes]
  cat("  Of those, in rur_indiv:", length(in_rur), "\n")
  # Are they in ranges?
  in_ranges <- missing_from_unified[missing_from_unified %in% range_codes]
  cat("  Of those, in ranges:", length(in_ranges), "\n")
  # Nowhere?
  nowhere <- missing_from_unified[!missing_from_unified %in% c(urb_codes, rur_indiv_codes, range_codes)]
  cat("  NOWHERE (not in any source):", length(nowhere), "\n")
  if (length(nowhere) > 0) {
    cat("  First 10 nowhere codes:", head(nowhere, 10), "\n")
  }
} else {
  cat("ALL censobr codes are in unified! No missing.\n")
}

# Cross-check: codes removed by urban > rural filter
overlap_urb_compat <- range_codes[range_codes %in% urb_codes]
cat("\nRange codes also in urbano (removed by urb > rural):", length(overlap_urb_compat), "\n")
