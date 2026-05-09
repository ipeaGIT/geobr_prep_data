library(sf)
library(dplyr)
library(arrow)
library(geoarrow)
setwd("d:/Dropbox/Artigos/geobr_prep_data")
source("R/support_fun.R")
source("R/support_harmonize_geobr.R")
source("R/census_tract.R")

test_ufs <- c("11", "16", "27", "42")

cat("=== Diagnóstico: quais code_tracts censobr estão faltando? ===\n\n")

# Load censobr
censobr_all <- arrow::read_parquet("data-raw/census_tract/2000/censobr_tracts_2000.parquet",
                                    as_data_frame = TRUE)
censobr_all$code_tract <- as.character(censobr_all$code_tract)
censobr_test <- censobr_all[substr(censobr_all$code_tract, 1, 2) %in% test_ufs, ]
cat("Censobr for 4 UFs:", nrow(censobr_test), "tracts\n")

# Load rural and urbano
urb_clean <- read_geoparquet("data/census_tract/2000/census_tracts_2000_urbano.parquet")
rur_clean <- read_geoparquet("data/census_tract/2000/census_tracts_2000_rural.parquet")

urb_test <- urb_clean[substr(as.character(urb_clean$code_state), 1, 2) %in% test_ufs, ]
rur_test <- rur_clean[substr(as.character(rur_clean$code_state), 1, 2) %in% test_ufs, ]

# All code_tracts currently in unified (urb + rur)
all_codes_in_data <- c(as.character(urb_test$code_tract),
                        as.character(rur_test$code_tract))
cat("Codes in urbano+rural parquets:", length(unique(all_codes_in_data)), "\n")

# Which censobr codes are missing from the data entirely?
missing <- setdiff(censobr_test$code_tract, all_codes_in_data)
cat("Missing from urbano+rural:", length(missing), "\n\n")

if (length(missing) > 0) {
  # These are codes in censobr but NOT in any parquet
  # They must be inside ranges that were collapsed
  cat("=== Where are the missing codes? ===\n")

  # Check if they're in expanded ranges
  rur_agg <- dplyr::filter(rur_test, geom_source == "rural_aggregated")
  n_found_in_ranges <- 0
  n_not_in_ranges <- 0
  range_coverage <- list()

  for (mc in missing) {
    uf <- substr(mc, 1, 2)
    # Find which range contains this code
    found <- FALSE
    for (j in seq_len(nrow(rur_agg))) {
      rid <- rur_agg$original_id[j]
      if (is.na(rid)) next
      expanded <- expand_range(rid)
      if (mc %in% expanded) {
        n_found_in_ranges <- n_found_in_ranges + 1
        found <- TRUE
        break
      }
    }
    if (!found) n_not_in_ranges <- n_not_in_ranges + 1
  }

  cat(sprintf("  Found inside expanded ranges: %d\n", n_found_in_ranges))
  cat(sprintf("  NOT in any range: %d\n", n_not_in_ranges))
  cat(sprintf("  (these %d are the truly lost codes)\n\n", n_not_in_ranges))

  # Show first 10 missing codes and their situacao
  cat("First 20 missing codes + situacao:\n")
  missing_info <- censobr_test[censobr_test$code_tract %in% missing[1:20],
                                c("code_tract", "code_muni", "situacao")]
  print(missing_info)

  # Check situacao distribution
  cat("\nSituacao of ALL missing codes:\n")
  print(table(censobr_test$situacao[censobr_test$code_tract %in% missing]))
}

# Also check: how many censobr codes are in ranges but NOT in rur_indiv?
rur_indiv <- dplyr::filter(rur_test, geom_source == "rural")
censobr_rural <- censobr_test$code_tract[!censobr_test$code_tract %in%
                                           as.character(urb_test$code_tract)]
cat("\nCensobr rural codes (not in urbano):", length(censobr_rural), "\n")
cat("Of those, in rur_indiv:", sum(censobr_rural %in% as.character(rur_indiv$code_tract)), "\n")
cat("Of those, NOT in rur_indiv:", sum(!censobr_rural %in% as.character(rur_indiv$code_tract)), "\n")
cat("  → these are inside ranges and need partitioning\n")
