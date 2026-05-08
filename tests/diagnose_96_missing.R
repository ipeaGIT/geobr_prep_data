library(sf)
library(dplyr)
library(arrow)
library(geoarrow)
setwd("d:/Dropbox/Artigos/geobr_prep_data")
source("R/support_fun.R")
source("R/support_harmonize_geobr.R")
source("R/census_tract.R")

test_ufs <- c("11", "16", "27", "42")
cat("=== Diagnóstico: -96 faltantes + zero graph_* ===\n\n")

# Load censobr
censobr_all <- arrow::read_parquet("data-raw/census_tract/2000/censobr_tracts_2000.parquet",
                                    as_data_frame = TRUE)
censobr_all$code_tract <- as.character(censobr_all$code_tract)
censobr_test <- censobr_all$code_tract[substr(censobr_all$code_tract, 1, 2) %in% test_ufs]
censobr_test <- unique(censobr_test)

# Load parquets
urb <- read_geoparquet("data/census_tract/2000/census_tracts_2000_urbano.parquet")
rur <- read_geoparquet("data/census_tract/2000/census_tracts_2000_rural.parquet")
urb_test <- urb[substr(as.character(urb$code_state), 1, 2) %in% test_ufs, ]
rur_test <- rur[substr(as.character(rur$code_state), 1, 2) %in% test_ufs, ]

# All codes in parquets
all_data_codes <- unique(c(as.character(urb_test$code_tract),
                            as.character(rur_test$code_tract)))

# Missing from parquets entirely
missing <- setdiff(censobr_test, all_data_codes)
cat("Censobr codes missing from urbano+rural parquets:", length(missing), "\n")

# These missing codes are inside ranges. Check which ranges contain them.
rur_agg <- rur_test[rur_test$geom_source == "rural_aggregated", ]
censobr_codes <- unique(censobr_all$code_tract)

cat("\n=== Analysis of each still_aggregated range ===\n")
# Read compat for all test UFs
compat_cache <- list()
dir_compat <- "data-raw/census_tract/compat"
tracts_2010 <- read_geoparquet("data/census_tract/2010/census_tracts_2010.parquet")
tracts_2010$code_tract <- as.character(tracts_2010$code_tract)

n_still_agg_codes <- 0
for (i in seq_len(nrow(rur_agg))) {
  rid <- rur_agg$original_id[i]
  if (is.na(rid)) next
  expanded <- expand_range(rid)
  valid <- expanded[expanded %in% censobr_codes]
  uf <- substr(rid, 1, 2)
  csv_file <- file.path(dir_compat, paste0("compat_", uf, "_C2000_", uf, ".csv"))

  if (!file.exists(csv_file)) {
    cat(sprintf("  Range %s: NO COMPAT CSV for UF %s (%d valid codes lost)\n",
                rid, uf, length(valid)))
    n_still_agg_codes <- n_still_agg_codes + length(valid)
    next
  }

  # Try partition
  if (is.null(compat_cache[[uf]])) {
    compat_cache[[uf]] <- read_compat_uf(uf, dir_compat)
  }
  t2010_uf <- tracts_2010[substr(as.character(tracts_2010$code_tract), 1, 2) == uf, ]

  parts <- partition_range_combined(
    range_poly = rur_agg[i, ], range_id = rid,
    valid_codes = valid, tracts_2010_uf = t2010_uf,
    compat = compat_cache[[uf]]
  )

  if (is.null(parts) || nrow(parts) == 0) {
    cat(sprintf("  Range %s: partition returned NULL (%d valid codes, compat exists)\n",
                rid, length(valid)))
    n_still_agg_codes <- n_still_agg_codes + length(valid)
  }
}
cat(sprintf("\nTotal codes in still_aggregated ranges: %d\n", n_still_agg_codes))

cat("\n=== Strategy B (graph_indiv) diagnosis ===\n")
# Check if ANY individual edges exist for valid_codes in test UFs
n_indiv_edges_total <- 0
for (uf in test_ufs) {
  if (is.null(compat_cache[[uf]])) {
    csv_file <- file.path(dir_compat, paste0("compat_", uf, "_C2000_", uf, ".csv"))
    if (file.exists(csv_file)) compat_cache[[uf]] <- read_compat_uf(uf, dir_compat)
  }
  if (is.null(compat_cache[[uf]])) next

  edges <- compat_cache[[uf]]$edges
  # Individual edges = A.nome is NOT range notation AND IS a valid_code
  rur_agg_uf <- rur_agg[substr(as.character(rur_agg$code_state), 1, 2) == uf, ]
  all_valid <- character()
  for (j in seq_len(nrow(rur_agg_uf))) {
    rid <- rur_agg_uf$original_id[j]
    if (is.na(rid)) next
    exp <- expand_range(rid)
    val <- exp[exp %in% censobr_codes]
    all_valid <- c(all_valid, val)
  }
  all_valid <- unique(all_valid)

  indiv_edges <- edges[edges$A.nome %in% all_valid & !grepl("-", edges$A.nome), ]
  cat(sprintf("UF %s: %d valid codes in ranges, %d individual edges found\n",
              uf, length(all_valid), nrow(indiv_edges)))
  n_indiv_edges_total <- n_indiv_edges_total + nrow(indiv_edges)

  if (nrow(indiv_edges) > 0) {
    cat("  metodo distribution:\n")
    print(table(indiv_edges$metodo))
    cat("  Sample A.nome → B.nome:\n")
    print(head(indiv_edges[, c("A.nome", "B.nome", "metodo")], 5))
  }
}
cat(sprintf("\nTotal individual edges for range codes: %d\n", n_indiv_edges_total))
