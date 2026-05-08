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

cat("=== PART 1: Strategy B diagnosis for one range ===\n\n")

# Load data
rur <- read_geoparquet("data/census_tract/2000/census_tracts_2000_rural.parquet")
rur_test <- rur[substr(as.character(rur$code_state), 1, 2) %in% test_ufs, ]
rur_agg <- dplyr::filter(rur_test, geom_source == "rural_aggregated")
tracts_2010 <- read_geoparquet("data/census_tract/2010/census_tracts_2010.parquet")
tracts_2010$code_tract <- as.character(tracts_2010$code_tract)

censobr_all <- arrow::read_parquet("data-raw/census_tract/2000/censobr_tracts_2000.parquet",
                                    as_data_frame = TRUE)
censobr_codes <- unique(as.character(censobr_all$code_tract))

# Pick Ariquemes range (well-studied)
row <- rur_agg[rur_agg$original_id == "110002305000001-0074", ]
if (nrow(row) == 0) {
  cat("Ariquemes range not found, picking first RO range\n")
  row <- rur_agg[substr(as.character(rur_agg$code_state), 1, 2) == "11", ][1, ]
}
range_code <- row$original_id
cat("Range:", range_code, "\n")

codes <- expand_range(range_code)
valid_codes <- codes[codes %in% censobr_codes]
cat("Expanded:", length(codes), "Valid censobr:", length(valid_codes), "\n")

compat <- read_compat_uf("11", "data-raw/census_tract/compat")
t2010_uf <- tracts_2010[substr(tracts_2010$code_tract, 1, 2) == "11", ]

# Spatial pool
candidates_idx <- sf::st_intersects(sf::st_geometry(row), sf::st_geometry(t2010_uf))[[1]]
candidates <- t2010_uf[candidates_idx, ]
pool_clipped <- sf::st_intersection(candidates, sf::st_geometry(row))
pool_clipped <- sf::st_collection_extract(pool_clipped, "POLYGON")
pool_clipped <- sf::st_make_valid(pool_clipped[!sf::st_is_empty(pool_clipped), ])
pool_clipped$b_code <- as.character(pool_clipped$code_tract)
cat("Pool size:", nrow(pool_clipped), "fragments\n")
cat("Pool unique codes:", length(unique(pool_clipped$b_code)), "\n")

# Strategy A: direct match
direct <- valid_codes[valid_codes %in% pool_clipped$b_code]
cat("\nStrategy A (direct_match):", length(direct), "of", length(valid_codes), "\n")

remaining_after_A <- setdiff(valid_codes, direct)
cat("Remaining after A:", length(remaining_after_A), "\n")

# Strategy B: graph individual edges
indiv_edges <- compat$edges[compat$edges$A.nome %in% valid_codes &
                            !grepl("-", compat$edges$A.nome), ]
cat("\nIndividual edges for valid_codes:", nrow(indiv_edges), "\n")

# For each remaining code, check if Strategy B can find a match
n_B_found <- 0
n_B_fail <- 0
for (vc in remaining_after_A[1:min(20, length(remaining_after_A))]) {
  vc_edges <- indiv_edges[indiv_edges$A.nome == vc, ]
  if (nrow(vc_edges) > 0) {
    available <- vc_edges[vc_edges$B.nome %in% pool_clipped$b_code, ]
    if (nrow(available) > 0) {
      n_B_found <- n_B_found + 1
      cat(sprintf("  B HIT: %s → %s (%s)\n", vc, available$B.nome[1], available$metodo[1]))
    } else {
      n_B_fail <- n_B_fail + 1
      cat(sprintf("  B MISS: %s has edges but B.nome not in pool (%s)\n",
                  vc, paste(vc_edges$B.nome, collapse=",")))
    }
  } else {
    # No individual edge for this code — it only exists in the range node
    n_B_fail <- n_B_fail + 1
  }
}
cat(sprintf("\nStrategy B: %d found, %d fail (of first %d remaining)\n",
            n_B_found, n_B_fail, min(20, length(remaining_after_A))))

cat("\n=== PART 2: The 96 missing codes ===\n\n")

# Build unified for 4 UFs (quick version — just count codes)
urb <- read_geoparquet("data/census_tract/2000/census_tracts_2000_urbano.parquet")
urb_test <- urb[substr(as.character(urb$code_state), 1, 2) %in% test_ufs, ]
rur_indiv <- dplyr::filter(rur_test, geom_source == "rural")

censobr_test <- censobr_codes[substr(censobr_codes, 1, 2) %in% test_ufs]

# Codes present in data
urb_codes <- as.character(urb_test$code_tract)
rur_indiv_codes <- as.character(rur_indiv$code_tract)
all_data_codes <- unique(c(urb_codes, rur_indiv_codes))

# Missing from all data
missing_from_data <- setdiff(censobr_test, all_data_codes)
cat("Codes in censobr but not in urbano+rural_individual:", length(missing_from_data), "\n")
cat("  (these are inside ranges and need partitioning)\n")

# Check which ranges contain missing codes
n_in_range <- 0
n_not_in_range <- 0
for (mc in missing_from_data[1:min(50, length(missing_from_data))]) {
  found <- FALSE
  for (j in seq_len(nrow(rur_agg))) {
    rid <- rur_agg$original_id[j]
    if (is.na(rid)) next
    exp <- expand_range(rid)
    if (mc %in% exp) { found <- TRUE; break }
  }
  if (found) n_in_range <- n_in_range + 1
  else n_not_in_range <- n_not_in_range + 1
}
cat(sprintf("  Of first 50: %d in ranges, %d NOT in any range\n",
            n_in_range, n_not_in_range))

# Overlap analysis: codes in both urbano AND censobr_rural
censobr_rural_test <- censobr_test[!censobr_test %in% urb_codes]
cat(sprintf("\nCensobr rural codes (not in urbano): %d\n", length(censobr_rural_test)))
cat(sprintf("Rural individual codes: %d\n", length(unique(rur_indiv_codes))))
cat(sprintf("Overlap urb × censobr_rural: %d\n",
            length(intersect(urb_codes, censobr_rural_test))))
