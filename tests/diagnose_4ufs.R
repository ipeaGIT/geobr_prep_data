library(sf)
library(dplyr)
library(arrow)
library(geoarrow)
setwd("d:/Dropbox/Artigos/geobr_prep_data")
source("R/support_fun.R")
source("R/support_harmonize_geobr.R")
source("R/census_tract.R")

test_ufs <- c("11", "16", "27", "42")

cat("=== Diagnóstico detalhado 4 UFs ===\n\n")

# Load censobr
censobr_all <- arrow::read_parquet("data-raw/census_tract/2000/censobr_tracts_2000.parquet",
                                    col_select = "code_tract", as_data_frame = TRUE)
censobr_all$code_tract <- as.character(censobr_all$code_tract)
censobr_test <- censobr_all$code_tract[substr(censobr_all$code_tract, 1, 2) %in% test_ufs]
cat("Censobr for 4 UFs:", length(censobr_test), "unique tracts\n")

# Load rural
rur_clean <- read_geoparquet("data/census_tract/2000/census_tracts_2000_rural.parquet")
rur_test <- rur_clean[substr(as.character(rur_clean$code_state), 1, 2) %in% test_ufs, ]
rur_agg <- dplyr::filter(rur_test, geom_source == "rural_aggregated")

cat("Rural aggregated ranges in 4 UFs:", nrow(rur_agg), "\n\n")

# For each range, check expand_range vs censobr
cat("=== Range expansion vs censobr ===\n")
total_expanded <- 0
total_valid <- 0
total_ranges <- 0

for (i in seq_len(nrow(rur_agg))) {
  range_code <- rur_agg$original_id[i]
  if (is.na(range_code)) next
  total_ranges <- total_ranges + 1

  expanded <- expand_range(range_code)
  valid <- expanded[expanded %in% censobr_all$code_tract]
  total_expanded <- total_expanded + length(expanded)
  total_valid <- total_valid + length(valid)

  if (length(valid) < length(expanded) && i <= 5) {
    cat(sprintf("  Range %s: %d expanded, %d in censobr (missing %d)\n",
                range_code, length(expanded), length(valid),
                length(expanded) - length(valid)))
  }
}
cat(sprintf("\nTotal: %d ranges, %d expanded codes, %d in censobr (%.1f%% hit rate)\n",
            total_ranges, total_expanded, total_valid,
            100 * total_valid / total_expanded))

# Check: how many censobr tracts are in ranges vs individual
censobr_rural <- censobr_test[!censobr_test %in% as.character(
  rur_test$code_tract[rur_test$geom_source == "rural"])]
cat(sprintf("\nCensobr tracts NOT in rural individual: %d\n", length(censobr_rural)))

# Check edge structure for RO to understand graph matches
cat("\n=== Edge analysis for RO (UF 11) ===\n")
compat_11 <- read_compat_uf("11", "data-raw/census_tract/compat")
cat("Total edges:", nrow(compat_11$edges), "\n")
cat("Edges with range in A.nome:", sum(grepl("-", compat_11$edges$A.nome)), "\n")
cat("Edges WITHOUT range in A.nome:", sum(!grepl("-", compat_11$edges$A.nome)), "\n")

# What metodo values do range edges have?
range_edges <- compat_11$edges[grepl("-", compat_11$edges$A.nome), ]
cat("Range edge metodos:\n")
print(table(range_edges$metodo))

# What metodo values do non-range edges have?
nonrange_edges <- compat_11$edges[!grepl("-", compat_11$edges$A.nome), ]
cat("\nNon-range edge metodos:\n")
print(table(nonrange_edges$metodo))

# Check: B.nome values in range edges — are they also in non-range edges?
b_in_range <- unique(range_edges$B.nome)
b_in_nonrange <- unique(nonrange_edges$B.nome)
b_only_in_range <- setdiff(b_in_range, b_in_nonrange)
cat(sprintf("\n2010 tracts ONLY reachable via range edges: %d of %d\n",
            length(b_only_in_range), length(b_in_range)))
cat("These tracts lose their graph match after Fix A!\n")
