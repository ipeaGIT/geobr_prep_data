library(sf)
library(dplyr)
library(arrow)
library(geoarrow)
setwd("d:/Dropbox/Artigos/geobr_prep_data")

cat("=== Diagnóstico unified census_tract 2000 ===\n\n")

# Read unified
f <- "data/census_tract/2000/census_tracts_2000.parquet"
df <- arrow::read_parquet(f)
sfc <- sf::st_as_sfc(df$geometry)
sf::st_crs(sfc) <- 4674
df_plain <- as.data.frame(df)
df_plain$geometry <- NULL
uni <- sf::st_sf(df_plain, geometry = sfc)

cat("=== 1. Contagem total ===\n")
cat("N total unified:", nrow(uni), "\n")
cat("N censobr 2000 esperado: 215.811\n")
cat("Diferença:", nrow(uni) - 215811, "features a mais\n\n")

cat("=== 2. geom_source breakdown ===\n")
print(table(uni$geom_source, useNA = "ifany"))
cat("\n")

cat("=== 3. code_tract analysis ===\n")
cat("NAs:", sum(is.na(uni$code_tract)), "\n")
cat("Unique non-NA:", length(unique(uni$code_tract[!is.na(uni$code_tract)])), "\n")
cat("Total non-NA:", sum(!is.na(uni$code_tract)), "\n")
cat("Duplicates (non-NA):", sum(duplicated(uni$code_tract[!is.na(uni$code_tract)])), "\n\n")

cat("=== 4. Duplicates by geom_source ===\n")
dup_tracts <- uni$code_tract[!is.na(uni$code_tract) & duplicated(uni$code_tract)]
dup_rows <- uni[!is.na(uni$code_tract) & uni$code_tract %in% dup_tracts, ]
cat("Rows involved in duplicates:", nrow(dup_rows), "\n")
print(table(dup_rows$geom_source))
cat("\n")

cat("=== 5. NAs by geom_source ===\n")
na_rows <- uni[is.na(uni$code_tract), ]
cat("NA rows:", nrow(na_rows), "\n")
print(table(na_rows$geom_source))
cat("match_method of NA rows:\n")
print(table(na_rows$match_method, useNA = "ifany"))
cat("\n")

cat("=== 6. compat_partitioned details ===\n")
compat <- uni[uni$geom_source == "compat_partitioned", ]
cat("N compat_partitioned:", nrow(compat), "\n")
cat("Unique code_tract (non-NA):", length(unique(compat$code_tract[!is.na(compat$code_tract)])), "\n")
cat("code_tract NAs:", sum(is.na(compat$code_tract)), "\n")
cat("match_method:\n")
print(table(compat$match_method, useNA = "ifany"))
cat("\n")
cat("Unique original_id:", length(unique(compat$original_id)), "\n")
cat("N partitions per original_id (summary):\n")
parts_per_range <- table(compat$original_id)
print(summary(as.numeric(parts_per_range)))
cat("\n")

cat("=== 7. Expected count ===\n")
# Each range should expand to N tracts, but partition creates as many
# pieces as there are 2010 tracts intersecting the range
# The EXPECTED number = rural_individual + urbano + (one row per expanded code in each range)
rur_f <- "data/census_tract/2000/census_tracts_2000_rural.parquet"
rdf <- arrow::read_parquet(rur_f)
cat("Rural parquet total rows:", nrow(rdf), "\n")
cat("Rural n_tracts_inside (sum, non-NA):", sum(rdf$n_tracts_inside, na.rm = TRUE), "\n")
cat("Rural aggregated rows:", sum(rdf$geom_source == "rural_aggregated"), "\n")
cat("Rural individual rows:", sum(rdf$geom_source == "rural"), "\n\n")

cat("=== 8. What the count SHOULD be ===\n")
# censobr has 215.811 unique tracts
# Unified should have at most 215.811 rows (1 per tract)
# But compat_partitioned can produce MULTIPLE geometry pieces per tract
# (when a 2010 tract intersects a range in multiple pieces)
cat("If each code_tract appeared exactly once: ~215.811 rows\n")
cat("Actual: 236.174 rows\n")
cat("Excess:", 236174 - 215811, "= extra rows from partitioning\n")
cat("These are 2010 tracts that intersect a range but don't map 1:1 to a 2000 code\n")
