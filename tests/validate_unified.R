library(sf)
library(dplyr)
library(arrow)
library(geoarrow)
setwd("d:/Dropbox/Artigos/geobr_prep_data")

cat("=== Validating unified parquet ===\n")

f <- "data/census_tract/2000/census_tracts_2000.parquet"
df <- arrow::read_parquet(f)
sfc <- sf::st_as_sfc(df$geometry)
sf::st_crs(sfc) <- 4674
df_plain <- as.data.frame(df)
df_plain$geometry <- NULL
uni <- sf::st_sf(df_plain, geometry = sfc)

cat("N rows:", nrow(uni), "\n")
cat("CRS:", sf::st_crs(uni)$epsg, "\n")
cat("Columns:", paste(names(uni), collapse=", "), "\n\n")

cat("geom_source:\n")
print(table(uni$geom_source, useNA = "ifany"))

cat("\nmatch_method:\n")
print(table(uni$match_method, useNA = "ifany"))

cat("\noriginal_id non-NA:", sum(!is.na(uni$original_id)), "\n")
cat("original_id sample:\n")
print(head(uni$original_id[!is.na(uni$original_id)], 5))

cat("\ncode_tract class:", class(uni$code_tract), "\n")
cat("code_tract NAs:", sum(is.na(uni$code_tract)), "\n")
cat("code_tract duplicates:", sum(duplicated(uni$code_tract)), "\n")

cat("\nGeometry types:\n")
print(table(sf::st_geometry_type(uni)))

cat("\nLast column:", names(uni)[ncol(uni)], "\n")

# Checks
stopifnot(sf::st_crs(uni)$epsg == 4674)
stopifnot(names(uni)[ncol(uni)] == "geometry")
stopifnot(is.numeric(uni$code_tract))
stopifnot(is.numeric(uni$code_state))
stopifnot(sum(!is.na(uni$original_id)) > 0)

cat("\nPASS: all validations OK\n")
