library(sf)
library(dplyr)

cat("=== Toy test: metadata copy from sf row with as.vector ===\n")

# Simulate a range row (1-row sf, like rur_agg[i, ])
poly <- sf::st_polygon(list(matrix(c(0,0, 10,0, 10,10, 0,10, 0,0), ncol=2, byrow=TRUE)))
row <- sf::st_sf(
  code_tract = 110002305000001,
  code_muni = 1100023,
  name_muni = "Ariquemes",
  code_state = 11,
  abbrev_state = "RO",
  name_state = "Rondonia",
  code_region = 1,
  name_region = "Norte",
  year = 2000,
  zone = "Rural",
  original_id = "110002305000001-0074",
  geom_source = "rural_aggregated",
  geometry = sf::st_sfc(poly, crs = 4674)
)

# Simulate partitions (output of partition_range_combined)
p1 <- sf::st_polygon(list(matrix(c(1,1, 4,1, 4,4, 1,4, 1,1), ncol=2, byrow=TRUE)))
p2 <- sf::st_polygon(list(matrix(c(6,6, 9,6, 9,9, 6,9, 6,6), ncol=2, byrow=TRUE)))
parts <- sf::st_sf(
  code_tract = c("110002305000001", "110002305000002"),
  match_method = c("code_match", "graph_divisao"),
  match_confidence = c(1.0, 0.8),
  geometry = sf::st_sfc(p1, p2, crs = 4674)
)

# Copy metadata from row to parts (same logic as in unified)
for (col in c("code_muni", "name_muni", "code_state", "abbrev_state",
               "name_state", "code_region", "name_region", "year", "zone")) {
  if (col %in% names(row)) {
    val <- as.vector(row[[col]])
    cat(sprintf("  %s: class=%s, value=%s\n", col, class(val), as.character(val)))
    parts[[col]] <- val
  }
}
parts$geom_source <- "compat_partitioned"
parts$original_id <- "110002305000001-0074"

cat("\nResult:\n")
print(names(parts))
cat("N rows:", nrow(parts), "\n")
cat("code_muni:", parts$code_muni, "\n")
cat("name_muni:", parts$name_muni, "\n")
cat("original_id:", parts$original_id, "\n")

# Verify
stopifnot(nrow(parts) == 2)
stopifnot(all(parts$code_muni == 1100023))
stopifnot(all(parts$name_muni == "Ariquemes"))
stopifnot(all(parts$original_id == "110002305000001-0074"))
stopifnot(is.numeric(parts$code_muni))
stopifnot(is.character(parts$name_muni))

cat("PASS: metadata copy works correctly\n")
