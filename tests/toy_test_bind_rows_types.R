library(sf)
library(dplyr)

cat("=== Toy test: bind_rows with mixed code_tract types ===\n")

poly <- sf::st_polygon(list(matrix(c(0,0, 1,0, 1,1, 0,1, 0,0), ncol=2, byrow=TRUE)))

# rur_indiv has code_tract as numeric (from harmonize_geobr)
rur_indiv <- sf::st_sf(
  code_tract = c(110002305000001, 110002305000002),
  geom_source = c("rural", "rural"),
  geometry = sf::st_sfc(poly, poly, crs = 4674)
)

# partitioned_sf has code_tract as character (from partition_range_combined)
partitioned <- sf::st_sf(
  code_tract = c("110002305000003", "110002305000004"),
  geom_source = c("compat_partitioned", "compat_partitioned"),
  geometry = sf::st_sfc(poly, poly, crs = 4674)
)

cat("rur_indiv code_tract class:", class(rur_indiv$code_tract), "\n")
cat("partitioned code_tract class:", class(partitioned$code_tract), "\n")

# This should fail:
tryCatch({
  combined <- dplyr::bind_rows(rur_indiv, partitioned)
  cat("bind_rows without fix: OK (unexpected)\n")
}, error = function(e) {
  cat("bind_rows without fix: FAILED as expected:", conditionMessage(e), "\n")
})

# Fix: convert to numeric before bind_rows
partitioned$code_tract <- as.numeric(partitioned$code_tract)
cat("After fix - partitioned code_tract class:", class(partitioned$code_tract), "\n")

combined <- dplyr::bind_rows(rur_indiv, partitioned)
cat("bind_rows with fix: OK, nrow =", nrow(combined), "\n")
stopifnot(nrow(combined) == 4)
stopifnot(is.numeric(combined$code_tract))

cat("PASS: bind_rows type mismatch fixed\n")
