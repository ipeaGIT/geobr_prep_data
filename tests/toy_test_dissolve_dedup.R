library(sf)
library(dplyr)

cat("=== Toy test: dissolve duplicates + filter unmatched ===\n")

poly1 <- sf::st_polygon(list(matrix(c(0,0, 3,0, 3,3, 0,3, 0,0), ncol=2, byrow=TRUE)))
poly2 <- sf::st_polygon(list(matrix(c(3,0, 6,0, 6,3, 3,3, 3,0), ncol=2, byrow=TRUE)))
poly3 <- sf::st_polygon(list(matrix(c(0,3, 3,3, 3,6, 0,6, 0,3), ncol=2, byrow=TRUE)))
poly4 <- sf::st_polygon(list(matrix(c(6,0, 9,0, 9,3, 6,3, 6,0), ncol=2, byrow=TRUE)))

# Simulate partitioned_sf with problems:
# - code_tract 001: appears 2 times (duplicate from graph_divisao)
# - code_tract NA: unmatched partition (2010 code with no 2000 match)
# - code_tract 002: appears once (ok)
partitioned_sf <- sf::st_sf(
  code_tract = c(1, 1, NA, 2),
  match_method = c("code_match", "graph_divisao", "graph_sobreposicao", "code_match"),
  match_confidence = c(1.0, 0.8, 0.3, 1.0),
  geom_source = rep("compat_partitioned", 4),
  original_id = rep("000000000000001-0002", 4),
  geometry = sf::st_sfc(poly1, poly2, poly3, poly4, crs = 4674)
)

cat("BEFORE fix:\n")
cat("  N rows:", nrow(partitioned_sf), "\n")
cat("  NAs:", sum(is.na(partitioned_sf$code_tract)), "\n")
cat("  Duplicates:", sum(duplicated(partitioned_sf$code_tract[!is.na(partitioned_sf$code_tract)])), "\n")

# Fix 1: Remove unmatched (code_tract NA)
partitioned_sf <- partitioned_sf |>
  dplyr::filter(!is.na(code_tract))

cat("\nAfter removing NAs:\n")
cat("  N rows:", nrow(partitioned_sf), "\n")

# Fix 2: Dissolve duplicates by code_tract (union geometry, keep best metadata)
partitioned_sf <- partitioned_sf |>
  dplyr::group_by(code_tract) |>
  dplyr::summarise(
    geometry = sf::st_union(geometry),
    match_method = dplyr::first(match_method),
    match_confidence = min(match_confidence),
    geom_source = dplyr::first(geom_source),
    original_id = dplyr::first(original_id),
    .groups = "drop"
  ) |>
  sf::st_make_valid() |>
  sf::st_cast("MULTIPOLYGON")

cat("\nAfter dissolve:\n")
cat("  N rows:", nrow(partitioned_sf), "\n")
cat("  Duplicates:", sum(duplicated(partitioned_sf$code_tract)), "\n")
cat("  code_tract:", partitioned_sf$code_tract, "\n")

# Verify
stopifnot(nrow(partitioned_sf) == 2)  # code_tract 1 and 2
stopifnot(sum(is.na(partitioned_sf$code_tract)) == 0)
stopifnot(sum(duplicated(partitioned_sf$code_tract)) == 0)
stopifnot(all(sf::st_is_valid(partitioned_sf)))

# Check that dissolved geometry is larger than individual
area_dissolved <- as.numeric(sf::st_area(partitioned_sf[partitioned_sf$code_tract == 1, ]))
cat("  Area of dissolved tract 1:", area_dissolved, "(should be ~18 = 9+9)\n")
stopifnot(area_dissolved > 15)  # approximately 2 * 3x3 = 18

cat("\nPASS: dissolve + dedup works correctly\n")
