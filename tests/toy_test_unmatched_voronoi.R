library(sf)
library(dplyr)

cat("=== Toy test Fix C: unmatched partitions absorbed by Voronoi ===\n")

sf::sf_use_s2(FALSE)

# Simulate: 3 partitions inside a range, 1 is unmatched
range_poly <- sf::st_polygon(list(matrix(
  c(0,0, 10,0, 10,10, 0,10, 0,0), ncol=2, byrow=TRUE)))
range_sf <- sf::st_sf(geometry = sf::st_sfc(range_poly, crs = 4674))

p1 <- sf::st_polygon(list(matrix(c(1,1, 3,1, 3,3, 1,3, 1,1), ncol=2, byrow=TRUE)))
p2 <- sf::st_polygon(list(matrix(c(5,5, 8,5, 8,8, 5,8, 5,5), ncol=2, byrow=TRUE)))
p3 <- sf::st_polygon(list(matrix(c(1,6, 3,6, 3,9, 1,9, 1,6), ncol=2, byrow=TRUE)))  # unmatched

parts <- sf::st_sf(
  code_tract = c("001", "002", NA),   # 3rd is unmatched (NA)
  match_method = c("code_match", "graph_divisao", "unmatched"),
  match_confidence = c(1.0, 0.8, 0.0),
  geometry = sf::st_sfc(p1, p2, p3, crs = 4674)
)

cat("BEFORE filtering unmatched:\n")
cat("  N partitions:", nrow(parts), "\n")
cat("  code_tract:", parts$code_tract, "\n")

# Area of all 3 partitions
area_all_3 <- sum(as.numeric(sf::st_area(parts)))
cat("  Total partition area:", area_all_3, "\n")

# STEP 1: Filter out unmatched BEFORE Voronoi
parts_matched <- parts[!is.na(parts$code_tract), ]
cat("\nAFTER filtering unmatched:\n")
cat("  N matched:", nrow(parts_matched), "\n")

# STEP 2: Voronoi (simplified version of fill_gaps_voronoi)
centroids <- sf::st_centroid(sf::st_geometry(parts_matched))
pts_union <- sf::st_union(centroids)
voronoi_raw <- sf::st_voronoi(pts_union, envelope = sf::st_geometry(range_sf))
voronoi_polys <- sf::st_collection_extract(voronoi_raw, "POLYGON")
voronoi_clipped <- sf::st_intersection(voronoi_polys, sf::st_geometry(range_sf))
voronoi_clipped <- sf::st_collection_extract(voronoi_clipped, "POLYGON")
nearest_idx <- sf::st_nearest_feature(sf::st_centroid(voronoi_clipped), centroids)
result <- parts_matched[nearest_idx, ]
sf::st_geometry(result) <- voronoi_clipped
result <- result |>
  dplyr::group_by(code_tract, match_method, match_confidence) |>
  dplyr::summarise(geometry = sf::st_union(geometry), .groups = "drop") |>
  sf::st_make_valid()

cat("\nAFTER Voronoi:\n")
cat("  N features:", nrow(result), "\n")
cat("  code_tract:", result$code_tract, "\n")
area_voronoi <- sum(as.numeric(sf::st_area(result)))
area_range <- as.numeric(sf::st_area(range_sf))
coverage <- area_voronoi / area_range
cat("  Voronoi coverage:", round(coverage * 100, 1), "% of range area\n")

# Verify
stopifnot(nrow(result) == 2)  # only matched tracts
stopifnot(all(result$code_tract %in% c("001", "002")))
stopifnot(coverage > 0.99)  # ~100% coverage (within rounding)
stopifnot(!any(is.na(result$code_tract)))

cat("\nPASS: unmatched absorbed by Voronoi, 100% coverage maintained\n")
