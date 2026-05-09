library(sf)
library(dplyr)
library(stringi)

cat("=== Toy test: original_id survives use_encoding_utf8 ===\n")

# Simulate a minimal sf with original_id (range notation) and code_tract
poly <- sf::st_polygon(list(matrix(c(0,0, 1,0, 1,1, 0,1, 0,0), ncol=2, byrow=TRUE)))
test_sf <- sf::st_sf(
  code_tract = c(110002305000001, 110002305000002),
  code_muni = c(1100023, 1100023),
  original_id = c("110002305000001-0074", NA_character_),
  geom_source = c("rural_aggregated", "rural"),
  geometry = sf::st_sfc(poly, poly, crs = 4674)
)

cat("BEFORE use_encoding_utf8:\n")
cat("  original_id:", test_sf$original_id, "\n")
cat("  code_tract:", test_sf$code_tract, "\n")

# Apply the exact same transformation as use_encoding_utf8 line 290
test_sf <- test_sf |>
  dplyr::mutate_at(dplyr::vars(dplyr::starts_with("code_")), .funs = function(x){ as.numeric(x) })

cat("AFTER mutate_at(starts_with('code_'), as.numeric):\n")
cat("  original_id:", test_sf$original_id, "\n")
cat("  code_tract:", test_sf$code_tract, "\n")

# Verify
stopifnot(!is.na(test_sf$original_id[1]))
stopifnot(test_sf$original_id[1] == "110002305000001-0074")
stopifnot(is.na(test_sf$original_id[2]))
stopifnot(is.numeric(test_sf$code_tract))
stopifnot(is.numeric(test_sf$code_muni))

cat("PASS: original_id survives use_encoding_utf8\n")
