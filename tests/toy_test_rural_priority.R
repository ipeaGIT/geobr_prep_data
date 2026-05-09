library(sf)
library(dplyr)

cat("=== Toy test: rural > compat priority in merge ===\n")

poly1 <- sf::st_polygon(list(matrix(c(0,0, 5,0, 5,5, 0,5, 0,0), ncol=2, byrow=TRUE)))
poly2 <- sf::st_polygon(list(matrix(c(1,1, 4,1, 4,4, 1,4, 1,1), ncol=2, byrow=TRUE)))

# Rural individual: code_tract 001 with original IBGE geometry
rur_indiv <- sf::st_sf(
  code_tract = c(1, 2),
  geom_source = c("rural", "rural"),
  geometry = sf::st_sfc(poly1, poly1, crs = 4674)
)

# Compat partitioned: code_tract 001 also appears (from range partition)
# plus code_tract 003 which is genuinely new
compat <- sf::st_sf(
  code_tract = c(1, 3),
  geom_source = c("compat_partitioned", "compat_partitioned"),
  geometry = sf::st_sfc(poly2, poly2, crs = 4674)
)

cat("Before priority filter:\n")
combined <- dplyr::bind_rows(rur_indiv, compat)
cat("  N rows:", nrow(combined), "\n")
cat("  code_tract 1 appears:", sum(combined$code_tract == 1), "times\n")

# Fix: remove compat rows that duplicate a rural code_tract
compat_filtered <- compat |>
  dplyr::filter(!code_tract %in% rur_indiv$code_tract)

combined_fixed <- dplyr::bind_rows(rur_indiv, compat_filtered)
cat("\nAfter priority filter:\n")
cat("  N rows:", nrow(combined_fixed), "\n")
cat("  code_tract 1 appears:", sum(combined_fixed$code_tract == 1), "times\n")
cat("  code_tract 1 geom_source:", combined_fixed$geom_source[combined_fixed$code_tract == 1], "\n")

# Verify
stopifnot(nrow(combined_fixed) == 3)  # 1(rural), 2(rural), 3(compat)
stopifnot(sum(combined_fixed$code_tract == 1) == 1)
stopifnot(combined_fixed$geom_source[combined_fixed$code_tract == 1] == "rural")

cat("\nPASS: rural > compat priority works\n")
