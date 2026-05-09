library(sf)
library(dplyr)

cat("=== Toy test: new code-first partition_range_combined ===\n")
sf::sf_use_s2(FALSE)

# === Setup: simulate a range with 5 valid_codes ===

# Range polygon (10x10 square)
range_poly <- sf::st_sf(
  geometry = sf::st_sfc(
    sf::st_polygon(list(matrix(c(0,0, 10,0, 10,10, 0,10, 0,0), ncol=2, byrow=TRUE))),
    crs = 4674
  )
)

# 5 valid codes from censobr (001-005, but 003 doesn't exist → 4 valid)
valid_codes <- c("110002305000001", "110002305000002",
                 "110002305000004", "110002305000005")
range_id <- "110002305000001-0005"

# 2010 tracts that intersect the range:
# - tract 001: same code as 2000 (direct match, Strategy A)
# - tract 002: same code (direct match, Strategy A)
# - tract 007: NEW tract, graph says it corresponds to 2000 code 004 (Strategy B)
# - tract 008: NEW tract, only reachable via range edge (Strategy C)
# - tract 009: NEW tract, no edge at all → not in pool
t2010 <- sf::st_sf(
  code_tract = c("110002305000001", "110002305000002",
                 "110002305000007", "110002305000008"),
  geometry = sf::st_sfc(
    sf::st_polygon(list(matrix(c(0,0, 4,0, 4,4, 0,4, 0,0), ncol=2, byrow=TRUE))),     # tract 001
    sf::st_polygon(list(matrix(c(4,0, 8,0, 8,4, 4,4, 4,0), ncol=2, byrow=TRUE))),     # tract 002
    sf::st_polygon(list(matrix(c(0,5, 5,5, 5,9, 0,9, 0,5), ncol=2, byrow=TRUE))),     # tract 007
    sf::st_polygon(list(matrix(c(5,5, 9,5, 9,9, 5,9, 5,5), ncol=2, byrow=TRUE))),     # tract 008
    crs = 4674
  )
)

# Simulate compat graph:
compat <- list(
  c2000 = data.frame(
    ID = c("110002305000001-0005", "110002305000001", "110002305000002",
           "110002305000004", "110002305000005"),
    CD_MCA = c("MCA1", "MCA1", "MCA1", "MCA1", "MCA1"),
    stringsAsFactors = FALSE
  ),
  c2010 = data.frame(
    ID = c("110002305000001", "110002305000002", "110002305000007", "110002305000008"),
    CD_MCA = c("MCA1", "MCA1", "MCA1", "MCA1"),
    stringsAsFactors = FALSE
  ),
  edges = data.frame(
    A.nome = c(
      "110002305000001", "110002305000002",   # individual maintenance
      "110002305000004",                       # individual → 007 (division)
      "110002305000001-0005",                  # range → 008 (overlap)
      "110002305000001-0005"                   # range → 007 (overlap, lower weight)
    ),
    B.nome = c(
      "110002305000001", "110002305000002",
      "110002305000007",
      "110002305000008",
      "110002305000007"
    ),
    metodo = c(
      "Manutencao", "Manutencao",
      "Divisao",
      "Sobreposicao (-50m)",
      "Sobreposicao (-50m)"
    ),
    stringsAsFactors = FALSE
  )
)

# ============================================
# NEW ALGORITHM (to be implemented in census_tract.R)
# ============================================

compat_weight_map <- c(
  "Manutencao" = 1.0, "Manutencao desassociada" = 0.9,
  "Divisao" = 0.8, "Sobreposicao (-50m)" = 0.5,
  "Sobreposicao (-20m)" = 0.4, "Sobreposicao (0m)" = 0.3
)

cat("\n--- Phase 1: Build geometry pool ---\n")

# Collect all B.nome from edges
range_edges <- compat$edges[compat$edges$A.nome == range_id, ]
b_from_range <- unique(range_edges$B.nome)
cat("B.nome from range edges:", b_from_range, "\n")

indiv_edges <- compat$edges[compat$edges$A.nome %in% valid_codes, ]
b_from_indiv <- unique(indiv_edges$B.nome)
cat("B.nome from individual edges:", b_from_indiv, "\n")

all_b <- unique(c(b_from_range, b_from_indiv))
cat("All B.nome in pool:", all_b, "\n")

# Get 2010 tracts and clip to range
pool <- t2010[t2010$code_tract %in% all_b, ]
cat("Pool 2010 tracts:", nrow(pool), "\n")

pool_clipped <- sf::st_intersection(pool, sf::st_geometry(range_poly))
pool_clipped <- sf::st_collection_extract(pool_clipped, "POLYGON")
pool_clipped <- sf::st_make_valid(pool_clipped)
pool_clipped$b_code <- as.character(pool_clipped$code_tract)
cat("Pool clipped:", nrow(pool_clipped), "fragments\n")

cat("\n--- Phase 2: Assign geometry per valid_code ---\n")

assignments <- data.frame(valid_code=character(), b_code=character(),
                          method=character(), confidence=numeric(),
                          stringsAsFactors=FALSE)

# Strategy A: direct match
for (vc in valid_codes) {
  if (vc %in% pool_clipped$b_code) {
    assignments <- rbind(assignments,
      data.frame(valid_code=vc, b_code=vc, method="direct_match",
                 confidence=1.0, stringsAsFactors=FALSE))
  }
}
cat("After Strategy A:", nrow(assignments), "assigned\n")
cat("  Assigned:", assignments$valid_code, "\n")

# Strategy B: individual graph edges
remaining <- setdiff(valid_codes, assignments$valid_code)
cat("Remaining after A:", remaining, "\n")

for (vc in remaining) {
  vc_edges <- indiv_edges[indiv_edges$A.nome == vc, ]
  if (nrow(vc_edges) > 0) {
    vc_edges$weight <- compat_weight_map[vc_edges$metodo]
    vc_edges$weight[is.na(vc_edges$weight)] <- 0.1
    available <- vc_edges[vc_edges$B.nome %in% pool_clipped$b_code, ]
    if (nrow(available) > 0) {
      best <- available[which.max(available$weight), ]
      assignments <- rbind(assignments,
        data.frame(valid_code=vc, b_code=best$B.nome,
                   method=paste0("graph_", tolower(gsub(" .*", "", best$metodo))),
                   confidence=best$weight, stringsAsFactors=FALSE))
    }
  }
}
cat("After Strategy B:", nrow(assignments), "assigned\n")
remaining <- setdiff(valid_codes, assignments$valid_code)
cat("  Remaining:", remaining, "\n")

# Strategy C: range edges + code proximity
if (length(remaining) > 0 && nrow(range_edges) > 0) {
  avail_b <- range_edges$B.nome[range_edges$B.nome %in% pool_clipped$b_code]
  # Don't exclude already-claimed b_codes — sharing is allowed
  if (length(avail_b) > 0) {
    nd <- 4  # suffix digits
    for (vc in remaining) {
      vc_num <- as.integer(substr(vc, nchar(vc) - nd + 1, nchar(vc)))
      b_nums <- as.integer(substr(avail_b, nchar(avail_b) - nd + 1, nchar(avail_b)))
      best_idx <- which.min(abs(b_nums - vc_num))
      assignments <- rbind(assignments,
        data.frame(valid_code=vc, b_code=avail_b[best_idx],
                   method="range_proximity", confidence=0.3,
                   stringsAsFactors=FALSE))
    }
  }
}
cat("After Strategy C:", nrow(assignments), "assigned\n")
remaining <- setdiff(valid_codes, assignments$valid_code)

# Strategy D: unresolved
for (vc in remaining) {
  assignments <- rbind(assignments,
    data.frame(valid_code=vc, b_code=NA_character_,
               method="unresolved", confidence=0.0,
               stringsAsFactors=FALSE))
}

cat("\nFinal assignments:\n")
print(assignments)

cat("\n--- Phase 3: Build geometry ---\n")

result_list <- list()
for (i in seq_len(nrow(assignments))) {
  vc <- assignments$valid_code[i]
  bc <- assignments$b_code[i]
  if (is.na(bc)) {
    # Unresolved: centroid seed
    result_list[[i]] <- sf::st_sf(
      code_tract = vc, match_method = "unresolved", match_confidence = 0.0,
      geometry = sf::st_centroid(sf::st_geometry(range_poly))
    )
  } else {
    frag <- pool_clipped[pool_clipped$b_code == bc, ]
    result_list[[i]] <- sf::st_sf(
      code_tract = vc,
      match_method = assignments$method[i],
      match_confidence = assignments$confidence[i],
      geometry = sf::st_union(sf::st_geometry(frag))
    )
  }
}
result <- do.call(rbind, result_list)

cat("Result:\n")
print(sf::st_drop_geometry(result))
cat("N rows:", nrow(result), "\n")
cat("N valid_codes:", length(valid_codes), "\n")

# === Verify ===
stopifnot(nrow(result) == length(valid_codes))
stopifnot(!any(is.na(result$code_tract)))
stopifnot(all(valid_codes %in% result$code_tract))

# Check strategies used
cat("\nStrategies used:\n")
print(table(result$match_method))

# Expected:
# 001 → direct_match (code 001 exists in 2010)
# 002 → direct_match (code 002 exists in 2010)
# 004 → graph_divisao (edge 004→007)
# 005 → range_proximity (no individual edge, nearest range B.nome)
stopifnot(result$match_method[result$code_tract == "110002305000001"] == "direct_match")
stopifnot(result$match_method[result$code_tract == "110002305000002"] == "direct_match")
stopifnot(result$match_method[result$code_tract == "110002305000004"] == "graph_divisao")
stopifnot(result$match_method[result$code_tract == "110002305000005"] == "range_proximity")

cat("\nPASS: all 4 strategies work correctly\n")
