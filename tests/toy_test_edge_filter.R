library(dplyr)

cat("=== Toy test Fix A: filter edges without range notation in A.nome ===\n")

# Simulate compat edges like the real CompatMalhas output
edges <- data.frame(
  A.nome = c("110002305000032", "110002305000039",
             "110002305000001-0074",   # RANGE NOTATION — bad!
             "110002305000001-0074",   # RANGE NOTATION — bad!
             "110002305000073"),
  B.nome = c("110002305000032", "110002305000039",
             "110002305000080", "110002305000099",
             "110002305000079"),
  metodo = c("Manutencao", "Manutencao",
             "Sobreposicao (-50m)", "Divisao",
             "Divisao"),
  stringsAsFactors = FALSE
)

# Simulate weight map
compat_weight_map <- c(
  "Manutencao" = 1.0, "Divisao" = 0.8,
  "Sobreposicao (-50m)" = 0.5, "Sobreposicao (-20m)" = 0.4
)
edges$weight <- compat_weight_map[edges$metodo]

cat("All edges:\n")
print(edges)

# Scenario: 2010 tract "110002305000080" is unmatched
id_2010 <- "110002305000080"

# BEFORE fix: lookup ALL edges for this B.nome
hits_before <- edges[edges$B.nome == id_2010, ]
cat("\nBEFORE fix — hits for", id_2010, ":\n")
print(hits_before)
cat("Best A.nome:", hits_before$A.nome[which.max(hits_before$weight)], "\n")
# Returns "110002305000001-0074" — RANGE NOTATION! as.numeric will fail

# AFTER fix: exclude edges where A.nome has range notation
hits_after <- edges[edges$B.nome == id_2010 & !grepl("-", edges$A.nome), ]
cat("\nAFTER fix — hits for", id_2010, ":\n")
print(hits_after)

if (nrow(hits_after) > 0) {
  cat("Best A.nome:", hits_after$A.nome[which.max(hits_after$weight)], "\n")
} else {
  cat("No valid edges — partition becomes 'unmatched'\n")
}

# Verify: no range notation in results
stopifnot(nrow(hits_before) > 0)  # before had results
stopifnot(any(grepl("-", hits_before$A.nome)))  # before had range notation
stopifnot(!any(grepl("-", hits_after$A.nome)))  # after has no range notation

# Test another scenario: 2010 tract with only individual edges
id_2010_b <- "110002305000079"
hits_b <- edges[edges$B.nome == id_2010_b & !grepl("-", edges$A.nome), ]
cat("\nHits for", id_2010_b, "(should find individual code):\n")
print(hits_b)
stopifnot(nrow(hits_b) == 1)
stopifnot(hits_b$A.nome == "110002305000073")

cat("\nPASS: edge filter correctly excludes range notation from A.nome\n")
