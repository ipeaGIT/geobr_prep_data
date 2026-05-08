# Toy test: normalize_metro_2008_2009 com schema 4-col + footnotes filter
suppressMessages(library(dplyr))
suppressMessages(library(tidyr))

source("R/metro_area.R")

# 2008: linha 0 e metadata header (skip), 1 footer
raw_2008 <- data.frame(
  v1 = c("HEADER METADATA", "RM Manaus (1)", NA, NA, "Organização: IBGE"),
  v2 = c("X", NA, "subdiv1", "subdiv1", NA),
  v3 = c("Y", 1300144, 1302603, 1302900, NA),
  v4 = c("Z", "Iranduba", "Manaus", "Novo Airão", NA),
  stringsAsFactors = FALSE
)

result_2008 <- normalize_metro_2008_2009(raw_2008, year = 2008)

# Após skip linha 1 e remover footer (sem code_muni numerico): 3 linhas
stopifnot(nrow(result_2008) == 3)
# Forward-fill name_metro: linhas 2 e 3 devem ter "RM Manaus" (sufixo "(1)" removido)
stopifnot(all(result_2008$name_metro == "RM Manaus"))
stopifnot(is.numeric(result_2008$code_muni))
stopifnot(!any(is.na(result_2008$code_muni)))

# 2009: NÃO skip linha 1 (já é dado); footer com mais notas
raw_2009 <- data.frame(
  v1 = c("RM Manaus", NA, NA, "Nota (1):", "Nota (2):"),
  v2 = c(NA, NA, NA, NA, NA),
  v3 = c(1300144, 1302603, 1302900, NA, NA),
  v4 = c("Iranduba", "Manaus", "Novo Airão", NA, NA),
  stringsAsFactors = FALSE
)

result_2009 <- normalize_metro_2008_2009(raw_2009, year = 2009)
stopifnot(nrow(result_2009) == 3)
stopifnot(all(result_2009$name_metro == "RM Manaus"))

cat("PASS: toy_normalize_metro_2008_2009\n")
