# Toy test: normalize_metro_2021_2024 com schema 16-col novo
# Inclui validacao do mapeamento correto de `type` (LABEL_CATMETROPOL,
# nao LABEL_RECMETROPOL): "type" deve conter a CATEGORIA (Regiao Metropolitana,
# RIDE, Aglomeracao Urbana), nao o NOME do recorte (Manaus (AM) etc.).
suppressMessages(library(dplyr))
suppressMessages(library(tidyr))

source("R/metro_area.R")

# Schema sintetico mimetizando 2021-2024 (16 cols)
raw_2021 <- data.frame(
  GRANDE_REGIAO        = c("Norte", "Norte"),
  COD_UF               = c(13, 13),
  SIGLA_UF             = c("AM", "AM"),
  COD_RECMETROPOL      = c(1300001, 1300001),
  NOME_RECMETROPOL     = c("Manaus", "Manaus"),
  LABEL_RECMETROPOL    = c("Manaus (AM)", "Manaus (AM)"),
  COD_CATMETROPOL      = c(1, 1),
  NOME_CATMETROPOL     = c("RM", "RM"),
  LABEL_CATMETROPOL    = c("Região Metropolitana", "Região Metropolitana"),
  COD_SUBCATMETROPOL   = c(1, 1),
  NOME_SUBCATMETROPOL  = c("Núcleo", "Núcleo"),
  LABEL_SUBCATMETROPOL = c("Núcleo Metropolitano", "Núcleo Metropolitano"),
  COD_MUN              = c(1302603, 1303106),
  NOME_MUN             = c("Manaus", "Itacoatiara"),
  LEG                  = c("LC 52", "LC 52"),
  DATA                 = c("30.05.2007", "30.05.2007"),
  stringsAsFactors = FALSE
)

result <- normalize_metro_2021_2024(raw_2021, year = 2021)

stopifnot(nrow(result) == 2)
stopifnot(all(c("name_metro", "code_muni") %in% names(result)))
stopifnot(result$name_metro[1] == "Manaus")
stopifnot(result$code_muni[1] == 1302603)
stopifnot(is.numeric(result$code_muni))
stopifnot(is.character(result$legislation_date))
stopifnot(result$legislation_date[1] == "30.05.2007")

# CRITICO: type deve mapear LABEL_CATMETROPOL (categoria), nao LABEL_RECMETROPOL (nome)
stopifnot("type" %in% names(result))
stopifnot(result$type[1] == "Região Metropolitana")
stopifnot(result$type[1] != "Manaus (AM)")  # rejeita o bug antigo

# Subdivision deve mapear de LABEL_SUBCATMETROPOL
stopifnot("subdivision" %in% names(result))
stopifnot(result$subdivision[1] == "Núcleo Metropolitano")

# Schema mismatch deve fazer stop()
raw_bad <- raw_2021[, -which(names(raw_2021) == "NOME_RECMETROPOL")]
err <- tryCatch(normalize_metro_2021_2024(raw_bad, 2021),
                error = function(e) e$message)
stopifnot(grepl("Schema mismatch", err))

cat("PASS: toy_normalize_metro_2021_2024\n")
