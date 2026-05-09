# Toy test: normalize_metro_2001_2003 deve aplicar prefix "RM " em 2001, 2002 E 2003
# Bug histórico: o helper só aplicava em 2001, deixando 2002 e 2003 inconsistentes
suppressMessages(library(dplyr))
suppressMessages(library(tidyr))

source("R/metro_area.R")

# Construir raw sintetico mimetizando 2002 (NOME DA REGIÃO MET. + sem prefix)
raw_2002 <- data.frame(
  `CÓDIGO`                = c(1, NA, 2, NA),
  `NOME DA REGIÃO MET.`   = c("Baixada Santista", NA, "RIDE Petrolina/Juazeiro", NA),
  `CÓDIGO_DO_MUNICÍPIO`   = c(3504107, 3548500, 2611101, 2906857),
  `LEGISLAÇÃO`            = c("LC 815", NA, "LC 113", NA),
  `DATA DA LEI`           = c("30.07.1996", NA, "21.09.2001", NA),
  check.names = FALSE,
  stringsAsFactors = FALSE
)

result <- normalize_metro_2001_2003(raw_2002, year = 2002)

stopifnot(nrow(result) == 4)
# Baixada Santista deve receber prefix RM
stopifnot(any(grepl("^RM Baixada Santista", result$name_metro)))
# RIDE NÃO deve receber prefix
stopifnot(!any(grepl("^RM RIDE", result$name_metro)))
# code_muni numerico
stopifnot(is.numeric(result$code_muni))

# Repetir para 2003
result_2003 <- normalize_metro_2001_2003(raw_2002, year = 2003)
stopifnot(any(grepl("^RM Baixada Santista", result_2003$name_metro)))

# Para 2005, NAO deve aplicar (helper diferente cuida disso)
# (sanity-check: helper só cobre 2001-2003)

cat("PASS: toy_normalize_metro_2001_2003_prefix\n")
