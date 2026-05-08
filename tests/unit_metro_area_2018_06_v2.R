# Unit test: 2018_06_v2 deve usar normalize_metro_2016_2018 (schema 9-col UPPERCASE),
# NAO normalize_metro_2018_2020 (schema 12-col existe só em 2018_12_v2 que NÃO é nosso target).
# O snapshot 06/2018 mantém o schema antigo de 2016-2017.
suppressMessages(library(dplyr))
suppressMessages(library(tidyr))
suppressMessages(library(readxl))

source("R/metro_area.R")

# Baixar 2018_06_v2 do FTP (real, mas pequeno: ~91 KB)
url_2018 <- .metro_area_urls()[["2018"]]
tmp <- tempfile(fileext = ".xlsx")

dl_ok <- tryCatch({
  httr::GET(url_2018, httr::write_disk(tmp, overwrite = TRUE), httr::timeout(120))
  TRUE
}, error = function(e) {
  cat("SKIP: download falhou (offline?):", e$message, "\n")
  FALSE
})

if (dl_ok && file.exists(tmp) && file.info(tmp)$size > 1000) {
  raw <- readxl::read_excel(tmp)

  # Schema esperado para 2018_06_v2: 9 cols UPPERCASE
  expected_cols <- c("COD_UF", "SIGLA_UF", "NOME_RM", "SUBDIVISAO",
                     "COD_MUN", "NOME_MUN", "LEG", "DATA", "TIPO")
  stopifnot(all(expected_cols %in% names(raw)))

  # Aplica helper 2016-2018 (correto)
  result <- normalize_metro_2016_2018(raw)

  # Validar
  stopifnot(nrow(result) > 1000)  # ~1402 esperado
  stopifnot(length(unique(result$name_metro)) > 70)  # ~81 esperado
  stopifnot(is.numeric(result$code_muni))
  stopifnot(!any(is.na(result$code_muni)))

  cat("PASS: unit_metro_area_2018_06_v2 (nrow=", nrow(result),
      "n_rms=", length(unique(result$name_metro)), ")\n")
} else {
  cat("SKIP: unit_metro_area_2018_06_v2 (download indisponivel)\n")
}
