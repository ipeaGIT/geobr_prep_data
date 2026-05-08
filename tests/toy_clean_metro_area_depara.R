# Toy test: clean_metro_area_depara combina 4 sheets num parquet unico.
# Usa dataframes sinteticos mimetizando a estrutura de cada sheet.
suppressMessages({
  library(dplyr); library(tidyr); library(readxl); library(openxlsx)
})

source("R/metro_area.R")

# Criar XLSX sintetico em /tmp com as 4 sheets do DePara
tmp_xlsx <- tempfile(fileext = ".xlsx")

# Sheet 1: SIDRA_DePara_CategMetropol (8 cols)
# Row 1 = header manual; row 2+ = dados
s1 <- data.frame(
  V1 = c("Cod",  "1101",         "3501"),
  V2 = c("Nome", "Porto Velho",  "Sao Paulo"),
  V3 = c(NA,     NA,             NA),
  V4 = c("cod_recmetropol",   "001",                    "049"),
  V5 = c("cod_catmetropol",   "01",                     "01"),
  V6 = c("nome_catmetropol",  "Regiao Metropolitana",   "Regiao Metropolitana"),
  V7 = c("label_recmetropol", "RM de Porto Velho (RO)", "RM de Sao Paulo (SP)"),
  V8 = c("CodConcatenado",    "00101",                  "04901"),
  stringsAsFactors = FALSE
)
names(s1) <- c("Antigo do SIDRA", "...2", "...3",
               "Nova modelagem/codificação (2022)", "...5", "...6", "...7", "...8")

# Sheet 2: DownloadsBET_DePara_CategMetrop (9 cols, com NIVEL_TERRITORIAL)
s2 <- data.frame(
  V1 = c("NIVEL_TERRITORIAL",    "Regiao Metropolitana"),
  V2 = c("CODIGO_UNIDADE",       "1101"),
  V3 = c("NOME_UNIDADE",         "Porto Velho"),
  V4 = c(NA,                     NA),
  V5 = c("cod_recmetropol",      "001"),
  V6 = c("cod_catmetropol",      "01"),
  V7 = c("nome_catmetropol",     "Regiao Metropolitana"),
  V8 = c("label_recmetropol",    "RM de Porto Velho (RO)"),
  V9 = c("CodConcatenado",       "00101"),
  stringsAsFactors = FALSE
)
names(s2) <- c("Antigo no Downloads do IBGE", "...2", "...3", "...4",
               "Nova modelagem/codificação (2022)", "...6", "...7", "...8", "...9")

# Sheet 3: SIDRA_DePara_SubCategMetropol (9 cols)
s3 <- data.frame(
  V1 = c("CODIGO_UNIDADE",       "350101"),
  V2 = c("NOME_UNIDADE",         "Norte de Sao Paulo"),
  V3 = c(NA,                     NA),
  V4 = c("cod_recmetropol",      "049"),
  V5 = c("cod_catmetropol",      "01"),
  V6 = c("cod_subcatmetropol",   "02"),
  V7 = c("nome_subcatmetropol",  "Sub-regiao Norte"),
  V8 = c("label_subcatmetropol", "Sub-regiao Norte"),
  V9 = c("CodConcatenado",       "049010102"),
  stringsAsFactors = FALSE
)
names(s3) <- c("Antigo no SIDRA", "...2", "...3",
               "Nova modelagem/codificação (2022)",
               "...5", "...6", "...7", "...8", "...9")

# Sheet 4: DownlBET_DePara_SubCategMetropo (10 cols)
s4 <- data.frame(
  V1  = c("NIVEL_TERRITORIAL",    "Subdivisao de Regioes Metropolitanas"),
  V2  = c("CODIGO_UNIDADE",       "350101"),
  V3  = c("NOME_UNIDADE",         "Norte"),
  V4  = c(NA,                     NA),
  V5  = c("cod_recmetropol",      "049"),
  V6  = c("cod_catmetropol",      "01"),
  V7  = c("cod_subcatmetropol",   "02"),
  V8  = c("nome_subcatmetropol",  "Sub-regiao Norte"),
  V9  = c("label_subcatmetropol", "Sub-regiao Norte"),
  V10 = c("CodConcatenado",       "049010102"),
  stringsAsFactors = FALSE
)
names(s4) <- c("Antigo no Downloads do IBGE", "...2", "...3", "...4",
               "Nova modelagem/codificação (2022)",
               "...6", "...7", "...8", "...9", "...10")

openxlsx::write.xlsx(list(
  "SIDRA_DePara_CategMetropol"      = s1,
  "DownloadsBET_DePara_CategMetrop" = s2,
  "SIDRA_DePara_SubCategMetropol"   = s3,
  "DownlBET_DePara_SubCategMetropo" = s4
), file = tmp_xlsx)

# Salvar output em temp dir para nao poluir data/
old_wd <- getwd()
temp_project <- tempfile("depara_test")
dir.create(temp_project); setwd(temp_project)

out_files <- clean_metro_area_depara(tmp_xlsx)

# Normalizar path (era relativo a temp_project) antes de mudar wd
out_abs <- normalizePath(out_files[1])
setwd(old_wd)

stopifnot(length(out_files) == 1)
stopifnot(file.exists(out_abs))

result <- arrow::read_parquet(out_abs)

# Validacoes
# sheet 1 (sidra_cat): 2 data rows (Porto Velho + Sao Paulo)
# sheet 2 (bet_cat), 3 (sidra_subcat), 4 (bet_subcat): 1 data row cada
stopifnot(nrow(result) == 5)
stopifnot(all(c("source", "cod_antigo", "nome_antigo", "cod_rec_metropol",
                "cod_cat_metropol", "nome_cat_metropol", "label_rec_metropol",
                "year") %in% names(result)))

# Valores sources
expected_sources <- c("SIDRA_categ", "BET_categ", "SIDRA_subcateg", "BET_subcateg")
stopifnot(all(expected_sources %in% unique(result$source)))

# Mapeamento Porto Velho 1101 -> 1 funciona
pv <- result |> dplyr::filter(source == "SIDRA_categ", cod_antigo == 1101)
stopifnot(nrow(pv) == 1)
stopifnot(pv$cod_rec_metropol[1] == 1)
stopifnot(grepl("Porto Velho", pv$nome_antigo[1]))

# Mapeamento Sao Paulo 3501 -> 49
sp <- result |> dplyr::filter(source == "SIDRA_categ", cod_antigo == 3501)
stopifnot(sp$cod_rec_metropol[1] == 49)

# Year sempre 2022
stopifnot(all(result$year == 2022))

# Cleanup
unlink(temp_project, recursive = TRUE)

cat("PASS: toy_clean_metro_area_depara\n")
