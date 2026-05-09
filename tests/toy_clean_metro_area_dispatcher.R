# Toy test: dispatcher de clean_metro_area direciona cada year ao helper correto
# Não invoca clean_metro_area inteira (precisaria parquets de muni reais);
# valida que cada helper individualmente roda sem erro com input minimo
suppressMessages(library(dplyr))
suppressMessages(library(tidyr))

source("R/metro_area.R")

# 1970: hardcoded
df_1970 <- metro_area_1970_data()
stopifnot(nrow(df_1970) == 113)
stopifnot(length(unique(df_1970$name_metro)) == 9)
stopifnot(is.numeric(df_1970$code_muni))
stopifnot(all(df_1970$year == 1970))

# 2001-2003 (com prefix RM aplicado)
raw_01 <- data.frame(
  CODIGO = 1,
  NOME_DA_RM = "Baixada Santista",
  CODIGO_DO_MUNICIPIO = 3504107,
  LEGISLACAO = "LC",
  DATA_DA_LEI = "01.01.2001",
  check.names = FALSE, stringsAsFactors = FALSE
)
names(raw_01) <- c("CÓDIGO", "NOME_DA_RM", "CÓDIGO_DO_MUNICÍPIO", "LEGISLAÇÃO", "DATA_DA_LEI")
r01 <- normalize_metro_2001_2003(raw_01, 2001)
stopifnot(grepl("^RM ", r01$name_metro[1]))

# 2005
raw_05 <- data.frame(
  X1 = c("RM Recife", NA),
  CODIGO = c(2607604, NA),
  MUNICIPIO = c("Recife", NA),
  LEGISLACAO = c("LC", NA),
  DATA_LEI = c("01.01.1973", NA),
  check.names = FALSE, stringsAsFactors = FALSE
)
names(raw_05)[1] <- "Nome da Região Metropolitana - RM, Aglomeração Urbana - AU ou Região Integrada - RIDE"
names(raw_05)[2] <- "CÓDIGO"
names(raw_05)[3] <- "MUNICÍPIO"
names(raw_05)[4] <- "LEGISLAÇÃO"
r05 <- normalize_metro_2005(raw_05)
stopifnot(nrow(r05) == 1)

# 2010-2015 schema portugues
raw_13 <- data.frame(
  COD_UF = 26, SIGLA_UF = "PE",
  X1 = "RM Recife", X2 = "Núcleo",
  X5 = 2607604, X6 = "Recife",
  X7 = "LC", X8 = "01.01.1973", tipo = "RM",
  check.names = FALSE, stringsAsFactors = FALSE
)
names(raw_13)[3] <- "Região Metropolitana, RIDE ou Aglomeração Urbana"
names(raw_13)[4] <- "Subdivisões"
names(raw_13)[5] <- "Código Município"
names(raw_13)[6] <- "Nome Município"
names(raw_13)[7] <- "Legislação"
names(raw_13)[8] <- "Data Lei"
# raw_13 tem nrow=1; helper para year=2014 NAO faz trim de rodape (filter limpa)
r13 <- normalize_metro_2010_2015(raw_13, 2014)
stopifnot(nrow(r13) == 1)
stopifnot(r13$name_metro[1] == "RM Recife")

# 2016-2018 schema UPPERCASE 9 cols
raw_17 <- data.frame(
  COD_UF = 26, SIGLA_UF = "PE",
  NOME_RM = "RM Recife", SUBDIVISAO = "Núcleo",
  COD_MUN = 2607604, NOME_MUN = "Recife",
  LEG = "LC", DATA = "01.01.1973", TIPO = "RM",
  stringsAsFactors = FALSE
)
r17 <- normalize_metro_2016_2018(raw_17)
stopifnot(nrow(r17) == 1)

# 2019-2020 schema 12 cols
raw_19 <- data.frame(
  GRANDE_REG = "Nordeste", COD_UF = 26, SIGLA_UF = "PE",
  COD = 2600001, NOME = "RM Recife", TIPO = "RM",
  COD_CAT_ASSOC = 1, CAT_ASSOC = "Núcleo",
  COD_MUN = 2607604, NOME_MUN = "Recife",
  LEG = "LC", DATA = "01.01.1973",
  stringsAsFactors = FALSE
)
r19 <- normalize_metro_2019_2020(raw_19, 2019)
stopifnot(nrow(r19) == 1)

# 2021-2024 schema 16 cols (testado em toy_normalize_metro_2021_2024.R)

cat("PASS: toy_clean_metro_area_dispatcher\n")
