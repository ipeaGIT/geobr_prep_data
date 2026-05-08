# Toy test: valida que as 2 AUs extintas permanecem extintas em dados IBGE.
# AU Franca (COD 3509, SP) e AU Litoral Norte (COD 4304, RS) estão em
# metro_area_2020 mas NUNCA em 2021-2024 (nem seus membros, nem o nome).
# Se o IBGE algum dia as recriar, este teste falha e sinaliza revisao.
suppressMessages({
  library(readxl); library(dplyr); library(arrow)
  library(geoarrow); library(sfarrow); library(sf)
})

source("R/support_harmonize_geobr.R")

# --- Fatos conhecidos (documentados em memory/orphan_rms_extintas.md) ---
# AU Franca: 19 municipios em 2020 (codigos 3503000, 3508207, ..., 3549508)
# AU Litoral Norte: 20 municipios em 2020 (codigos 4301057, ..., 4323804)

membros_franca <- c(
  3503000, 3508207, 3513207, 3516200, 3517406, 3517703, 3520103,
  3521309, 3523701, 3524105, 3525409, 3529708, 3536307, 3537008,
  3542701, 3543105, 3543600, 3549409, 3549508
)

membros_litoral_norte <- c(
  4301057, 4301636, 4304630, 4304671, 4304713, 4305454, 4306551,
  4310330, 4310652, 4311734, 4311775, 4312443, 4313508, 4313656,
  4321436, 4321501, 4321600, 4321667, 4321832, 4323804
)

# --- Check 1: AUs estao presentes em metro_area_2020 ---
f_2020 <- "data/metro_area/2020/metro_area_2020.parquet"
if (file.exists(f_2020)) {
  ds_2020 <- read_geoparquet(f_2020)
  franca_2020  <- ds_2020 |> filter(code_muni %in% membros_franca)
  litnor_2020  <- ds_2020 |> filter(code_muni %in% membros_litoral_norte)

  # Franca tem um nome específico em 2020
  stopifnot(nrow(franca_2020)  > 0)
  stopifnot(any(grepl("Franca", franca_2020$name_metro, ignore.case = TRUE)))
  # Litoral Norte RS tem "Litoral Norte" no nome em 2020
  stopifnot(nrow(litnor_2020) > 0)
  stopifnot(any(grepl("Litoral Norte", litnor_2020$name_metro, ignore.case = TRUE)))
} else {
  cat("SKIP check 1: metro_area_2020 nao encontrado\n")
}

# --- Check 2: nenhum membro aparece em metro_area 2021-2024 ---
for (yr in 2021:2024) {
  f <- sprintf("data/metro_area/%d/metro_area_%d.parquet", yr, yr)
  if (!file.exists(f)) next
  ds <- read_geoparquet(f)

  n_franca  <- sum(ds$code_muni %in% membros_franca)
  n_litnor  <- sum(ds$code_muni %in% membros_litoral_norte)

  # Se IBGE reinserir: falha aqui e sinaliza revisao
  stopifnot(n_franca == 0)
  stopifnot(n_litnor == 0)
}

# --- Check 3: cod_antigo 3509 e 4304 NAO estao no DePara ---
f_dp <- "data/metro_area_depara/metro_area_depara_clean.parquet"
if (file.exists(f_dp)) {
  dp <- arrow::read_parquet(f_dp)
  stopifnot(sum(dp$cod_antigo == 3509, na.rm = TRUE) == 0)
  stopifnot(sum(dp$cod_antigo == 4304, na.rm = TRUE) == 0)
} else {
  cat("SKIP check 3: DePara parquet nao encontrado\n")
}

cat("PASS: toy_orphan_rms_extintas\n")
cat("  (Se IBGE algum dia recriar AU Franca/Litoral Norte, este teste falha.)\n")
