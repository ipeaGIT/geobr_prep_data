#> DATASET: State Capitals (Capitais dos Estados)
#> Source: Derived from municipal_seat data (IBGE)
#> Metadata:
# Titulo: Capitais dos Estados
# Titulo alternativo: State Capitals
# Frequencia de atualizacao: Censal
# Forma de apresentacao: Points
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Localizacao pontual das capitais dos 26 estados + DF.
# Informacoes adicionais: Derivado dos dados de sedes municipais (IBGE).
# Proposito: Identificacao das capitais estaduais brasileiras.
#
# Estado: Ativo
# Informacao do Sistema de Referencia: SIRGAS 2000
#
# Anos disponiveis: 2010

### Libraries (use any library as necessary) -----------------------------------

# library(sf)
# library(dplyr)
# library(arrow)
# source("./R/support_harmonize_geobr.R")
# source("./R/support_fun.R")

# Official state capital municipal codes (27 = 26 states + DF)
CAPITAL_CODES <- c(
  1100205,  # Porto Velho (RO)
  1200401,  # Rio Branco (AC)
  1302603,  # Manaus (AM)
  1400100,  # Boa Vista (RR)
  1501402,  # Belem (PA)
  1600303,  # Macapa (AP)
  1721000,  # Palmas (TO)
  2111300,  # Sao Luis (MA)
  2211001,  # Teresina (PI)
  2304400,  # Fortaleza (CE)
  2408102,  # Natal (RN)
  2507507,  # Joao Pessoa (PB)
  2611606,  # Recife (PE)
  2704302,  # Maceio (AL)
  2800308,  # Aracaju (SE)
  2927408,  # Salvador (BA)
  3106200,  # Belo Horizonte (MG)
  3205309,  # Vitoria (ES)
  3304557,  # Rio de Janeiro (RJ)
  3550308,  # Sao Paulo (SP)
  4106902,  # Curitiba (PR)
  4205407,  # Florianopolis (SC)
  4314902,  # Porto Alegre (RS)
  5002704,  # Campo Grande (MS)
  5103403,  # Cuiaba (MT)
  5208707,  # Goiania (GO)
  5300108   # Brasilia (DF)
)

# Download the data  -----------------------------------------------------------
download_capitals <- function(municipal_seat_files) {

  ## Read municipal_seat parquets and filter for capitals
  seats <- read_geoparquet(municipal_seat_files)

  ## Filter for capital codes
  capitals <- seats |>
    dplyr::filter(code_muni %in% CAPITAL_CODES)

  message("Capitais encontradas: ", nrow(capitals), "/27")

  if (nrow(capitals) != 27) {
    missing <- setdiff(CAPITAL_CODES, capitals$code_muni)
    warning("Capitais faltando: ", paste(missing, collapse = ", "))
  }

  return(capitals)
}

# Clean the data  --------------------------------------------------------------
clean_capitals <- function(capitals_raw) {

  dir_clean <- "./data/capitals/2010"
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  temp_sf <- capitals_raw

  ## Reorder columns (geometry ALWAYS last)
  temp_sf <- temp_sf |>
    dplyr::select(
      code_muni, name_muni,
      code_state, abbrev_state, name_state,
      code_region, name_region,
      year, geometry
    )

  ## Validation
  if (nrow(temp_sf) != 27) {
    warning("Esperado 27 capitais, obtido ", nrow(temp_sf))
  }
  stopifnot(nrow(temp_sf) >= 1)
  stopifnot(!is.na(sf::st_crs(temp_sf)))
  stopifnot(sf::st_crs(temp_sf)$epsg == 4674)
  stopifnot(is.numeric(temp_sf$code_muni))
  stopifnot(is.numeric(temp_sf$code_state))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")
  stopifnot(all(sf::st_geometry_type(temp_sf) == "POINT"))

  ## Save
  write_geobr_parquet(temp_sf,
    paste0(dir_clean, "/capitals_2010.parquet"))

  files <- list.files(path = dir_clean, pattern = "\\.parquet$",
                      recursive = TRUE, full.names = TRUE)
  return(files)
}
