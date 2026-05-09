#> DATASET: metropolitan areas (Regioes Metropolitanas)
#> Source: IBGE - https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/municipios_por_regioes_metropolitanas/
#> Metadata:
# Titulo: Regioes Metropolitanas, RIDEs e Aglomeracoes Urbanas
# Frequencia de atualizacao: Irregular (anual com lacunas: 2004, 2006, 2007, 2011, 2012 ausentes)
# Forma de apresentacao: Planilhas Excel (geometria via municipios)
# Linguagem: Pt-BR
# Character set: UTF-8 (R/readxl resolve encoding nativamente nos XLS antigos)
# Informacao do Sistema de Referencia: SIRGAS 2000 (atribuido por harmonize_geobr)

# ==============================================================================
# URL catalog (regra: 2002 = 30/08; 2013-2020 = junho; demais = unico disponivel)
# ==============================================================================

.metro_area_urls <- function() {
  base <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/municipios_por_regioes_metropolitanas/"

  list(
    "2001" = paste0(base, "Situacao_2000a2009/2001a2005/RM%2004.09.01.xls"),
    "2002" = paste0(base, "Situacao_2000a2009/2001a2005/RM%2030.08.02.xls"),
    "2003" = paste0(base, "Situacao_2000a2009/2001a2005/RM%2017.02.03.xls"),
    "2005" = paste0(base, "Situacao_2000a2009/2001a2005/RM%2031.12.05.xls"),
    "2008" = paste0(base, "Situacao_2000a2009/2006a2009/rm_atualizada_2008.xls"),
    "2009" = paste0(base, "Situacao_2000a2009/2006a2009/rm_atualizada_2009.xls"),
    "2010" = paste0(base, "Situacao_2010a2019/Composicao_RMs_RIDEs_AglomUrbanas_2010_07_31.xls"),
    "2013" = paste0(base, "Situacao_2010a2019/Composicao_RMs_RIDEs_AglomUrbanas_2013_06_30.xls"),
    "2014" = paste0(base, "Situacao_2010a2019/Composicao_RMs_RIDEs_AglomUrbanas_2014_06_30.xls"),
    "2015" = paste0(base, "Situacao_2010a2019/Composicao_RMs_RIDEs_AglomUrbanas_2015_06_30.xls"),
    "2016" = paste0(base, "Situacao_2010a2019/Composicao_RMs_RIDEs_AglomUrbanas_2016_06_30.xlsx"),
    "2017" = paste0(base, "Situacao_2010a2019/Composicao_RMs_RIDEs_AglomUrbanas_2017_06_30.xls"),
    "2018" = paste0(base, "Situacao_2010a2019/Composicao_RMs_RIDEs_AglomUrbanas_2018_06_30_v2.xlsx"),
    "2019" = paste0(base, "Situacao_2010a2019/Composicao_RMs_RIDEs_AglomUrbanas_2019_06_30.xlsx"),
    "2020" = paste0(base, "Situacao_2020a2029/Composicao_RMs_RIDEs_AglomUrbanas_2020_06_30_v2.xlsx"),
    "2021" = paste0(base, "Situacao_2020a2029/Composicao_RMs_RIDEs_AglomUrbanas_2021_v2.xlsx"),
    "2022" = paste0(base, "Situacao_2020a2029/Composicao_RMs_RIDEs_AglomUrbanas_2022_v2.xlsx"),
    "2023" = paste0(base, "Situacao_2020a2029/Composicao_RM_2023.xlsx"),
    "2024" = paste0(base, "Situacao_2020a2029/Composicao_RM_2024.xlsx")
  )
}

# Download the data ------------------------------------------------------------
download_metro_area <- function(year) {

  ## 0. Special case: 1970 hardcoded data
  if (year == 1970) {
    return(metro_area_1970_data())
  }

  ## 1. Resolve URL from catalog
  urls <- .metro_area_urls()
  ftp_link <- urls[[as.character(year)]]
  if (is.null(ftp_link)) {
    stop(paste("Ano", year, "nao suportado para metro_area"))
  }

  ## 2. Download to temp
  ext <- if (grepl("[.]xlsx$", ftp_link)) ".xlsx" else ".xls"
  tmp <- tempfile(fileext = ext)

  httr::GET(url = ftp_link, httr::progress(),
            httr::write_disk(path = tmp, overwrite = TRUE),
            httr::timeout(300))

  ## 3. Read Excel
  raw <- readxl::read_excel(tmp)
  raw$year <- year

  return(raw)
}

# Clean the data ---------------------------------------------------------------
# Sample interativo para debug local (descomente no console pra trabalhar
# linha-por-linha em clean_metro_area). Estas linhas executam a cada
# tar_source("R") e quebram tar_make/tar_destroy/tar_invalidate quando o
# cache de targets esta vazio ou inconsistente.
# raw <- tar_read(metro_area_raw, 5)
# head(raw)
# municipality_clean <- tar_read(municipality_clean)
# hist_muni_clean <- tar_read(hist_muni_clean)

# 2001 falta nome de rm no para
# raw <- tar_read(metro_area_raw, 12)
clean_metro_area <- function(raw, municipality_clean, hist_muni_clean) {

  ## 0. Create folder to save clean data
  yyyy <- raw$year[1]
  dir_clean <- paste0("./data/metro_area/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  ## 1. Normalize columns (year-specific dispatcher)
  if (yyyy == 1970) {
    temp_df <- raw |>
      dplyr::mutate(
        name_metro       = as.character(name_metro),
        code_muni        = code_muni,
        legislation      = as.character(legislation),
        legislation_date = as.character(legislation_date)
      ) |>
      dplyr::select(name_metro, code_muni, legislation, legislation_date)

  } else if (yyyy %in% c(2001, 2002, 2003)) {
    temp_df <- normalize_metro_2001_2003(raw, yyyy)

  } else if (yyyy == 2005) {
    temp_df <- normalize_metro_2005(raw)

  } else if (yyyy %in% c(2008, 2009)) {
    temp_df <- normalize_metro_2008_2009(raw, yyyy)

  } else if (yyyy %in% c(2010, 2013, 2014, 2015)) {
    temp_df <- normalize_metro_2010_2015(raw, yyyy)

  } else if (yyyy %in% c(2016, 2017, 2018)) {
    temp_df <- normalize_metro_2016_2018(raw)

  } else if (yyyy %in% c(2019, 2020)) {
    temp_df <- normalize_metro_2019_2020(raw, yyyy)

  } else if (yyyy %in% c(2021, 2022, 2023, 2024)) {
    temp_df <- normalize_metro_2021_2024(raw, yyyy)

  } else {
    stop(paste("Ano", yyyy, "nao suportado para metro_area"))
  }

  ## 1b. Validacoes pos-normalizacao
  stopifnot(!is.null(temp_df))
  stopifnot("name_metro" %in% names(temp_df))
  stopifnot("code_muni"  %in% names(temp_df))
  stopifnot(is.numeric(temp_df$code_muni))
  stopifnot(!any(is.na(temp_df$code_muni)))
  stopifnot(all(temp_df$code_muni > 0))
  stopifnot(!any(temp_df$name_metro == "" | is.na(temp_df$name_metro)))
  # Codigos IBGE com 6 digitos (sem digito verificador) sao recuperados apos
  # load do parquet de municipios (§1d abaixo); aqui nao validamos nchar==7.

  ## 2. Get municipality geometry (year_muni mapping; ver plano §8)
  year_muni <- dplyr::case_when(
    yyyy == 1970               ~ 1970L,   # hist_muni
    yyyy %in% c(2002, 2003)    ~ 2005L,   # 0% unmatched validado; proxy 2001 perderia RN
    yyyy == 2008               ~ 2007L,   # 99.4% match #666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666
    yyyy == 2009               ~ 2010L,   # 99.8% match #666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666
    TRUE                        ~ as.integer(yyyy)
  )

  # seleciona arquivo de municipios
  muni_files <- c(municipality_clean, hist_muni_clean)
  muni_files <- muni_files[grep(year_muni, muni_files)]
  muni_file <- muni_files[!grepl('simplified', muni_files)]
  
  municipios <- read_geoparquet(muni_file)

  # 1970: Guanabara -> Rio de Janeiro (Guanabara era code_state=34;
  # após fusão com Estado do Rio em 1975, virou RJ code_state=33)
  if (yyyy == 1970) {
    rio <- dplyr::filter(municipios, name_muni == "Guanabara")
    if (nrow(rio) > 0) {
      rio$code_muni  <- 3304557
      rio$name_muni  <- "Rio de Janeiro"
      rio$code_state <- 33L
      municipios <- rbind(municipios, rio)
    }
  }

  ## 1d. Recuperar codigo verificador para codigos IBGE de 6 digitos
  # Codigos IBGE sao unicos por 6 digitos; o 7o e digito verificador. Planilhas
  # IBGE antigas (ex: 2005) ocasionalmente reportam 6 digitos. Recuperamos o
  # 7o digito via lookup no parquet de municipios.
  short_codes_mask <- nchar(as.character(temp_df$code_muni)) == 6
  if (any(short_codes_mask)) {
    short_codes <- temp_df$code_muni[short_codes_mask]
    muni_codes <- municipios$code_muni
    for (i in which(short_codes_mask)) {
      bad <- temp_df$code_muni[i]
      # Match: 7-digit code cujo floor(.../10) == bad
      candidates <- muni_codes[floor(muni_codes / 10) == bad]
      if (length(candidates) == 1) {
        temp_df$code_muni[i] <- candidates
        message("Recuperado digito verificador (year ", yyyy, "): ",
                bad, " -> ", candidates)
      } else {
        message("Aviso (year ", yyyy, "): codigo ", bad,
                " (6 digitos) sem match unico em municipios; linha sera descartada por falta de geometria.")
      }
    }
  }
  
  
  ## 3. Join geometry
  temp_sf <- dplyr::left_join(temp_df, municipios, by = "code_muni") |>
    sf::st_as_sf()

  # Aviso sobre duplicatas (municipio em mais de uma RM e legitimo, mas alertar)
  dups <- temp_sf$code_muni[duplicated(temp_sf$code_muni)]
  if (length(dups) > 0) {
    message("Aviso (year ", yyyy, "): ", length(unique(dups)),
            " municipios em mais de uma RM (esperado, nao e bug).")
  }
  
  # remove Murici da RM Zona da Mata a partir de 2014
  # issue #250 https://github.com/ipeaGIT/geobr/issues/250
  if (yyyy>=2015) {
    temp_sf <- temp_sf |> 
      filter_out(code_muni == 2705507 & name_metro =="RM da Zona da Mata")
  }

  # Aviso sobre unmatched (perda de geometria por proxy/dado IBGE)
  n_unmatched <- sum(sf::st_is_empty(sf::st_geometry(temp_sf)))
  n_no_state  <- sum(is.na(temp_sf$code_state))
  if (n_no_state > 0) {
    message("Aviso (year ", yyyy, "): ", n_no_state,
            " municipios sem geometria/code_state (proxy year_muni=", year_muni,
            ", ver plano §7-8).")
  }

  # Remove rows without geometry match or missing metro name
  temp_sf <- dplyr::filter(temp_sf, !is.na(name_metro), !sf::st_is_empty(sf::st_geometry(temp_sf)))

  ## 4. Harmonize
  temp_sf <- harmonize_geobr(
    temp_sf            = temp_sf,
    year               = yyyy,
    add_state          = F,
    # state_column       = "code_state",
    add_region         = F,
    # region_column      = "code_state",
    add_snake_case     = TRUE,
    snake_colname      = "name_metro",
    projection_fix     = TRUE,
    encoding_utf8      = TRUE,
    topology_fix       = TRUE,
    remove_z_dimension = TRUE,
    use_multipolygon   = TRUE
  )

  ## 5. Column order (geometry sempre por ultimo; ver column-conventions.md)
  temp_sf <- temp_sf |>
    dplyr::select(
      name_metro, code_muni, name_muni,
      dplyr::any_of(c("type", "subdivision", "legislation", "legislation_date")),
      code_state, abbrev_state, name_state,
      code_region, name_region,
      year, geometry
    )
  
  ## 7. Sort
  temp_sf <- temp_sf |>
    dplyr::arrange(code_state, code_muni, name_metro)

  ## 8. Validate (final)
  stopifnot(is.numeric(temp_sf$code_muni))
  stopifnot(is.numeric(temp_sf$code_state))
  stopifnot(!is.na(sf::st_crs(temp_sf)))
  stopifnot(sf::st_crs(temp_sf)$epsg == 4674)
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")

  ## 9. Lighter version
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)

  ## 10. Save datasets
  write_geobr_parquet(
    sf_obj = temp_sf, 
    path = paste0(dir_clean, "/metroarea_", yyyy, ".parquet")
    )

  write_geobr_parquet(
    sf_obj = temp_sf_simplified,
    path = paste0(dir_clean, "/metroarea_", yyyy, "_simplified.parquet")
    )

  ## 11. Return file list
  files <- list.files(dir_clean, pattern = "[.]parquet$",
                      full.names = TRUE, recursive = TRUE)
  return(files)
}


# ==============================================================================
# Helper: Normalize 2001-2003 data
# Column names vary by year:
#   2001: CÓDIGO | NOME_DA_RM          | CÓDIGO_DO_MUNICÍPIO | ... | DATA_DA_LEI
#   2002: CÓDIGO | NOME DA REGIÃO MET. | CÓDIGO_DO_MUNICÌPIO | ... | DATA DA LEI
#   2003: CÓDIGO | NOME DA REGIÃO MET. | CÓDIGO_DO_MUNICÍPIO | ... | DATA DA LEI
# 2001-2003: todos os 3 anos precisam de prefix "RM " (validado empiricamente)
# ==============================================================================
normalize_metro_2001_2003 <- function(raw, year) {

  temp_df <- raw
  cols <- names(temp_df)

  metro_col <- grep("NOME.*(RM|REGI)", cols, value = TRUE)[1]
  if (is.na(metro_col)) stop("Schema mismatch ", year, ": coluna NOME RM nao encontrada")
  names(temp_df)[names(temp_df) == metro_col] <- "name_metro"

  muni_col <- grep("MUNIC", cols, value = TRUE)[1]
  if (is.na(muni_col)) stop("Schema mismatch ", year, ": coluna MUNIC nao encontrada")
  names(temp_df)[names(temp_df) == muni_col] <- "code_muni"

  leg_col <- grep("LEGISLA", cols, value = TRUE)[1]
  if (!is.na(leg_col)) names(temp_df)[names(temp_df) == leg_col] <- "legislation"

  date_col <- grep("DATA", cols, value = TRUE)[1]
  if (!is.na(date_col)) names(temp_df)[names(temp_df) == date_col] <- "legislation_date"

  # Forward-fill NAs in name_metro and legislation
  temp_df <- tidyr::fill(temp_df, name_metro, .direction = "down")
  if ("legislation" %in% names(temp_df))
    temp_df <- tidyr::fill(temp_df, legislation, .direction = "down")
  if ("legislation_date" %in% names(temp_df)) {
    temp_df <- tidyr::fill(temp_df, legislation_date, .direction = "down")
    temp_df$legislation_date <- gsub(" ", "", as.character(temp_df$legislation_date))
  }

  temp_df$code_muni <- as.numeric(as.character(temp_df$code_muni))

  # Add "RM " prefix to harmonize metro names (2001-2003: nenhum vem com prefix
  # nas planilhas IBGE; validado empiricamente para todos os 3 anos).
  if (year %in% c(2001, 2002, 2003)) {
    no_prefix <- !grepl(
      "Distrito Federal|Aglomera|RIDE|Colar Metropolitano|Expans|cleo Metro",
      temp_df$name_metro, ignore.case = TRUE
    )
    needs_prefix <- no_prefix & !grepl("^RM ", temp_df$name_metro)
    temp_df$name_metro[needs_prefix] <- paste0("RM ", temp_df$name_metro[needs_prefix])
  }

  temp_df <- temp_df |>
    dplyr::select(name_metro, code_muni,
                  dplyr::any_of(c("legislation", "legislation_date"))) |>
    dplyr::filter(!is.na(code_muni))

  return(temp_df)
}


# ==============================================================================
# Helper: Normalize 2005 data
# Cols: Nome da Regiao Metropolitana... | CODIGO | MUNICIPIO | LEGISLACAO | DATA_LEI
# ==============================================================================
normalize_metro_2005 <- function(raw) {

  # Remove last row (totals/notes)
  raw <- raw[-nrow(raw), ]

  metro_col <- grep("Regi.o Metropolitana", names(raw), value = TRUE)[1]
  if (is.na(metro_col)) stop("Schema mismatch 2005: coluna Regiao Metropolitana nao encontrada")

  temp_df <- raw
  names(temp_df)[names(temp_df) == metro_col] <- "name_metro"

  # CODIGO here is code_muni (not metro code like in 2001-2003)
  code_col <- grep("^C.DIGO", names(temp_df), value = TRUE)[1]
  if (is.na(code_col)) stop("Schema mismatch 2005: coluna CODIGO nao encontrada")
  names(temp_df)[names(temp_df) == code_col] <- "code_muni"

  leg_col <- grep("LEGISLA", names(temp_df), value = TRUE)[1]
  if (!is.na(leg_col)) names(temp_df)[names(temp_df) == leg_col] <- "legislation"

  date_col <- grep("DATA", names(temp_df), value = TRUE)[1]
  if (!is.na(date_col)) names(temp_df)[names(temp_df) == date_col] <- "legislation_date"

  temp_df <- tidyr::fill(temp_df, name_metro, .direction = "down")
  if ("legislation" %in% names(temp_df)) {
    temp_df <- tidyr::fill(temp_df, legislation, .direction = "down")
  }
  if ("legislation_date" %in% names(temp_df)) {
    temp_df <- tidyr::fill(temp_df, legislation_date, .direction = "down")
    temp_df$legislation_date <- gsub(" ", "", as.character(temp_df$legislation_date))
  }

  temp_df$code_muni <- as.numeric(as.character(temp_df$code_muni))

  temp_df <- temp_df |>
    dplyr::select(name_metro, code_muni,
                  dplyr::any_of(c("legislation", "legislation_date"))) |>
    dplyr::filter(!is.na(code_muni))

  return(temp_df)
}


# ==============================================================================
# Helper: Normalize 2008-2009 data
# Cols (4): name_metro | subdivision | code_muni | name_muni
# Estruturas:
#   - 2008: linha 0 e metadata header (skip), 1 linha de footer
#   - 2009: linha 0 ja e dado, ~5 linhas de footnotes
# Ambos SEM legislation/legislation_date.
# Renomeacao por POSICAO (cols 1-4) porque nomes literais variam entre anos.
# ==============================================================================
normalize_metro_2008_2009 <- function(raw, year) {

  # Skip primeira linha em 2008 (e header metadata, nao dado)
  if (year == 2008) raw <- raw[-1, ]

  if (ncol(raw) < 4) {
    stop("Schema mismatch ", year, ": esperadas >=4 colunas, encontradas ", ncol(raw))
  }

  # Renomear cols 1-4 por posicao (nomes literais variam: 'Unnamed: X' vs nomes longos)
  names(raw)[1:4] <- c("name_metro", "subdivision", "code_muni", "name_muni")

  # Forward-fill name_metro (varias linhas tem NA, valor herdado da linha anterior)
  raw <- tidyr::fill(raw, name_metro, .direction = "down")

  raw$code_muni <- suppressWarnings(as.numeric(as.character(raw$code_muni)))

  # Filtra footer/notas (sem code_muni numerico) ou nome_metro vazio
  raw <- raw[!is.na(raw$code_muni), ]
  raw <- raw[!is.na(raw$name_metro), ]

  # Limpa nomes: trim + remove sufixo "(N)" usado em footnotes ("RM Manaus (1)" -> "RM Manaus")
  raw$name_metro <- trimws(raw$name_metro)
  raw$name_metro <- gsub(" *\\(\\d+\\) *$", "", raw$name_metro)

  raw |>
    dplyr::select(name_metro, code_muni, dplyr::any_of("subdivision"))
}


# ==============================================================================
# Helper: Normalize 2010-2015 data
# Cols (variavel): Regiao Metropolitana, RIDE ou Aglomeracao Urbana | Subdivisoes |
#                  Codigo Municipio | Nome Municipio | Legislacao | Data Lei | Tipo
# 2013-2015 acrescentam COD_UF, SIGLA_UF; 2015 split de Data Lei em "assinatura"+"publicacao"
# ==============================================================================
normalize_metro_2010_2015 <- function(raw, year) {

  # Remove trailing rows: 2010/2013 ate 2; 2014 detectado via filter posterior; 2015 ate 4
  if (year %in% c(2010, 2013)) {
    raw <- raw[1:(nrow(raw) - 2), ]
  } else if (year == 2015) {
    raw <- raw[1:(nrow(raw) - 4), ]
  }
  # 2014: rodape (4 linhas ~ 2 NA + 2 notas) e removido pelo filter !is.na(code_muni) abaixo

  metro_col <- grep("Regi.o Metropolitana", names(raw), value = TRUE)[1]
  muni_col  <- grep("C.digo.*Munic", names(raw), value = TRUE)[1]
  sub_col   <- grep("Subdivis", names(raw), value = TRUE)[1]
  leg_col   <- grep("Legisla", names(raw), value = TRUE)[1]
  type_col  <- grep("^[Tt]ipo$", names(raw), value = TRUE)[1]
  # Date col varia: "Data Lei" (2010-2014); 2015 tem "Data de assinatura..." e "Data de publicacao..."
  date_col  <- grep("^Data", names(raw), value = TRUE)[1]

  if (is.na(metro_col)) stop("Schema mismatch ", year, ": coluna Regiao Metropolitana nao encontrada")
  if (is.na(muni_col))  stop("Schema mismatch ", year, ": coluna Codigo Municipio nao encontrada")

  temp_df <- raw
  names(temp_df)[names(temp_df) == metro_col] <- "name_metro"
  names(temp_df)[names(temp_df) == muni_col]  <- "code_muni"
  if (!is.na(sub_col))  names(temp_df)[names(temp_df) == sub_col]  <- "subdivision"
  if (!is.na(leg_col))  names(temp_df)[names(temp_df) == leg_col]  <- "legislation"
  if (!is.na(date_col)) names(temp_df)[names(temp_df) == date_col] <- "legislation_date"
  if (!is.na(type_col)) names(temp_df)[names(temp_df) == type_col] <- "type"

  temp_df$code_muni <- suppressWarnings(as.numeric(as.character(temp_df$code_muni)))
  if ("legislation_date" %in% names(temp_df)) {
    temp_df$legislation_date <- as.character(temp_df$legislation_date)
  }

  temp_df <- temp_df |>
    dplyr::select(name_metro, code_muni,
                  dplyr::any_of(c("type", "subdivision",
                                  "legislation", "legislation_date"))) |>
    dplyr::filter(!is.na(code_muni), !is.na(name_metro))

  return(temp_df)
}


# ==============================================================================
# Helper: Normalize 2016-2018 data (junho)
# Cols (9): COD_UF | SIGLA_UF | NOME_RM | SUBDIVISAO | COD_MUN | NOME_MUN |
#           LEG | DATA | TIPO
# 2018_06_v2 mantem este schema (dezembro 2018 e diferente, fora do escopo)
# ==============================================================================
normalize_metro_2016_2018 <- function(raw) {

  required <- c("NOME_RM", "COD_MUN")
  missing <- setdiff(required, names(raw))
  if (length(missing)) stop("Schema mismatch 2016-2018: cols faltando: ",
                            paste(missing, collapse = ","))

  temp_df <- raw |>
    dplyr::rename(
      name_metro       = "NOME_RM",
      code_muni        = "COD_MUN",
      subdivision      = "SUBDIVISAO",
      legislation      = "LEG",
      legislation_date = "DATA",
      type             = "TIPO"
    ) |>
    dplyr::mutate(
      code_muni        = suppressWarnings(as.numeric(code_muni)),
      legislation_date = as.character(legislation_date)
    ) |>
    dplyr::select(name_metro, code_muni,
                  dplyr::any_of(c("type", "subdivision",
                                  "legislation", "legislation_date"))) |>
    dplyr::filter(!is.na(code_muni), !is.na(name_metro))

  return(temp_df)
}


# ==============================================================================
# Helper: Normalize 2019-2020 data (junho)
# Cols (12): GRANDE_REG | COD_UF | SIGLA_UF | COD | NOME | TIPO |
#            COD_CAT_ASSOC | CAT_ASSOC | COD_MUN | NOME_MUN | LEG | DATA
# Em junho, DATA vem como POSIXct -> formatado para "DD.MM.YYYY"
# ==============================================================================
normalize_metro_2019_2020 <- function(raw, year) {

  required <- c("NOME", "COD_MUN")
  missing <- setdiff(required, names(raw))
  if (length(missing)) stop("Schema mismatch ", year, ": cols faltando: ",
                            paste(missing, collapse = ","))

  temp_df <- raw |>
    dplyr::rename(
      name_metro       = "NOME",
      code_muni        = "COD_MUN",
      subdivision      = "CAT_ASSOC",
      legislation      = "LEG",
      legislation_date = "DATA",
      type             = "TIPO"
    )

  # Format date: POSIXt em junho (esperado); fallback para character generico
  if (inherits(temp_df$legislation_date, "POSIXt") ||
      inherits(temp_df$legislation_date, "Date")) {
    temp_df$legislation_date <- format(temp_df$legislation_date, "%d.%m.%Y")
  }

  temp_df <- temp_df |>
    dplyr::mutate(
      code_muni        = suppressWarnings(as.numeric(code_muni)),
      legislation_date = as.character(legislation_date)
    ) |>
    dplyr::select(name_metro, code_muni,
                  dplyr::any_of(c("type", "subdivision",
                                  "legislation", "legislation_date"))) |>
    dplyr::filter(!is.na(code_muni), !is.na(name_metro))

  return(temp_df)
}


# ==============================================================================
# Helper: Normalize 2021-2024 data
# Cols (16): GRANDE_REGIAO | COD_UF | SIGLA_UF |
#            COD_RECMETROPOL | NOME_RECMETROPOL | LABEL_RECMETROPOL |
#            COD_CATMETROPOL | NOME_CATMETROPOL | LABEL_CATMETROPOL |
#            COD_SUBCATMETROPOL | NOME_SUBCATMETROPOL | LABEL_SUBCATMETROPOL |
#            COD_MUN | NOME_MUN | LEG | DATA
# Schema identico para 2021, 2022, 2023, 2024.
# DATA vem como character "DD.MM.YYYY" (nao precisa conversao)
# ==============================================================================
normalize_metro_2021_2024 <- function(raw, year) {

  required <- c("NOME_RECMETROPOL", "COD_MUN", "LEG", "DATA")
  missing <- setdiff(required, names(raw))
  if (length(missing)) stop("Schema mismatch ", year, ": cols faltando: ",
                            paste(missing, collapse = ","))

  temp_df <- raw |>
    dplyr::rename(
      name_metro       = "NOME_RECMETROPOL",
      code_muni        = "COD_MUN",
      subdivision      = "LABEL_SUBCATMETROPOL",
      legislation      = "LEG",
      legislation_date = "DATA",
      type             = "LABEL_CATMETROPOL"
    )

  if (inherits(temp_df$legislation_date, "POSIXt") ||
      inherits(temp_df$legislation_date, "Date")) {
    temp_df$legislation_date <- format(temp_df$legislation_date, "%d.%m.%Y")
  }

  temp_df <- temp_df |>
    dplyr::mutate(
      code_muni        = suppressWarnings(as.numeric(code_muni)),
      legislation_date = as.character(legislation_date)
    ) |>
    dplyr::select(name_metro, code_muni,
                  dplyr::any_of(c("type", "subdivision",
                                  "legislation", "legislation_date"))) |>
    dplyr::filter(!is.na(code_muni), !is.na(name_metro))

  return(temp_df)
}


# ==============================================================================
# Helper: 1970 hardcoded data (from IBGE Lei Complementar 014/020)
# ==============================================================================
metro_area_1970_data <- function() {

  data.frame(
    name_metro = c(
      rep("RM Belém", 2),
      rep("RM Fortaleza", 5),
      rep("RM Recife", 9),
      rep("RM Salvador", 8),
      rep("RM Belo Horizonte", 14),
      rep("RM Rio de Janeiro", 10),
      rep("RM São Paulo", 37),
      rep("RM Curitiba", 14),
      rep("RM Porto Alegre", 14)
    ),
    code_muni = c(
      # Belem
      1500800, 1501402,
      # Fortaleza
      2301000, 2303709, 2304400, 2307700, 2309706,
      # Recife
      2602902, 2606804, 2607604, 2607901, 2609402, 2609600, 2610707,
      2611606, 2613701,
      # Salvador
      2905701, 2906501, 2916104, 2919207, 2927408, 2929206, 2930709,
      2933208,
      # Belo Horizonte
      3106200, 3106705, 3110004, 3118601, 3129806, 3137601, 3144805,
      3149309, 3153905, 3154606, 3154804, 3156700, 3157807, 3171204,
      # Rio de Janeiro
      3301702, 3301900, 3302502, 3303203, 3303302, 3303500, 3303609,
      3304557, 3304904, 3305109,
      # Sao Paulo
      3503901, 3505708, 3506607, 3509007, 3509205, 3510609, 3513009,
      3513801, 3515004, 3515103, 3515707, 3516309, 3516408, 3518305,
      3518800, 3522208, 3522505, 3523107, 3525003, 3526209, 3528502,
      3529401, 3530607, 3534401, 3539103, 3539806, 3543303, 3544103,
      3545001, 3546801, 3547304, 3547809, 3548708, 3548807, 3550308,
      3552502, 3552809,
      # Curitiba
      4100400, 4101804, 4102307, 4103107, 4104006, 4104204, 4105805,
      4106209, 4106902, 4114302, 4119509, 4120804, 4122206, 4125506,
      # Porto Alegre
      4300604, 4303103, 4303905, 4304606, 4307609, 4307708, 4309209,
      4309308, 4313409, 4314902, 4318705, 4319901, 4320008, 4323002
    ),
    legislation = c(
      rep("Lei Complementar 014 (Federal)", 2),     # Belem
      rep("Lei Complementar 014 (Federal)", 5),     # Fortaleza
      rep("Lei Complementar 014 (Federal)", 9),     # Recife
      rep("Lei Complementar 014 (Federal)", 8),     # Salvador
      rep("Lei Complementar 014 (Federal)", 14),    # Belo Horizonte
      rep("Lei Complementar 020 (Federal)", 10),    # Rio
      rep("Lei Complementar 014 (Federal)", 37),    # Sao Paulo
      rep("Lei Complementar 014 (Federal)", 14),    # Curitiba
      rep("Lei Complementar 014 (Federal)", 14)     # Porto Alegre
    ),
    legislation_date = c(
      rep("08.06.1973", 2),
      rep("08.06.1973", 5),
      rep("08.06.1973", 9),
      rep("08.06.1973", 8),
      rep("08.06.1973", 14),
      rep("01.07.1974", 10),
      rep("08.06.1973", 37),
      rep("08.06.1973", 14),
      rep("08.06.1973", 14)
    ),
    year = 1970L,
    stringsAsFactors = FALSE
  )
}


# ==============================================================================
# DePara: mapeamento RMs antigas (SIDRA/BET) -> novas (cod_recmetropol 2022)
# Fonte: DePara_RecortesMetropolitanosEAfins.xlsx no FTP IBGE
# Uso: tabela de lookup para usuario fazer bridge temporal 2019-2020 -> 2021+.
# Cobertura empirica: 86% via SIDRA; 2 RMs ficam orfas (COD 3509=Franca-SP,
# COD 4304=Litoral Norte-RS).
# ==============================================================================

download_metro_area_depara <- function() {
  url <- paste0(
    "https://geoftp.ibge.gov.br/organizacao_do_territorio/",
    "estrutura_territorial/municipios_por_regioes_metropolitanas/",
    "Situacao_2020a2029/DePara_RecortesMetropolitanosEAfins.xlsx"
  )
  tmp <- tempfile(fileext = ".xlsx")
  httr::GET(url = url, httr::progress(),
            httr::write_disk(path = tmp, overwrite = TRUE),
            httr::timeout(300))
  return(tmp)
}

# Parser interno: le uma sheet do DePara e devolve df padronizado
# sheet_type: "sidra_cat" | "bet_cat" | "sidra_subcat" | "bet_subcat"
.parse_depara_sheet <- function(xlsx_path, sheet_name, sheet_type) {
  # Skip row 1 (header manual dentro dos dados)
  raw <- suppressMessages(
    readxl::read_excel(xlsx_path, sheet = sheet_name,
                       skip = 1, col_names = FALSE)
  )
  # Remove linhas totalmente NA (separadores)
  raw <- raw[rowSums(is.na(raw)) < ncol(raw), ]

  # Mapeamento de colunas por tipo de sheet
  # (posicional, dado que os nomes variam entre sheets)
  if (sheet_type == "sidra_cat") {
    # 8 cols: Cod | Nome | NA | cod_rec | cod_cat | nome_cat | label_rec | CodConcat
    df <- data.frame(
      source                = "SIDRA_categ",
      cod_antigo            = suppressWarnings(as.integer(raw[[1]])),
      nome_antigo           = as.character(raw[[2]]),
      cod_rec_metropol      = suppressWarnings(as.integer(raw[[4]])),
      cod_cat_metropol      = suppressWarnings(as.integer(raw[[5]])),
      nome_cat_metropol     = as.character(raw[[6]]),
      label_rec_metropol    = as.character(raw[[7]]),
      cod_subcat_metropol   = NA_integer_,
      nome_subcat_metropol  = NA_character_,
      label_subcat_metropol = NA_character_,
      stringsAsFactors      = FALSE
    )
  } else if (sheet_type == "bet_cat") {
    # 9 cols: NIVEL | CODIGO | NOME | NA | cod_rec | cod_cat | nome_cat | label | CodConcat
    df <- data.frame(
      source                = "BET_categ",
      cod_antigo            = suppressWarnings(as.integer(raw[[2]])),
      nome_antigo           = as.character(raw[[3]]),
      cod_rec_metropol      = suppressWarnings(as.integer(raw[[5]])),
      cod_cat_metropol      = suppressWarnings(as.integer(raw[[6]])),
      nome_cat_metropol     = as.character(raw[[7]]),
      label_rec_metropol    = as.character(raw[[8]]),
      cod_subcat_metropol   = NA_integer_,
      nome_subcat_metropol  = NA_character_,
      label_subcat_metropol = NA_character_,
      stringsAsFactors      = FALSE
    )
  } else if (sheet_type == "sidra_subcat") {
    # 9 cols: CODIGO | NOME | NA | cod_rec | cod_cat | cod_subcat | nome_subcat | label_subcat | CodConcat
    df <- data.frame(
      source                = "SIDRA_subcateg",
      cod_antigo            = suppressWarnings(as.integer(raw[[1]])),
      nome_antigo           = as.character(raw[[2]]),
      cod_rec_metropol      = suppressWarnings(as.integer(raw[[4]])),
      cod_cat_metropol      = suppressWarnings(as.integer(raw[[5]])),
      nome_cat_metropol     = NA_character_,
      label_rec_metropol    = NA_character_,
      cod_subcat_metropol   = suppressWarnings(as.integer(raw[[6]])),
      nome_subcat_metropol  = as.character(raw[[7]]),
      label_subcat_metropol = as.character(raw[[8]]),
      stringsAsFactors      = FALSE
    )
  } else if (sheet_type == "bet_subcat") {
    # 10 cols: NIVEL | CODIGO | NOME | NA | cod_rec | cod_cat | cod_subcat | nome_subcat | label_subcat | CodConcat
    df <- data.frame(
      source                = "BET_subcateg",
      cod_antigo            = suppressWarnings(as.integer(raw[[2]])),
      nome_antigo           = as.character(raw[[3]]),
      cod_rec_metropol      = suppressWarnings(as.integer(raw[[5]])),
      cod_cat_metropol      = suppressWarnings(as.integer(raw[[6]])),
      nome_cat_metropol     = NA_character_,
      label_rec_metropol    = NA_character_,
      cod_subcat_metropol   = suppressWarnings(as.integer(raw[[7]])),
      nome_subcat_metropol  = as.character(raw[[8]]),
      label_subcat_metropol = as.character(raw[[9]]),
      stringsAsFactors      = FALSE
    )
  } else {
    stop("Tipo de sheet desconhecido: ", sheet_type)
  }

  # Remove linhas sem cod_antigo ou sem cod_rec_metropol valido
  df <- df[!is.na(df$cod_antigo) & !is.na(df$cod_rec_metropol), ]
  return(df)
}

clean_metro_area_depara <- function(xlsx_path) {

  dir_clean <- "./data/metro_area_depara"
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  sheet_map <- list(
    list(name = "SIDRA_DePara_CategMetropol",      type = "sidra_cat"),
    list(name = "DownloadsBET_DePara_CategMetrop", type = "bet_cat"),
    list(name = "SIDRA_DePara_SubCategMetropol",   type = "sidra_subcat"),
    list(name = "DownlBET_DePara_SubCategMetropo", type = "bet_subcat")
  )

  dfs <- lapply(sheet_map, function(s) {
    .parse_depara_sheet(xlsx_path, s$name, s$type)
  })
  combined <- do.call(rbind, dfs)
  combined$year <- 2022L

  # Ordem de colunas
  combined <- combined[, c("source",
                           "cod_antigo", "nome_antigo",
                           "cod_rec_metropol",
                           "cod_cat_metropol", "nome_cat_metropol",
                           "label_rec_metropol",
                           "cod_subcat_metropol", "nome_subcat_metropol",
                           "label_subcat_metropol",
                           "year")]

  # Ordenar para legibilidade
  combined <- combined[order(combined$cod_rec_metropol, combined$source,
                             combined$cod_subcat_metropol), ]

  # Validacoes
  stopifnot(nrow(combined) > 0)
  stopifnot(!any(is.na(combined$cod_antigo)))
  stopifnot(!any(is.na(combined$cod_rec_metropol)))
  stopifnot(all(combined$cod_rec_metropol >= 1 & combined$cod_rec_metropol <= 200))

  out_file <- paste0(dir_clean, "/metro_area_depara_clean.parquet")
  arrow::write_parquet(combined, sink = out_file,
                       compression = "zstd", compression_level = 7)

  files <- list.files(dir_clean, pattern = "[.]parquet$",
                      full.names = TRUE, recursive = TRUE)
  return(files)
}
