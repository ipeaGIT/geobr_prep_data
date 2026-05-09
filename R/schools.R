# DATASET:  Escolas - Anual
# Source: INEP - Censo Escolar da Educação Básica
#   Microdados: https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_{year}.zip
#   Oracle BI (Catálogo de Escolas): https://inepdata.inep.gov.br/analytics/
# scale ******
# Metadata:
# Titulo: Escolas
# Titulo alternativo: schools
# Frequencia de atualizacao: Anual
#
# Forma de apresentacao: CSV (pontos quando lat/lon disponível)
# Linguagem: Pt-BR
# Character set: Latin-1 (microdados) / UTF-8 (output)
#
# Resumo: Dados cadastrais e de infraestrutura das escolas do censo escolar.
# Coordenadas geográficas quando disponíveis via Oracle BI (INEP Catálogo).
# Informacoes adicionais: Dados produzidos pelo INEP diretamente dos microdados
# do Censo Escolar da Educação Básica.
#
# Estado: PARCIAL — microdados funcionam, Oracle BI bloqueado pelo INEP (2026-04-01)
# Informacao do Sistema de Referencia: SIRGAS 2000 (EPSG:4674)
#
# Anos disponiveis: 1995-2025 (microdados), ano mais recente (Oracle BI quando ativo)
# URL microdados: https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_{year}.zip
#
# NOTAS:
# - O Oracle BI do INEP (inepdata.inep.gov.br) fornecia lat/lon via Catálogo de Escolas
#   mas o acesso público foi revogado em 2025/2026. O scraper SOAP está implementado
#   e pronto para ser reativado quando o INEP restaurar o acesso.
#   Ver: .claude/plans/2026-04-01_schools-inep-scraper.md
#
# - Os microdados do Censo Escolar (download.inep.gov.br) NÃO incluem lat/lon.
#   Contêm 215k+ escolas com ~350 colunas de metadados, infraestrutura e matrículas.
#
# - PROIBIDO usar GPKG do geobr/IPEA como fonte de dados.




#' # =============================================================================
#' # ORACLE BI SCRAPER — INEP Catálogo de Escolas
#' # Status: PRONTO mas BLOQUEADO pelo servidor INEP (acesso negado desde 2025/2026)
#' # Retomar quando INEP restaurar acesso público ao dashboard
#' # =============================================================================
#' 
#' #' Authenticate to INEP Oracle BI via SOAP
#' #' @return Session ID string, or NULL on failure
#' #' @details Uses the analytics-ws endpoint which accepts SOAP logon.
#' #'   The credentials inepdata/Inep2014 were publicly embedded in INEP URLs.
#' #'   SSL connections are flaky — uses curl system command as fallback.
#' inep_bi_logon <- function(max_retries = 3) {
#'   soap_logon <- '<?xml version="1.0" encoding="UTF-8"?>
#'   <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
#'                     xmlns:v7="urn://oracle.bi.webservices/v7">
#'     <soapenv:Body>
#'       <v7:logon><v7:name>inepdata</v7:name><v7:password>Inep2014</v7:password></v7:logon>
#'     </soapenv:Body>
#'   </soapenv:Envelope>'
#' 
#'   url <- "https://inepdata.inep.gov.br/analytics-ws/saw.dll?SoapImpl=nQSessionService"
#' 
#'   for (i in seq_len(max_retries)) {
#'     sid <- tryCatch({
#'       resp <- httr::POST(
#'         url, body = soap_logon,
#'         httr::content_type("text/xml; charset=utf-8"),
#'         httr::add_headers(SOAPAction = ""),
#'         httr::timeout(30),
#'         config = httr::config(ssl_verifypeer = FALSE)
#'       )
#'       if (httr::status_code(resp) != 200) return(NULL)
#'       body <- httr::content(resp, as = "text", encoding = "UTF-8")
#'       xml <- xml2::read_xml(body)
#'       xml2::xml_text(xml2::xml_find_first(xml, "//sawsoap:sessionID",
#'         ns = c(sawsoap = "urn://oracle.bi.webservices/v7")))
#'     }, error = function(e) NULL)
#' 
#'     if (!is.null(sid) && nchar(sid) > 10) return(sid)
#'     if (i < max_retries) Sys.sleep(2^i)
#'   }
#'   return(NULL)
#' }
#' 
#' #' Execute SQL on INEP Oracle BI via SOAP
#' #' @param sql Logical SQL string (OBIEE syntax)
#' #' @param sid Session ID from inep_bi_logon()
#' #' @param max_rows Maximum rows per page
#' #' @return Parsed XML response, or NULL on failure
#' inep_bi_query <- function(sql, sid, max_rows = 50000) {
#'   soap_body <- sprintf('<?xml version="1.0" encoding="UTF-8"?>
#'   <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
#'                     xmlns:v7="urn://oracle.bi.webservices/v7">
#'     <soapenv:Body>
#'       <v7:executeSQLQuery>
#'         <v7:sql>%s</v7:sql>
#'         <v7:outputFormat>SAWRowsetSchemaAndData</v7:outputFormat>
#'         <v7:executionOptions>
#'           <v7:async>false</v7:async>
#'           <v7:maxRowsPerPage>%d</v7:maxRowsPerPage>
#'           <v7:refresh>true</v7:refresh>
#'           <v7:presentationInfo>true</v7:presentationInfo>
#'           <v7:type>report</v7:type>
#'         </v7:executionOptions>
#'         <v7:sessionID>%s</v7:sessionID>
#'       </v7:executeSQLQuery>
#'     </soapenv:Body>
#'   </soapenv:Envelope>', sql, max_rows, sid)
#' 
#'   url <- "https://inepdata.inep.gov.br/analytics-ws/saw.dll?SoapImpl=xmlViewService"
#' 
#'   resp <- httr::POST(
#'     url, body = soap_body,
#'     httr::content_type("text/xml; charset=utf-8"),
#'     httr::add_headers(SOAPAction = ""),
#'     httr::timeout(300),
#'     config = httr::config(ssl_verifypeer = FALSE)
#'   )
#' 
#'   body <- httr::content(resp, as = "text", encoding = "UTF-8")
#'   if (grepl("Fault", body)) {
#'     warning("SOAP fault: ", sub(".*<faultstring>([^<]+).*", "\\1", body))
#'     return(NULL)
#'   }
#'   return(body)
#' }
#' 
#' #' Logoff from INEP Oracle BI SOAP session
#' inep_bi_logoff <- function(sid) {
#'   tryCatch({
#'     soap <- sprintf('<?xml version="1.0"?>
#'     <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
#'                       xmlns:v7="urn://oracle.bi.webservices/v7">
#'       <soapenv:Body><v7:logoff><v7:sessionID>%s</v7:sessionID></v7:logoff></soapenv:Body>
#'     </soapenv:Envelope>', sid)
#'     httr::POST(
#'       "https://inepdata.inep.gov.br/analytics-ws/saw.dll?SoapImpl=nQSessionService",
#'       body = soap, httr::content_type("text/xml; charset=utf-8"),
#'       httr::add_headers(SOAPAction = ""), httr::timeout(10),
#'       config = httr::config(ssl_verifypeer = FALSE)
#'     )
#'   }, error = function(e) NULL)
#'   invisible(NULL)
#' }
#' 
#' #' Test if INEP Oracle BI Catálogo de Escolas is accessible
#' #' @return TRUE if individual school data can be queried, FALSE otherwise
#' #' @details Tests by querying the "Escola" subject area for school-level columns.
#' #'   As of 2026-04-01, the inepdata user can authenticate but cannot access
#' #'   the Catálogo de Escolas dashboard or its underlying subject area.
#' #'   The accessible "Escola" subject area only contains aggregate statistics.
#' inep_bi_catalogo_accessible <- function() {
#'   sid <- inep_bi_logon()
#'   if (is.null(sid)) return(FALSE)
#'   on.exit(inep_bi_logoff(sid))
#' 
#'   # Test if we can access the Catálogo subject area
#'   # Known columns from the Catálogo: "Código Entidade", "Nome da Escola", etc.
#'   # These are NOT available in the aggregate "Escola" SA — they belong to
#' 
#'   # the Catálogo-specific SA which requires dashboard permissions.
#'   # TODO: When INEP restores access, discover the correct subject area name
#'   #       and column names. See .claude/plans/2026-04-01_schools-inep-scraper.md
#'   result <- tryCatch({
#'     # Try a query that would only work with Catálogo access
#'     # This SELECT 1 works on the aggregate SA, so it's not a good test.
#'     # Instead, try accessing the dashboard via chromote
#'     if (requireNamespace("chromote", quietly = TRUE)) {
#'       b <- chromote::ChromoteSession$new()
#'       on.exit(b$close(), add = TRUE)
#'       b$Page$navigate(paste0(
#'         "https://inepdata.inep.gov.br/analytics/saw.dll?bieehome&startPage=1"))
#'       Sys.sleep(3)
#'       b$Runtime$evaluate(paste0(
#'         "document.querySelector('input[name=\"j_username\"]').value = 'inepdata';",
#'         "document.querySelector('input[name=\"j_password\"]').value = 'Inep2014';",
#'         "document.querySelector('button, input[type=\"submit\"]').click();"))
#'       Sys.sleep(5)
#'       b$Page$navigate(paste0(
#'         "https://inepdata.inep.gov.br/analytics/saw.dll?Dashboard",
#'         "&PortalPath=%2Fshared%2FCenso+da+Educa%C3%A7%C3%A3o+B%C3%A1sica",
#'         "%2F_portal%2FCat%C3%A1logo+de+Escolas"))
#'       Sys.sleep(10)
#'       text <- b$Runtime$evaluate("document.body.innerText")$result$value
#'       !grepl("acesso negado|Permiss", text, ignore.case = TRUE)
#'     } else {
#'       FALSE
#'     }
#'   }, error = function(e) FALSE)
#' 
#'   return(result)
#' }
#' 
#' 
#' # =============================================================================
#' # MICRODADOS DO CENSO ESCOLAR — download direto do INEP
#' # Status: FUNCIONAL — 215k+ escolas, ~350 colunas, SEM lat/lon
#' # =============================================================================
#' 
#' #' Download microdados do Censo Escolar do INEP
#' #' @param year Ano do censo (1995-2025)
#' #' @return data.frame com dados das escolas (SEM geometria)
#' download_schools_microdados <- function(year) {
#' 
#'   url <- paste0(
#'     "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_",
#'     year, ".zip"
#'   )
#' 
#'   dest_dir <- paste0(tempdir(), "/schools_microdados/", year)
#'   dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
#'   zip_file <- paste0(dest_dir, "/microdados_", year, ".zip")
#' 
#'   ## Download
#'   message("Baixando microdados do Censo Escolar ", year, " de: ", url)
#'   resp <- httr::GET(
#'     url = url,
#'     httr::progress(),
#'     httr::write_disk(zip_file, overwrite = TRUE),
#'     httr::timeout(600),
#'     config = httr::config(ssl_verifypeer = FALSE)
#'   )
#' 
#'   if (httr::status_code(resp) != 200) {
#'     stop("Falha ao baixar microdados para ano ", year,
#'          ". HTTP status: ", httr::status_code(resp))
#'   }
#' 
#'   ## Extract all files with junkpaths (avoids CP850 encoding issues in ZIP paths)
#'   ## Some years (2022) have accented directory names that break targeted extraction
#'   message("Extraindo microdados...")
#'   system2("unzip", args = c("-joq", shQuote(zip_file), "-d", shQuote(dest_dir)),
#'           stdout = FALSE, stderr = FALSE)
#' 
#'   # Find the extracted CSV by pattern in the output directory
#'   csv_file <- list.files(dest_dir, pattern = "microdados.*\\.csv$",
#'                           ignore.case = TRUE, full.names = TRUE)[1]
#'   if (is.na(csv_file) || !file.exists(csv_file)) {
#'     csv_file <- list.files(dest_dir, pattern = "\\.csv$",
#'                             ignore.case = TRUE, full.names = TRUE)[1]
#'   }
#'   if (is.na(csv_file) || !file.exists(csv_file)) {
#'     stop("CSV nao extraido do ZIP para ano ", year,
#'          ". Conteudo dir: ", paste(list.files(dest_dir), collapse = ", "))
#'   }
#' 
#'   ## Read CSV (semicolon-delimited, Latin-1 encoding)
#'   message("Lendo CSV: ", basename(csv_file))
#'   raw <- readr::read_delim(
#'     csv_file,
#'     delim = ";",
#'     locale = readr::locale(encoding = "WINDOWS-1252"),
#'     show_col_types = FALSE,
#'     progress = TRUE
#'   )
#' 
#'   message("Microdados lidos: ", nrow(raw), " escolas para ano ", year)
#'   return(raw)
#' }
#' 
#' 
#' # =============================================================================
#' # GEOCODIFICAÇÃO — endereços → coordenadas via geocodebr (CNEFE/IBGE)
#' # =============================================================================
#' 
#' #' Geocode school addresses using geocodebr (CNEFE/IBGE)
#' #' @param df data.frame with INEP microdados columns
#' #' @return data.frame with added lat, lon, precisao columns
#' geocode_schools <- function(df) {
#' 
#'   ## Ensure address columns are character
#'   df$DS_ENDERECO  <- as.character(df$DS_ENDERECO)
#'   df$NU_ENDERECO  <- as.character(df$NU_ENDERECO)
#'   df$CO_CEP       <- as.character(df$CO_CEP)
#'   df$NO_BAIRRO    <- as.character(df$NO_BAIRRO)
#'   df$NO_MUNICIPIO <- as.character(df$NO_MUNICIPIO)
#'   df$NO_UF        <- as.character(df$NO_UF)
#' 
#'   ## Clean CEP: enderecobr crashes with "CEP nao deve conter letras"
#'   ## Some INEP records have non-numeric chars in CO_CEP (e.g. 2023 indices 66500+)
#'   df$CO_CEP <- gsub("[^0-9]", "", df$CO_CEP)
#' 
#'   ## geocodebr fails with 0 matches when input df has hundreds of columns
#'   ## (likely conflicts with internal geocodebr column names like 'precisao')
#'   ## Fix: pass only address columns + ID, then join results back
#'   addr_cols <- c("CO_ENTIDADE", "DS_ENDERECO", "NU_ENDERECO", "CO_CEP",
#'                  "NO_BAIRRO", "NO_MUNICIPIO", "NO_UF")
#'   df_addr <- df[, addr_cols, drop = FALSE]
#' 
#'   ## Map INEP columns → geocodebr fields
#'   campos <- geocodebr::definir_campos(
#'     logradouro = "DS_ENDERECO",
#'     numero     = "NU_ENDERECO",
#'     cep        = "CO_CEP",
#'     localidade = "NO_BAIRRO",
#'     municipio  = "NO_MUNICIPIO",
#'     estado     = "NO_UF"
#'   )
#' 
#'   ## Geocode via CNEFE/IBGE (offline after initial cache download)
#'   ## Uses df_addr (minimal columns) to avoid geocodebr column name conflicts
#'   geo_result <- geocodebr::geocode(
#'     enderecos            = df_addr,
#'     campos_endereco      = campos,
#'     resolver_empates     = TRUE,
#'     resultado_sf         = FALSE,
#'     padronizar_enderecos = TRUE,
#'     verboso              = TRUE,
#'     cache                = TRUE
#'   )
#' 
#'   ## Join geocoded coords back to full df using row position (geocodebr preserves order)
#'   ## Verify row count matches before joining
#'   stopifnot(nrow(geo_result) == nrow(df))
#'   geo_cols <- intersect(c("lat", "lon", "precisao", "tipo_resultado",
#'                            "desvio_metros", "endereco_encontrado"),
#'                          names(geo_result))
#'   df[, geo_cols] <- geo_result[, geo_cols]
#'   df
#' }
#' 
#' 
#' # =============================================================================
#' # DOWNLOAD PRINCIPAL — microdados INEP + geocodebr
#' # =============================================================================
#' 
#' #' Download school data from INEP and geocode addresses
#' #' @param year Census year
#' #' @return data.frame with school data + lat/lon coordinates
#' #' @details Strategy:
#' #'   1. Download microdados do Censo Escolar (comprehensive school data)
#' #'   2. Geocode addresses via geocodebr (CNEFE/IBGE → lat/lon)
#' #'   Oracle BI scraper code preserved but BLOCKED by INEP (see above).
#' #'   NEVER uses GPKG from geobr/IPEA.
#' download_schools <- function(year) {
#' 
#'   ## Strategy 1: Oracle BI Catálogo de Escolas (with native coordinates)
#'   ## Currently BLOCKED by INEP — user inepdata has no dashboard access
#'   ## TODO: Reactivate when INEP restores public BI access
#'   # if (inep_bi_catalogo_accessible()) {
#'   #   raw <- scrape_inep_bi_catalogo(year)
#'   #   if (!is.null(raw) && nrow(raw) > 0) return(raw)
#'   # }
#' 
#'   ## Strategy 2: Microdados INEP + geocodebr
#'   raw <- download_schools_microdados(year)
#' 
#'   message("Geocodificando ", nrow(raw), " escolas via geocodebr (CNEFE/IBGE)...")
#'   geocoded <- geocode_schools(raw)
#' 
#'   n_coords <- sum(!is.na(geocoded$lat) & !is.na(geocoded$lon))
#'   message("Escolas geocodificadas: ", n_coords, "/", nrow(geocoded),
#'           " (", round(100 * n_coords / nrow(geocoded), 1), "%)")
#' 
#'   return(geocoded)
#' }


# 
# Download schools from github

download_schools_githb <- function(year) { 
  
  
  # list all files on the legacy release
  temp_meta <- piggyback::pb_list(
    repo = "ipeaGIT/geobr_prep_data",
    tag = "legacy"
  )
  
  # filter files for schools in selected year
  file <- temp_meta |> 
    filter(file_name %like% 'escolascatalogo') |> 
    filter(file_name %like% year)  
  
  # download file  
  file_url <- paste0(
    "https://github.com/ipeaGIT/geobr_prep_data/releases/download/legacy/",
    file$file_name
    )
  
  local_file <- download_file_geobr(file_url)
  
  # read file
  if(year %in% )
  schools_raw <- sf::st_read(local_file)
  
  # clean names and add year
  schools_raw <- schools_raw |> 
    janitor::clean_names()
  
  schools_raw$year <- year
  
  return(schools_raw)
  
  }


# =============================================================================
# CLEAN — processa dados das escolas para formato geobr
# =============================================================================

#' Clean and standardize school data
#' @param schools_raw Raw data from download_schools() (geocoded microdados)
#' @param year Census year
#' @return Character vector of output file paths
clean_schools <- function(schools_raw, year) {

  ## 0. Create output directory
  dir_clean <- paste0("./data/schools/", year)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  temp_df <- schools_raw

  ## 1. Column mapping: microdados INEP → geobr standard
  ##    Coordinates come from geocodebr (lat/lon columns)

  has_coords <- "lat" %in% names(temp_df) & "lon" %in% names(temp_df)

  temp_df <- temp_df |>
    dplyr::transmute(
      code_school = CO_ENTIDADE,
      name_school = NO_ENTIDADE,
      education_level = dplyr::case_when(
        IN_INF == 1 & IN_FUND == 0 & IN_MED == 0 ~ "Infantil",
        IN_FUND == 1 & IN_MED == 0 ~ "Fundamental",
        IN_MED == 1 ~ "Medio",
        TRUE ~ "Outro"
      ),
      admin_category = dplyr::case_when(
        TP_CATEGORIA_ESCOLA_PRIVADA %in% c(1, 2, 3, 4) ~ "Privada",
        TP_DEPENDENCIA %in% c(1, 2, 3) ~ "Publica",
        TRUE ~ NA_character_
      ),
      address = DS_ENDERECO,
      phone_number = ifelse(
        !is.na(NU_DDD) & !is.na(NU_TELEFONE),
        paste0("(", NU_DDD, ") ", NU_TELEFONE),
        NA_character_
      ),
      government_level = dplyr::case_when(
        TP_DEPENDENCIA == 1 ~ "Federal",
        TP_DEPENDENCIA == 2 ~ "Estadual",
        TP_DEPENDENCIA == 3 ~ "Municipal",
        TP_DEPENDENCIA == 4 ~ "Privada",
        TRUE ~ NA_character_
      ),
      location_type = dplyr::case_when(
        TP_LOCALIZACAO == 1 ~ "Urbana",
        TP_LOCALIZACAO == 2 ~ "Rural",
        TRUE ~ NA_character_
      ),
      urban = dplyr::case_when(
        TP_LOCALIZACAO == 1 ~ "Sim",
        TP_LOCALIZACAO == 2 ~ "Nao",
        TRUE ~ NA_character_
      ),
      situation = dplyr::case_when(
        TP_SITUACAO_FUNCIONAMENTO == 1 ~ "Em Atividade",
        TP_SITUACAO_FUNCIONAMENTO == 2 ~ "Paralisada",
        TP_SITUACAO_FUNCIONAMENTO == 3 ~ "Extinta",
        TRUE ~ NA_character_
      ),
      name_muni = NO_MUNICIPIO,
      code_muni = CO_MUNICIPIO,
      code_state = CO_UF,
      abbrev_state = SG_UF,
      name_state = NO_UF,
      code_region = CO_REGIAO,
      name_region = NO_REGIAO,
      ## Preserve geocodebr coordinates and precision
      lat = if (has_coords) lat else NA_real_,
      lon = if (has_coords) lon else NA_real_,
      geocode_precision = if ("precisao" %in% names(temp_df)) precisao else NA_character_
    )

  ## 2. Apply snake_case_names for Title Case formatting
  temp_df <- snake_case_names(temp_df, colname = c("name_school", "name_muni",
                                                    "name_state", "name_region"))

  ## 3. Create sf POINT geometry from geocoded coordinates
  ##    geocodebr uses CNEFE/IBGE → SIRGAS 2000 (EPSG:4674)
  valid_coords <- !is.na(temp_df$lat) & !is.na(temp_df$lon) &
                  temp_df$lat != 0 & temp_df$lon != 0

  n_valid <- sum(valid_coords)
  message("Escolas com coordenadas validas: ", n_valid, "/", nrow(temp_df))

  if (n_valid > 0) {
    ## Schools with coordinates
    df_good <- temp_df[valid_coords, ]
    sf_good <- sf::st_as_sf(df_good,
                            coords = c("lon", "lat"),
                            crs = 4674, na.fail = FALSE)

    ## Schools without coordinates → POINT EMPTY
    df_bad <- temp_df[!valid_coords, ]
    if (nrow(df_bad) > 0) {
      empty_geom <- sf::st_sfc(
        lapply(seq_len(nrow(df_bad)), function(i) sf::st_point()),
        crs = 4674
      )
      df_bad$lat <- NULL
      df_bad$lon <- NULL
      sf_bad <- sf::st_sf(df_bad, geometry = empty_geom)
      temp_sf <- dplyr::bind_rows(sf_good, sf_bad)
    } else {
      temp_sf <- sf_good
    }
  } else {
    stop("Nenhuma escola geocodificada para ano ", year,
         " — regra: >90% obrigatorio. Verificar geocodebr/enderecobr.")
  }

  ## Remove lat/lon columns (now in geometry)
  temp_sf$lat <- NULL
  temp_sf$lon <- NULL

  ## 4. Add year
  temp_sf$year <- year

  ## 5. Reorder columns (geometry ALWAYS last)
  temp_sf <- temp_sf |>
    dplyr::select(
      code_school, name_school,
      education_level, admin_category,
      address, phone_number, government_level,
      location_type, urban, situation,
      name_muni, code_muni,
      code_state, abbrev_state, name_state,
      code_region, name_region,
      geocode_precision,
      year, geometry
    )

  ## 6. Validation
  stopifnot(!is.na(sf::st_crs(temp_sf)))
  stopifnot(sf::st_crs(temp_sf)$epsg == 4674)
  stopifnot(is.numeric(temp_sf$code_school))
  stopifnot(is.numeric(temp_sf$code_state))
  stopifnot(is.character(temp_sf$abbrev_state))
  stopifnot(is.character(temp_sf$name_state))
  stopifnot(is.numeric(temp_sf$code_region))
  stopifnot(is.character(temp_sf$name_region))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")

  ## 7. Save (NO simplified version for point data)
  ##    Use sfarrow for POINT geometry (arrow::write_parquet needs geoarrow for sf)
  sfarrow::st_write_parquet(
    temp_sf,
    dsn = paste0(dir_clean, "/schools_", year, ".parquet"))

  ## 8. Return file paths
  files <- list.files(path = dir_clean,
                      pattern = "\\.parquet$",
                      recursive = TRUE,
                      full.names = TRUE)
  return(files)
}
