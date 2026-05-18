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


#' Download microdados do Censo Escolar do INEP  -------------------------------------------
#' @param year Ano do censo (1995-2025)
#' @return data.frame com dados das escolas (SEM geometria)
download_schools_microdados <- function(year) {

  url <- paste0(
    "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_",
    year, ".zip"
  )

  if(year >= 2025) {
    
    url <- paste0(
      "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_",
      year, "_.zip"
    )
  }
  
  dest_dir <- paste0(tempdir(), "/schools_microdados/", year)
  dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
  zip_file <- paste0(dest_dir, "/microdados_", year, ".zip")

  ## Download
  file <- download_file_geobr(
    file_url = url, 
    dest_dir = dest_dir
    )

  ## Extract all files with junkpaths (avoids CP850 encoding issues in ZIP paths)
  ## Some years (2022) have accented directory names that break targeted extraction
  message("Extraindo microdados...")
  files <- unzip_geobr(dest_dir)


  # Find the extracted CSV by pattern in the output directory
  csv_file <- files[files %like% "microdados"]
  csv_file <- csv_file[csv_file %like% ".csv|.CSV"]
  
  if (year > 2022) {
    csv_file <- csv_file[csv_file %like% "ed_basica|Tabela_Escola"]
  }
  
  ## Read CSV (semicolon-delimited, Latin-1 encoding)
  message("Lendo CSV: ", basename(csv_file))
  schools_raw <- readr::read_delim(
    csv_file,
    delim = ";",
    locale = readr::locale(encoding = "WINDOWS-1252"),
    show_col_types = FALSE,
    progress = TRUE
  )

  schools_raw <- schools_raw |> 
    janitor::clean_names()
  
  schools_raw$year <- year
  
  if(nrow(schools_raw)==0){stop(paste0("Error downloading school data ", year))}
  a <- 3+3
  
  return(schools_raw)
}




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


# Download schools from github legacy catalogo de escolas --------------------
download_schools_githb <- function(year) {

  closest_number <- function(x, candidates) {
    candidates[order(abs(candidates - x), -candidates)][1]
  }
  
  yyyy <- closest_number(year, c(2020, 2024, 2026))
  
  
  ## legacy
  # list all files on the legacy release
  temp_meta <- piggyback::pb_list(
    repo = "ipeaGIT/geobr_prep_data",
    tag = "legacy"
  )

  # filter files for schools in selected year
  file <- temp_meta |>
    filter(file_name %like% 'escolascatalogo') |>
    filter(file_name %like% yyyy)

  # download file
  file_url <- paste0(
    "https://github.com/ipeaGIT/geobr_prep_data/releases/download/legacy/",
    file$file_name
    )

  local_file <- download_file_geobr(file_url)

  # read file
  schools_raw <- data.table::fread(local_file)

  # clean names and add year
  schools_catalogue <- schools_raw |>
    janitor::clean_names() |> 
    dplyr::select(abbrev_state, name_muni, code_school, longitude, latitude)

  schools_catalogue$yyyy <- yyyy

  return(schools_catalogue)

  }



#' Clean and standardize school data -------------------------------------------
#' @param schools_raw Raw data from download_schools() (geocoded microdados)
#' @param year Census year
#' @return Character vector of output file paths
#' 

# schools_raw <- tar_read(schools_raw, 1)
clean_schools <- function(schools_raw) {

  ## 0. Create output directory
  yyyy <- schools_raw$year[1]
  dir_clean <- paste0("./data/schools/")
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)
  
  
  ## 1. Column mapping: microdados INEP
  temp_df <- schools_raw |>
    dplyr::select(
      code_school = co_entidade,
      name_school = no_entidade,
      name_state = no_uf,
      abbrev_state = sg_uf,
      code_state = co_uf,
      name_muni = no_municipio,
      code_muni = co_municipio,
      # no_mesorregiao,
      # co_mesorregiao,
      # no_microrregiao,
      # co_microrregiao,
      # co_distrito,
      tp_dependencia,
      tp_categoria_escola_privada,
      tp_situacao_funcionamento,
      dplyr::any_of(c("latitude", "longitude")),
      tp_localizacao,
      tp_localizacao_diferenciada,
      ds_endereco,
      nu_endereco,
      ds_complemento,
      no_bairro,
      co_cep
      ) |> 
    unique()

  
  
  # coordenadas INEP ----------------------------------------------------------
  has_coords <- any(names(temp_df) %like% "latitude")
  
  # Se dados ja trazem coordendas, soh renomeia colunas
  if (isTRUE(has_coords)) {
    temp_df <- temp_df |> 
      dplyr::rename(lat_inep = latitude, lon_inep = longitude)
  }
  
  # Se nao trazem coordendas, entao traz coordenadas do catologo
  if (isFALSE(has_coords)) {
    
    cat <- download_schools_githb(yyyy)
  
    temp_df <- dplyr::left_join(temp_df, cat) |> 
      dplyr::rename(lon_inep = longitude, lat_inep = latitude) |> 
      select(-yyyy)
    
  }
  
  # geocodebr coordinates --------------------------------------------------
  
  fields <- geocodebr::definir_campos(
    estado = 'abbrev_state',
    municipio = 'name_muni',
    logradouro = 'ds_endereco',
    numero = 'nu_endereco',
    cep = 'co_cep',
    localidade = 'no_bairro'
  )
  
  # apply enc2utf8() to all character columns
  # see https://github.com/ipeaGIT/enderecobr/issues/66
  data.table::setDT(temp_df)
  char_cols <- names(temp_df)[vapply(temp_df, is.character, logical(1))]
  temp_df[, (char_cols) := lapply(.SD, enc2utf8), .SDcols = char_cols]  
  
  temp_geo <- geocodebr::geocode(
    enderecos = temp_df, 
    campos_endereco = fields
  )
  
  # edita nome de colunas do geocodebr
  temp_geo <- temp_geo |> 
    dplyr::rename(lat_geocodebr = lat, 
                  lon_geocodebr = lon, 
                  precisao_geocodebr = precisao, 
                  tipo_resultado_geocodebr = tipo_resultado, 
                  desvio_metros_geocodebr = desvio_metros) |> 
    select(-endereco_encontrado)
  
  
  # traz as colunas do geocodebr para a tabela principal
  df <- dplyr::left_join(x = temp_df, y = temp_geo)
  
  
  # calcula distancia entre coordenadas oficias e do geocodebr
  # ignora pontos onde lat é missing -1
  dt.haversine <- function(lat_from, lon_from, lat_to, lon_to, r = 6378137){
    radians <- pi/180
    lat_to <- lat_to * radians
    lat_from <- lat_from * radians
    lon_to <- lon_to * radians
    lon_from <- lon_from * radians
    dLat <- (lat_to - lat_from)
    dLon <- (lon_to - lon_from)
    a <- (sin(dLat/2)^2) + (cos(lat_from) * cos(lat_to)) * (sin(dLon/2)^2)
    return(2 * atan2(sqrt(a), sqrt(1 - a)) * r)
  }
  
  data.table::setDT(df)
  df[, dist := dt.haversine(lat_inep, lon_inep, lat_geocodebr, lon_geocodebr)]
  
  
  # decide which spatial coordinates to use --------------------------------------------------
  
  # Criteria:
  # 1. if distance between sources < 800, use official source
  # 2. if distance between sources > 800 & desvio_metros_geocodebr < 800, use geocodebr
  # 3. if official coords missing, use geocodebr
  
  df[, coords_source := fcase(
    dist < 800, 'inep', 
    dist > 800 & desvio_metros_geocodebr < 800, 'geocodebr',
    is.na(lat_inep), 'geocodebr', 
    default = 'inep'  # abbrev_state ==  "ZZ", -1 # voting places overseas
  )]
  
  # janitor::tabyl(df$coords_source)
  # 
  #  df$coords_source      n    percent
  #         geocodebr  21012 0.06110822
  #               tse 322837 0.93889178
  
  df[, lat := ifelse(coords_source=="inep", lat_inep, lat_geocodebr)]
  df[, lon := ifelse(coords_source=="inep", lon_inep, lon_geocodebr)]
  
  # drop columns created in the geocoding part
  df <- df |> 
    dplyr::select(-dist, -dplyr::any_of("yyyy"))
  
  ## 2. Create sf object from coordinates --------------------------------------
  
  data.table::setDF(df)
  schools_sf <- sfheaders::sf_point(
    obj = df,
    x = 'lon',
    y = 'lat', 
    keep = TRUE
  )
  
  sf::st_crs(schools_sf) <- 4674
  
  
  
  ## 3. Apply harmonize geobr cleaning -----------------------------------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = schools_sf,
    year = yyyy,
    add_state = T,
    state_column = 'code_state',
    add_region = T, 
    region_column = 'code_state',
    add_snake_case = TRUE,
    snake_colname = "name_muni",
    projection_fix = TRUE,
    encoding_utf8 = TRUE,
    topology_fix = FALSE,
    remove_z_dimension = TRUE,
    use_multipolygon = FALSE
  )  
  

  ## 5. Reorder columns (geometry ALWAYS last)
  temp_sf <- temp_sf |> 
    # first columns
    dplyr::relocate(code_muni,
                    name_muni,
                    code_school,
                    name_school 
    ) |> 
    # last columns
    dplyr::relocate(code_state,
                    name_state,
                    code_region,
                    name_region,
                    lat_inep, 
                    lon_inep,
                    lat_geocodebr,
                    lon_geocodebr,
                    precisao_geocodebr,
                    tipo_resultado_geocodebr,
                    desvio_metros_geocodebr,
                    coords_source,
                    year, 
                    geometry,
                    .after = last_col())
  
  
  # sort by key columns
  temp_sf <- temp_sf |>
    dplyr::arrange(code_state, code_muni, code_school)
  
  
  
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

  ## 7. Save datasets  ---------------------------------------------------------
  
  ### Save in parquet
  write_geobr_parquet(
    sf_obj = temp_sf,
    path = paste0(dir_clean, "/schools_", yyyy, ".parquet")
  )
  
  
  ## 7. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean,
                      pattern = ".parquet",
                      recursive = TRUE,
                      full.names = TRUE)
  
  return(files)
}