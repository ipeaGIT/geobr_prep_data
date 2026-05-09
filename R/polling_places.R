#> DATASET: polling places
#> Source: TSE — Tribunal Superior Eleitoral
#> Metadata:
# Titulo: Locais de votacao
# Titulo alternativo: electoral places / polling places
# Frequencia de atualizacao: a cada 2 anos (anos eleitorais)
#
# Forma de apresentacao: Points (CSV com coordenadas)
# Linguagem: Pt-BR
# Character set: Latin-1
#
# Resumo: Pontos dos locais de votacao do Brasil
# Informacoes adicionais: Dados produzidos pelo TSE, eleitorado por local de votacao.


# Download the data  -----------------------------------------------------------
download_pollingplaces <- function(year){ # year = 2024

  ## 0. Generate the correct ftp link ------------------------------------------

  # Error conditions
  if(year < 2010){
    stop("Geobr: Polling place data is available for all general and local elections starting in 2010.")}
  if(year %% 2 != 0){
    stop("Geobr: Please, review the year input. Electoral data is available even-numbered years.")
    }

  url_start <- "https://cdn.tse.jus.br/estatistica/sead/odsele/eleitorado_locais_votacao/eleitorado_local_votacao"

  ftp_link <- paste0(url_start, "_", year, ".zip")

  ## 1. Create temp folder -----------------------------------------------------

  zip_dir <- paste0(tempdir(), "/polling_places/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)

  ## 2. Download Raw data ------------------------------------------------------
  
  file_raw <- download_file_geobr(
    file_url = ftp_link,
    dest_dir = zip_dir
  )
  
  # 4. Unzip Raw data
  file <- unzip_geobr(zip_dir = zip_dir)

  ## 5. Read CSV data ----------------------------------------------------------

  csv_file <- file[file %like% ".csv"]

  pollingplaces_raw <- data.table::fread(
    input = csv_file,
    encoding = "Latin-1",
    sep = ";") |>
    janitor::clean_names()
  
  pollingplaces_raw$year <- year
  
  return(pollingplaces_raw)
}



# crosswall of code_muni tse_ibge  --------------------------------------------------------
get_crosswalk_tse_ibge <- function() {
  
  crosswalk_url = "https://raw.githubusercontent.com/GV-CEPESP/cepespdata/refs/heads/main/tabelas_auxiliares/dados/codigo_municipio_ibge_tse.csv"
  
  # read crosswalk (original source is CEPESP data) Obrigado Gelape!
  crosswalk_url <- download_file_geobr(crosswalk_url)
  
  crosswalk <- data.table::fread(crosswalk_url) |> 
    janitor::clean_names() |> 
    select(code_muni_tse = cod_mun_tse , code_muni = cod_mun_ibge)
  
  return(crosswalk)
}


# Clean polling Places  --------------------------------------------------------

# pollingplaces_raw <- tar_read(pollingplaces_raw, branches = 1)

clean_pollingplaces <- function(pollingplaces_raw, crosswalk_tse_ibge){

  ## 0. Create folder to save clean data ---------------------------------------

  yyyy <- pollingplaces_raw$year[1]
  dir_clean <- paste0("./data/polling_places/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  
  ## 1. Aggregate to unique polling places and recode columns ------------------
  
  pollingplaces <- pollingplaces_raw |>
    dplyr::filter(nr_turno == 1, cd_situ_local_votacao == 1) |>
    dplyr::select(code_muni_tse = cd_municipio,
                  name_muni_tse = nm_municipio, 
                  abbrev_state = sg_uf, 
                  dt_eleicao,
                  nr_zona, 
                  # nr_secao,
                  # cd_tipo_secao_agregada,
                  # ds_tipo_secao_agregada,
                  # nr_secao_principal,
                  nr_local_votacao, 
                  nm_local_votacao, 
                  cd_tipo_local,
                  ds_tipo_local, 
                  cd_situ_local_votacao,
                  ds_situ_local_votacao,
                  cd_situ_zona,
                  # cd_situ_secao,
                  # ds_situ_secao,
                  # cd_situ_secao_acessibilidade,
                  # ds_situ_secao_acessibilidade,
                  nr_local_votacao_original,
                  nm_local_votacao_original,
                  qt_eleitor_secao,
                  ds_endereco, 
                  nm_bairro, 
                  nr_cep, 
                  lat_tse = nr_latitude, 
                  lon_tse = nr_longitude,
                  ) |>
    dplyr::group_by_all() |> 
    dplyr::summarise(total_eleitores = sum(qt_eleitor_secao),
                     .groups = "drop")


  
  # add geocodebr coordinates --------------------------------------------------
  
  # separate logradouro and numero columns
  # this  attempt doesn't solve all issue but already makes a huger difference
  temp_address <- pollingplaces |>
    mutate(
      numero = case_when(
        str_detect(ds_endereco, regex("\\bS/N\\b", ignore_case = TRUE)) ~ "S/N",
        
        str_detect(ds_endereco, regex("\\bN\\s*\\d+", ignore_case = TRUE)) ~
          str_match(ds_endereco, regex("\\bN\\s*(\\d+)", ignore_case = TRUE))[, 2],
        
        str_detect(ds_endereco, "\\d{2,}\\s*$") ~
          str_extract(ds_endereco, "\\d{2,}\\s*$") |> str_trim(),
        
        str_detect(ds_endereco, ",") ~
          str_trim(str_extract(ds_endereco, "(?<=,).*")),
        
        TRUE ~ NA_character_
      ),
      
      logradouro = ds_endereco |>
        str_remove(regex("\\bS/N\\b", ignore_case = TRUE)) |>
        str_remove(regex("\\bN\\s*\\d+.*$", ignore_case = TRUE)) |>
        str_remove("\\d{2,}\\s*$") |>
        str_extract("^[^,]+") |>
        str_trim()
    ) |> 
    select(nr_zona, nr_local_votacao, ds_endereco,logradouro, numero, 
           nm_bairro, nr_cep, name_muni_tse, abbrev_state,
           -ds_endereco
           ) |> 
    unique()
  
  fields <- geocodebr::definir_campos(
    estado = 'abbrev_state',
    municipio = 'name_muni_tse',
    logradouro = 'logradouro',
    numero = 'numero',
    cep = 'nr_cep',
    localidade = 'nm_bairro'
  )
  
  # apply enc2utf8() to all character columns
  # see https://github.com/ipeaGIT/enderecobr/issues/66
  data.table::setDT(temp_address)
  char_cols <- names(temp_address)[vapply(temp_address, is.character, logical(1))]
  temp_address[, (char_cols) := lapply(.SD, enc2utf8), .SDcols = char_cols]  
  
  temp_geo <- geocodebr::geocode(
    enderecos = temp_address, 
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
  df <- dplyr::left_join(x = pollingplaces, y = temp_geo)
  
  
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
  df[ lat_tse != -1 , dist := dt.haversine(lat_tse, lon_tse, lat_geocodebr, lon_geocodebr)]
  
  
  # decide which spatial coordinates to use --------------------------------------------------
  
  # Criteria:
  # 1. if distance between sources < 800, use official source
  # 2. if distance between sources > 800 & desvio_metros_geocodebr < 800, use geocodebr
  # 3. if official coords missing, use geocodebr
  # * (custom) voting places oversees, use official source 

  df[, coords_source := fcase(
    dist < 800, 'tse', 
    dist > 800 & desvio_metros_geocodebr < 800, 'geocodebr',
    lat_tse == -1, 'tse',
    is.na(lat_tse), 'geocodebr', 
    default = 'tse'  # abbrev_state ==  "ZZ", -1 # voting places overseas
    )]
  
  # janitor::tabyl(df$coords_source)
  # 
  #  df$coords_source      n    percent
  #         geocodebr  21012 0.06110822
  #               tse 322837 0.93889178
  
  df[, lat := ifelse(coords_source=="tse", lat_tse, lat_geocodebr)]
  df[, lon := ifelse(coords_source=="tse", lon_tse, lon_geocodebr)]
  
  # drop columns created in the geocoding part
  df <- df |> 
    select(-dist, - logradouro, -numero)
  
  ## 2. Create sf object from coordinates --------------------------------------
  
  data.table::setDF(df)
  pollingplaces_sf <- sfheaders::sf_point(
    obj = df,
    x = 'lon',
    y = 'lat', 
    keep = TRUE
  )
  
  sf::st_crs(pollingplaces_sf) <- 4674
  
  
  # add code_muni from IBGE  --------------------------------------
  temp_sf <- left_join(pollingplaces_sf, crosswalk_tse_ibge) |> 
    mutate(
      code_muni = as.numeric(code_muni),
      code_state = substr(code_muni, 1, 2)
      ) |> 
    rename(name_muni = name_muni_tse) 
    
  
  
  ## 3. Apply harmonize geobr cleaning -----------------------------------------

  temp_sf <- harmonize_geobr(
    temp_sf = temp_sf,
    year = yyyy,
    add_state = TRUE,
    state_column = 'code_state',
    add_region = TRUE, 
    region_column = 'code_state',
    add_snake_case = TRUE,
    snake_colname = "name_muni",
    projection_fix = TRUE,
    encoding_utf8 = TRUE,
    topology_fix = FALSE,
    remove_z_dimension = TRUE,
    use_multipolygon = FALSE
  )
  
  # re-order columns
  temp_sf <- temp_sf |> 
    # first columns
    dplyr::relocate(code_muni,
                    name_muni,
                    code_muni_tse,
                    nr_zona 
                    ) |> 
    # last columns
    dplyr::relocate(code_state,
                    name_state,
                    code_region,
                    name_region,
                    lat_tse, 
                    lon_tse,
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
    dplyr::arrange(code_state, code_muni, nr_zona, nr_local_votacao)
  
  

  ## 4. Save datasets ----------------------------------------------------------

  ### Save main parquet
  write_geobr_parquet(
    sf_obj = temp_sf,
    path = paste0(dir_clean, "/pollingplaces_", yyyy, ".parquet"))

  ## 5. Return file list -------------------------------------------------------

  files <- list.files(path = dir_clean,
                      pattern = "\\.parquet$",
                      recursive = TRUE,
                      full.names = TRUE)

  return(files)
}


