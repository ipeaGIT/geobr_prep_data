#> DATASET: Estabelecimentos de Saúde
#> Source: Portal de Dados Abertos do Ministério da Saúde
#: ##### scale 1:5.000.000
#> Metadata:
# Título: Estabelecimentos de Saúde CNES
# Título alternativo: health facilities
# Frequência de atualização: ##########
# Forma de apresentação: Points
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Localização dos estabelecimentos registrados no Cadastro Nacional de Estabelecimentos de Saúde - CNES.
# Informações adicionais: Dados produzidos pelo Ministério da Saúde.
# Propósito: Identificação dos estabelecimentos de saúde.
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas:****
# Informação do Sistema de Referência: ##### SIRGAS 2000
#
# Observações: Anos disponíveis: ###########?

# Download the data  -----------------------------------------------------------
# year <- c(format(Sys.Date(), "%Y%m"))
download_healthfacilities <- function(year){ #  year= 202601 
  
  ## 0. Create temp folders and data folders -----------------------------------
  
  zip_dir <- paste0(tempdir(), "/health_facilities/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  ## 1. Get download link ------------------------------------------------------
  
  # Source:
  # "https://dados.gov.br/dados/conjuntos-dados/cnes-cadastro-nacional-de-estabelecimentos-de-saude"
  # file_url = 'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/CNES/cnes_estabelecimentos.zip'
  # file_url <- "s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/CNES/cnes_estabelecimentos_csv.zip"
  
  ftp_url <- "ftp://ftp.datasus.gov.br/cnes/"
    
  # list all files in ftp
  txt <- read_html(ftp_url) |>
    html_element("p") |>
    html_text()
  
 all_files <- str_split(txt, "\r?\n") |>
    unlist() |>
    str_trim()
 
 # file name
 target_file_name <- paste0("BASE_DE_DADOS_CNES_", year)
 
 # check if date is avilable
 file_exist <- any(all_files %like% target_file_name) 
 
 # if file does not exist yet, return informative message
 if (isFALSE(file_exist)) {
   msg <- paste("Data not available for the required date", year)
   return(msg)
   }
   
 
 ## 3. Download Raw data ------------------------------------------------------
 file_url <- paste0(ftp_url, target_file_name, ".ZIP")
 
 file_raw <- download_file_geobr(
    file_url = file_url, 
    dest_dir = zip_dir
  )
  
  
  ## 4. Unzip Raw data ---------------------------------------------------------
  
 files_unzipped <- unzip(file_raw, exdir = out_zip)

 
 ## 5. Read file and clean collumns names -------------------------------------
 
 estab_file <- files_unzipped[files_unzipped %like% "tbEstabelecimento"]

 healthfacilities_raw <- data.table::fread(
   file = estab_file,
   encoding = "UTF-8",
   integer64 = "character"
   ) |> 
   janitor::clean_names() |> 
   dplyr::select( -dplyr::any_of(c('no_complemento', 'nu_telefone', 'nu_fax', 'no_email', 'no_url'))
                  )
 
  ## 6. Show result ------------------------------------------------------------
  
  healthfacilities_raw$date_update <- year

  return(healthfacilities_raw)
}

# Clean the data  --------------------------------------------------------------
# healthfacilities_raw <- tar_read(healthfacilities_raw)
# municipality_clean <- tar_read(municipality_clean)

clean_healthfacilities <- function(healthfacilities_raw, municipality_clean){

  # check if data raw was created
  if(inherits(healthfacilities_raw, "character")){
    message(healthfacilities_raw)
    return(NULL)
  }
  
  
  ## 0. Adjust date of last update ---------------------------------------------
  
  date_update <- healthfacilities_raw$date_update[1]
  yyyy <- year_muni <- substring(date_update, 1,4)
  
  
  
  ## 1. Create folder to save clean data ---------------------------------------
  
  dir_clean <- paste0("./data/health_facilities/")
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  
  ## 3. Adjusts collumns names, reorder, add geocodebr -------------------------
  
  healthfacilities <- healthfacilities_raw |> 
    dplyr::rename(
            code_state = 'co_estado_gestor',
            code_muni6 = 'co_municipio_gestor',
            cep = 'co_cep',
            logradouro = 'no_logradouro',
            numero_endereco = 'nu_endereco',
            bairro = 'no_bairro'
            ) |> 
    dplyr::mutate(co_cnes = sprintf("%07d", co_cnes), # fix co_cnes to 7 digits
           month_update = format(Sys.Date(), "%m"),
           year_update = format(Sys.Date(), "%Y"),
           lon_cnes = nu_longitude,
           lat_cnes = nu_latitude
           ) |> 
    dplyr::relocate(any_of(c("code_muni", "code_state", "abbrev_state", "name_state",
                      "code_region", "name_region", "date_update")),
             .after = co_cnes)
    
  
  # recover 7-digit code_muni
  if(yyyy == lubridate::year(Sys.Date())) { year_muni <- yyyy -2 }
  
  munis <- municipality_clean[grep(year_muni, municipality_clean)]
  codigos_muni <- munis[!grepl('simplified', munis)] |>
    arrow::open_dataset() |>
    sf::st_as_sf() |>
    sf::st_drop_geometry() |>
    dplyr::select(code_muni, name_muni) |> 
    dplyr::mutate(
      code_muni6 = substring(code_muni, 1, 6) |> as.numeric()
    )
  
  healthfacilities <- dplyr::left_join(
    x = healthfacilities,
    y = codigos_muni, by = "code_muni6"
    ) |> 
    dplyr::select(-code_muni6)
  
  
  # add name f
  healthfacilities <- healthfacilities |> 
    mutate(code_muni = ifelse(code_state==53, 5300108, code_muni))
  
  
  
  # add geocodebr coordinates --------------------------------------------------
  
  fields <- geocodebr::definir_campos(
    estado = 'code_state',
    municipio = 'code_muni',
    logradouro = 'logradouro',
    numero = 'numero_endereco',
    cep = 'cep',
    localidade = 'bairro'
    )
  
  # a gente envia pro geocodebr apenas as colunas essenciais
  temp_health <- healthfacilities |> 
    dplyr::select(co_cnes, co_unidade, code_state, code_muni, 
           logradouro, numero_endereco, cep, bairro)
  
  temp_geo <- geocodebr::geocode(
    enderecos = temp_health, 
    campos_endereco = fields,
    n_cores = 1
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
  df <- dplyr::left_join(x = healthfacilities, y = temp_geo)
  
  
  # calcula distancia entre coordenadas oficias e do geocodebr
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
  df[, lat_cnes := as.numeric(lat_cnes)]
  df[, lon_cnes := as.numeric(lon_cnes)]
  df[!is.na(lat_cnes) & !is.na(lat_geocodebr), dist := dt.haversine(lat_cnes, lon_cnes, lat_geocodebr, lon_geocodebr)]
  
  
  # decide which spatial coordinates to use --------------------------------------------------
  
  # Criteria:
  # 1. if distance between sources < 800, use official source
  # 2. if distance between sources > 800 & desvio_metros_geocodebr < 800, use geocodebr
  # 3. (custom) voting places oversees, use official source 
  
  df[, coords_source := fcase(
    dist < 800, 'cnes', 
    dist > 800 & desvio_metros_geocodebr < 800, 'geocodebr',
    is.na(lat_cnes), 'geocodebr', 
    default = 'cnes'  # abbrev_state ==  "ZZ", -1 # voting places overseas
  )]
  
 # janitor::tabyl(df$coords_source)
 # df$coords_source      n   percent
 #             cnes  67849 0.1849603
 #        geocodebr 298981 0.8150397

  
  df[, lat := ifelse(coords_source=="cnes", lat_cnes, lat_geocodebr)]
  df[, lon := ifelse(coords_source=="cnes", lon_cnes, lon_geocodebr)]


    
  # converte para sf
  data.table::setDF(df)
  healthfacilities_sf <- sfheaders::sf_point(
    obj = df,
    x = 'lon',
    y = 'lat', 
    keep = TRUE
    )
  
  sf::st_crs(healthfacilities_sf) <- 4674

  # drop columns created in the geocoding part
  healthfacilities_sf <- healthfacilities_sf |> 
    select(-dist)
  
  
  ## 3. Apply harmonize geobr cleaning -----------------------------------------

  temp_sf <- harmonize_geobr(
    temp_sf = healthfacilities_sf,
    # year = year,
    add_state = T,
    state_column = "code_state",
    add_region = T,
    region_column = "code_state",
    add_snake_case = F,
    #snake_colname = snake_colname,
    projection_fix = T,
    encoding_utf8 = T,
    topology_fix = F,
    remove_z_dimension = T,
    use_multipolygon = F
  )
  
  
  # re-order columns
  # re-order columns
  temp_sf <- temp_sf |> 
    # first columns
    dplyr::relocate(code_muni,
                    name_muni,
                    co_unidade,
                    co_cnes
    ) |> 
    # last columns
    dplyr::relocate(code_state,
                    name_state,
                    code_region,
                    name_region,
                    lat_cnes, 
                    lon_cnes,
                    lat_geocodebr,
                    lon_geocodebr,
                    precisao_geocodebr,
                    tipo_resultado_geocodebr,
                    desvio_metros_geocodebr,
                    coords_source,
                    date_update, 
                    geometry,
                    .after = last_col())
  
    
    
  
  ## 7. Sort
  temp_sf <- temp_sf |>
    dplyr::arrange(code_state, code_muni, co_unidade, co_cnes)
  
  
  ## 4. Save results  ----------------------------------------------------------
  
  # Save in parquet
  write_geobr_parquet(
    sf_obj = temp_sf,
    path = paste0(dir_clean, "/healthfacilities_", date_update, ".parquet")
    )
  
  
  ## 5. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
  
}

