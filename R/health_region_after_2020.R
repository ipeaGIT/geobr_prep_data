#> DATASET: Health Regions
#> Source: DATASUS FTP — ftp://ftp.datasus.gov.br/territorio/mapas/
#> Metadata:
# Titulo: Regioes de Saude do SUS
# Titulo alternativo: health regions
# Frequencia de atualizacao:
#
# Forma de apresentacao: .MAP binario DATASUS (leitura via read_datasus_map)
# Linguagem: Pt-BR
# Character set: WINDOWS-1252 (.MAP)
# Character set: 2005 - WINDOWS-1252
#                2015 - UTF-8

# Resumo: Criado a partir do Decreto n. 7508 de junho de 2011, em substituicao aos
# # Colegiados de Gestao Regional (oriundos do Pacto pela Saude), o CIR a um colegiado
# # no qual participam as Secretarias Municipais de Saude, de uma dada regiao, e a Secretaria
# # de Estado de saude com o objetivo de promover a gestao colaborativa no setor saude do estado.
#
# Estado: Em desenvolvimento
# Palavras chaves descritivas: CIR; RAS; SUS
# Informacao do Sistema de Referencia: DATASUS - SIRGAS 2000
#
# Nota: DATASUS FTP disponibiliza formato .MAP binario proprietario.
#       Leitura via read_datasus_map() em support_harmonize_geobr.R (leitor puro R).


# !!!!!!!!!!!!!!!!!!!!!!! apos 2023, usamos a lista de municipios em cada regiao

# Download the data  -----------------------------------------------------------
download_healthregions_new <- function(year){ # year = 2013
  
  dest_dir <- paste0(tempdir(), "/health_region/", year)
  dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
  
  # if (year == 2025) {
  # 
  #   url <- "https://ide.saude.gov.br/geoserver/ows?service=WFS&version=1.0.0&request=GetFeature&typename=geonode%3Aview_tb_regiao_saude&outputFormat=json&srs=EPSG%3A4674&srsName=EPSG%3A4674"
  #   healthregions_raw25 <- sf::st_read(url)
  # 
  #   healthregions_raw <- healthregions_raw |>
  #     dplyr::select(co_regiao_saude, no_regiao_saude, geometry)
  # }
  
  base_url <- "ftp://ftp.datasus.gov.br/territorio/tabelas/"
  encoding <- "UTF-8"
  
  if (year == 2023) {
    url <- paste0(base_url, year, '/base_territorial_2023.zip')
    encoding <- "Latin-1"
  }
  
  if (year == 2024) {
    url <- paste0(base_url, year, "/02-base_territorio_fev", substr(year, 3,4), ".zip")
    encoding <- "Latin-1"
  }
  
  if (year == 2025) {
    url <- paste0(base_url, year, "/03-base_territorial_mar", substr(year, 3,4), ".zip")
    encoding <- "Latin-1"
  }
  
  if (year >= 2026) {
    url <- paste0(base_url, year, "/02-base_territorial_fev", substr(year, 3,4), ".zip")
  }
  
  # download
  file_zip <- download_file_geobr(
    file_url = url,
    dest_dir = dest_dir
  )
  
  # unzip and select csv files only
  all_files <- unzip_geobr(zip_dir = dest_dir)
  files <- all_files[grep('.csv', all_files)]
  
  #'     encoding <- "WINDOWS-1252"
  #'     encoding <- "IBM437"
  #'     encoding <- "UTF-8"
  #'     encoding <- "Latin-1"
  ## read each table
  # correspondencia entre muni e micro regioes
  df_muni_micro <- files[grep('rl_municip_regsaud', files)] |>
    data.table::fread() |>
    dplyr::select(code_muni6 = CO_MUNICIP,
                  code_health_region = CO_REGSAUD
    ) |>
    dplyr::filter(code_health_region != 0) |>
    unique()
  
  # detect_csv_encoding
  # detect_csv_encoding(files[grep('tb_regsaud', files)])
  #
  #     df <- readr::read_csv(
  #       files[grep('tb_regsaud', files)] ,
  #       locale = locale(encoding = "ISO-8859-1")
  #     ) |> View()
  
  # regioes de saude com nomes
  df_micro <- files[grep('tb_regsaud', files)] |>
    # readr::read_csv(locale = locale(encoding = "windows-1252")) |>
    data.table::fread(encoding = encoding) |>
    dplyr::filter(CO_STATUS %in% c("ATIVO", "S")) |>
    dplyr::select( code_health_region = dplyr::any_of(c('CO_REGSAUD')),
                   name_health_region = DS_NOME
    ) |>
    dplyr::filter(code_health_region != 0) |>
    unique()
  
  
  # correspondencia munis e macro regioes
  df_muni_macro <- files[grep('rl_municip_macsaud', files)] |>
    data.table::fread() |>
    dplyr::select(code_muni6 = dplyr::any_of(c('CO_MUNINIP', 'CO_MUNICIP')),
                  code_health_macroregion = dplyr::any_of(c('CO_MACRORR', 'CO_MACSAUD', 'MACSAUDE'))
    ) |>
    dplyr::filter(code_health_macroregion != 0) |> 
    unique()
  
  
  # macro regiao de saude
  df_macro <- files[grep('tb_macsaud', files)] |>
    data.table::fread(encoding = encoding) |>
    dplyr::filter(CO_STATUS %in% c("ATIVO", "S")) |>
    dplyr::select(code_health_macroregion = dplyr::any_of(c('CO_MACRORR', 'CO_MACSAUD')),
                  name_health_macroregion = DS_NOME
    ) |>
    dplyr::filter(code_health_macroregion != 0) |>
    unique()
  
  # join all
  healthregions_raw <- dplyr::left_join(
    x = df_muni_micro,
    y = df_micro,
    by = 'code_health_region'
  ) |>
    dplyr::left_join(y = df_muni_macro, by = "code_muni6") |>
    dplyr::left_join(y = df_macro, by = 'code_health_macroregion')
  
  healthregions_raw$year <- year
  
  return(healthregions_raw)
}

# Clean the data  --------------------------------------------------------------

# healthregions_raw <- tar_read(healthregions_raw_new, 1)
# municipality_clean <- tar_read(municipality_clean)
clean_healthregions_new <- function(healthregions_raw, municipality_clean){ # year = 2013
  
  ## 0. Create folder to save clean data ---------------------------------------
  yyyy <- healthregions_raw$year[1]
  dir_clean <- paste0("./data/health_region/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)
  
  # le geometria dos municpios
  munis <- c(municipality_clean)
  munis <- munis[grep(yyyy, munis)]
  munis <- munis[!grepl('simplified', munis)] |>
    arrow::open_dataset() |>
    sf::st_as_sf() |>
    dplyr::mutate(
      code_muni6 = substring(code_muni, 1, 6) |> as.numeric()
    )

  # recover 7-digit code_muni
  # bring muni geometries
  healthregions <- dplyr::left_join(
    x = munis,
    y = healthregions_raw
    ) |>
    dplyr::select(-code_muni6)
  
  ## 3. Apply harmonize geobr cleaning
  temp_sf <- harmonize_geobr(
    temp_sf = healthregions,
    year = yyyy,
    add_state = F,
    # state_column = "code_state",
    add_region = F,
    # region_column = "code_state",
    add_snake_case = TRUE,
    snake_colname = c("name_health_region" , "name_health_macroregion"),
    projection_fix = TRUE,
    encoding_utf8 = TRUE,
    topology_fix = TRUE,
    remove_z_dimension = TRUE,
    use_multipolygon = TRUE
  )
  
  # clean result
  temp_sf <- temp_sf |> 
    dplyr::filter(!is.na(code_muni)) |> 
    dplyr::filter(!is.na(name_health_region))
  
  # Enforce column order (year and geometry always last)
  temp_sf <- temp_sf |>
    dplyr::relocate(
      code_muni, name_muni,
      dplyr::any_of(c('code_health_region', 'name_health_region', 'code_health_macroregion', 'name_health_macroregion'))
    ) |>
    dplyr::relocate(year, geometry, .after = dplyr::last_col())
  
  ## 7. Sort
  temp_sf <- temp_sf |>
    dplyr::arrange(code_state, code_muni, code_health_region)
  
  
  ## 4. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 5. Save datasets  ---------------------------------------------------------

  write_geobr_parquet(
    sf_obj = temp_sf,
    path = paste0(dir_clean, "/healthregions_", yyyy, ".parquet")
    )
  
  write_geobr_parquet(
    sf_obj= temp_sf_simplified,
    path = paste0(dir_clean, "/healthregions_", yyyy, "_simplified.parquet")
  )
  
  ## 6. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean,
                      pattern = ".parquet",
                      recursive = TRUE,
                      full.names = TRUE)
  
  return(files)
}

