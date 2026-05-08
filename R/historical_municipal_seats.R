#> DATASET: historical municipal_seats (divisao territorial 1872-1991)
#> Source: IBGE - https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/evolucao_da_divisao_territorial_do_brasil/evolucao_da_divisao_territorial_do_brasil_1872_2010/municipios_1872_1991/divisao_territorial_1872_1991/
#> Metadata:
# Titulo: Evolucao da Divisao Territorial do Brasil 1872-1991
# Frequencia de atualizacao: Historico (11 anos)
# Forma de apresentacao: Shapefile per-year
# Linguagem: Pt-BR
# Character set: WINDOWS-1252
# Informacao do Sistema de Referencia: SIRGAS 2000

# Download the data  -----------------------------------------------------------
download_hist_muniseats <- function(year) {

  base_url <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/evolucao_da_divisao_territorial_do_brasil/evolucao_da_divisao_territorial_do_brasil_1872_2010/municipios_1872_1991/divisao_territorial_1872_1991/"
  
  base <- 'sede_municipal'
  
  ## 1. Download estadual mesh zip --------------------------------------------
  
  url_year <- paste0(base_url, year)
  all_files <- list_folders(url_year)
  file_base <- all_files[all_files %like% base]
  target_url <- paste0(url_year, "/", file_base)
  
  tmp_dir <- paste0(tempdir(), "/state_historical/", year)
  zip_dir <- paste0(tmp_dir, "/zips")
  shp_dir <- paste0(tmp_dir, "/shps")
  dir.create(zip_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(shp_dir, recursive = TRUE, showWarnings = FALSE)
  
  file_dest <- paste0(zip_dir, "/", file_base)
  message(sprintf("[historical %d] Baixando sede municipal...", year))
  
  file_raw <- download_file_geobr(
    file_url = target_url,
    dest_dir = zip_dir
  )
  
  
  ## 2. Unzip and read estadual mesh ------------------------------------------
  
  unzip_geobr(zip_dir = zip_dir, out_zip = shp_dir)
  
  raw <- readmerge_geobr(
    folder_path = shp_dir, 
    encoding = "WINDOWS-1252"
  )
  
  ## 3. standardize colnames
  raw <- raw |> 
    janitor::clean_names()
  
  raw <- rename_cols_geobr(raw, dicionario_municipality) |> 
    dplyr::select(
      dplyr::any_of(c('code_muni', 'name_muni'))
    )
  
  raw$year <- year
  
  return(raw)
}


# Clean the data  --------------------------------------------------------------
clean_hist_muniseats <- function(raw, muniseats_clean) {
  
  yyyy <- raw$year[1]
  dir_clean <- paste0("./data/municipal_seat/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)
  
  raw <- raw |> 
    mutate(code_state = substr(code_muni, 1, 2))
  
  ## 3. Harmonize --------------------------------------------------------------
  
  temp_sf <- harmonize_geobr(
    temp_sf        = raw,
    year           = yyyy,
    add_state      = TRUE,
    state_column   = "code_state",
    add_region     = TRUE,
    region_column  = "code_state",
    add_snake_case = TRUE,
    snake_colname  = "name_muni",
    projection_fix = TRUE,
    encoding_utf8  = TRUE,
    topology_fix   = FALSE,
    remove_z_dimension = TRUE,
    use_multipolygon   = FALSE
  )
  
  
  ## 5. Column order -----------------------------------------------------------
  
  temp_sf <- temp_sf |>
    dplyr::select(code_muni, name_muni,
                  code_state, abbrev_state, name_state,
                  code_region, name_region,
                  year, geometry)
  
  # sort by key columns
  temp_sf <- temp_sf |> 
    dplyr::arrange(code_state, code_muni)
  
  
  ## 7. save --------------------------------------------------------
  

  write_geobr_parquet(
    sf_obj = temp_sf,
    path = paste0(dir_clean, "/municipalseats_", yyyy, ".parquet")
  )
  
  
  files <- list.files(dir_clean, pattern = ".parquet$",
                      full.names = TRUE, recursive = TRUE)
  return(files)
}
