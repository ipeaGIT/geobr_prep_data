#> DATASET: historical states (divisao territorial 1872-1991)
#> Source: IBGE - https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/evolucao_da_divisao_territorial_do_brasil/evolucao_da_divisao_territorial_do_brasil_1872_2010/municipios_1872_1991/divisao_territorial_1872_1991/
#> Metadata:
# Titulo: Evolucao da Divisao Territorial do Brasil 1872-1991
# Frequencia de atualizacao: Historico (11 anos)
# Forma de apresentacao: Shapefile per-year
# Linguagem: Pt-BR
# Character set: WINDOWS-1252
# Informacao do Sistema de Referencia: SIRGAS 2000

# Download the data  -----------------------------------------------------------
download_hist_states <- function(year) { # year = 1872
  
  base_url <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/evolucao_da_divisao_territorial_do_brasil/evolucao_da_divisao_territorial_do_brasil_1872_2010/municipios_1872_1991/divisao_territorial_1872_1991/"
  
  base <- 'limite_estadual|limite_de_provincia'
  
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
  message(sprintf("[historical %d] Baixando malha estadual...", year))
  
  file_raw <- download_file_geobr(
    file_url = target_url,
    dest_dir = zip_dir
  )
  
  
  ## 2. Unzip and read estadual mesh ------------------------------------------
  
  files <- unzip_geobr(zip_dir = zip_dir, out_zip = shp_dir)
  
  raw <- readmerge_geobr(
    folder_path = shp_dir, 
    encoding = "WINDOWS-1252"
  )
  
  ## 3. standardize colnames
  raw <- rename_cols_geobr(raw, dicionario_state) |> 
    dplyr::select(
      dplyr::any_of(c('code_state', 'name_state'))
      )
  
  # projection fix
  raw <- harmonize_projection(raw)
  
  ## 4. Download and append litigio (disputed territory) if exists -------------
  
  liti_file <- NULL
  liti_file <- all_files[all_files %like% "litigio"]
  liti_url <- paste0(base_url, year, "/", liti_file)
  
  # se houver litigio, rbind as observacoes
  if (length(liti_file) == 1) {
    
    liti_dir <- paste0(shp_dir, "/liti")
    dir.create(liti_dir, showWarnings = FALSE)
    liti_dest <- paste0(liti_dir, "/", liti_file)

    file_raw <- download_file_geobr(
      file_url = liti_url,
      dest_dir = liti_dir
    )
    
    
    unzip_geobr(zip_dir = liti_dir, out_zip = liti_dir)
    
    liti <- readmerge_geobr(
      folder_path = liti_dir, 
      encoding = "WINDOWS-1252"
    )
    
    liti <- dplyr::select(liti, code_state = dplyr::any_of(c("id", "codigo")),
                          name_state = dplyr::any_of(c("nome")))
    # projection fix
    liti <- harmonize_projection(liti)
    
    # rbind
    raw <- dplyr::bind_rows(raw, liti)
  }
  
  # tail(raw)
  
  message(sprintf("[historical %d] Total: %d rows", year, nrow(raw)))
  
  raw$year <- year
  
  return(raw)
}


# Clean the data  --------------------------------------------------------------

# raw <- tar_read(hist_state_raw, 4)
# states_clean <- tar_read(states_clean)
# head(raw)

clean_hist_states <- function(raw, states_clean) {
  
  yyyy <- raw$year[1]
  dir_clean <- paste0("./data/states/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  # Pos-CF/88 (1989+): Fernando de Noronha foi reincorporado a Pernambuco como
  # Distrito Estadual (CF/88, ADCT Art. 96). O shapefile IBGE 1991 mantem o
  # arquipelago como poligono separado rotulado "Distrito Estadual de Fernando
  # de Noronha (PE)", mas administrativamente eh PE. Renomeamos pra "Pernambuco"
  # ANTES do dissolve para que os poligonos sejam unidos num unico multipoligono.
  fn_distrito <- grepl("(?i)distrito.*estadual.*fernando", raw$name_state)
  if (any(fn_distrito)) {
    raw$name_state[fn_distrito] <- "Pernambuco"
  }

  # dissolve borders to clean geometry and remove repetition
  raw <- dissolve_polygons_no_split(
    mysf = raw,
    group_column = "name_state"
  )
  

  ## 3. Harmonize --------------------------------------------------------------

  temp_sf <- harmonize_geobr(
    temp_sf        = raw,
    year           = yyyy,
    add_state      = TRUE,
    state_column   = "name_state",
    add_region     = TRUE,
    region_column  = "code_state",
    add_snake_case = TRUE,
    snake_colname  = "name_state",
    projection_fix = TRUE,
    encoding_utf8  = TRUE,
    topology_fix   = TRUE,
    remove_z_dimension = TRUE,
    use_multipolygon   = TRUE
  )


  
  ## 5. Column order -----------------------------------------------------------

  # identifica casos de litigio. Detectamos por:
  # (a) name_state com pattern "Liti" (cobre "Litígio Pi/Ce", "Litígio Mg/Es")
  # (b) code_state == 0 (sentinela do raw IBGE; pode ja ter virado NA apos
  #     harmonize_geobr/add_state_info, dai a deteccao por nome eh primaria).
  # Litigios recebem code_state = code_region = 99 (sentinela), nao NA.
  # NOTA: base::ifelse() coerce o resultado pra logical quando todas as linhas
  # satisfazem a condicao, gravando como bool no parquet e quebrando o write
  # do geoarrow downstream. dplyr::if_else type-strict + as.numeric() forca
  # double consistente em todos os anos historicos.
  temp_sf <- temp_sf |>
    dplyr::mutate(
      litigio     = grepl("Lit[ií]", name_state, ignore.case = TRUE) |
                    (!is.na(code_state) & code_state == 0),
      code_state  = dplyr::if_else(litigio, 99, as.numeric(code_state)),
      code_region = dplyr::if_else(litigio, 99, as.numeric(code_region))
    )
  # NOTA: "Município Neutro" (1872), "Districto Federal" (1900-1933),
  # "Distrito Federal" (1940-1950) e "Guanabara" (1960-1970) representam a
  # mesma area geografica (RJ-cidade-capital-federal antes de Brasilia).
  # Mapeados para code_state=34 (UF extinta IBGE) no add_state_info do
  # harmonize_geobr, distinto de RJ-estado (33). DF-Brasilia (year>=1960) = 53.
  # FN-territorio (1950-1980) = 20 (UF extinta IBGE), distinto de PE (26).
  
  temp_sf <- temp_sf |>
    dplyr::select(code_state, name_state,
                  code_state, abbrev_state, name_state,
                  code_region, name_region,
                  year, litigio, geometry)

  # sort by key columns
  temp_sf <- temp_sf |> 
    dplyr::arrange(code_state, code_state)
  
  ## 6. Validate ---------------------------------------------------------------

  stopifnot(!is.na(sf::st_crs(temp_sf)))
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")

  ## 7. Simplify + save --------------------------------------------------------

  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)

  write_geobr_parquet(
    sf_obj = temp_sf,
    path = paste0(dir_clean, "/states_", yyyy, ".parquet")
    )

  write_geobr_parquet(
    sf_obj = temp_sf_simplified,
    path = paste0(dir_clean, "/states_", yyyy, "_simplified.parquet")
    )

  files <- list.files(dir_clean, pattern = ".parquet$",
                      full.names = TRUE, recursive = TRUE)
  return(files)
}
