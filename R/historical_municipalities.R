#> DATASET: historical municipalities (divisao territorial 1872-1991)
#> Source: IBGE - https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/evolucao_da_divisao_territorial_do_brasil/evolucao_da_divisao_territorial_do_brasil_1872_2010/municipios_1872_1991/divisao_territorial_1872_1991/
#> Metadata:
# Titulo: Evolucao da Divisao Territorial do Brasil 1872-1991
# Frequencia de atualizacao: Historico (11 anos)
# Forma de apresentacao: Shapefile per-year
# Linguagem: Pt-BR
# Character set: WINDOWS-1252
# Informacao do Sistema de Referencia: SIRGAS 2000

# Download the data  -----------------------------------------------------------
download_hist_muni <- function(year) {

  base_url <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/evolucao_da_divisao_territorial_do_brasil/evolucao_da_divisao_territorial_do_brasil_1872_2010/municipios_1872_1991/divisao_territorial_1872_1991/"

  base <- 'malha_municipal'
  
  ## 1. Download municipal mesh zip --------------------------------------------

  url_year <- paste0(base_url, year, "/")
  all_files <- list_folders(url_year)
  file_base <- all_files[all_files %like% base]
  muni_url <- paste0(url_year, "/", file_base)

  tmp_dir <- paste0(tempdir(), "/municipality_historical/", year)
  zip_dir <- paste0(tmp_dir, "/zips")
  shp_dir <- paste0(tmp_dir, "/shps")
  dir.create(zip_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(shp_dir, recursive = TRUE, showWarnings = FALSE)

  muni_dest <- paste0(zip_dir, "/", file_base)
  message(sprintf("[historical %d] Baixando malha municipal...", year))
  
  file_raw <- download_file_geobr(
    file_url = muni_url,
    dest_dir = zip_dir
    )
    
    
  ## 2. Unzip and read municipal mesh ------------------------------------------

  files <- unzip_geobr(zip_dir = zip_dir, out_zip = shp_dir)
  
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
    
    download_file_geobr(
      file_url = liti_url,
      dest_dir = liti_dir
      )
    
    unzip_geobr(zip_dir = liti_dir, out_zip = liti_dir)
    
    liti <- readmerge_geobr(
      folder_path = liti_dir, 
      encoding = "WINDOWS-1252"
    )
    
    liti <- dplyr::select(liti, code_muni = dplyr::any_of(c("id", "codigo")),
                                 name_muni = dplyr::any_of(c("nome")))
    # projection fix
    liti <- harmonize_projection(liti)
    
    # add code 99 to litigio
    liti$code_muni <- 99
    
    # rbind
    raw <- rbind(raw, liti)
  }

  message(sprintf("[historical %d] Total: %d rows", year, nrow(raw)))
  
  raw$year <- year
  
  return(raw)
}

# Clean the data  --------------------------------------------------------------
# raw <- tar_read(hist_muni_raw, 6)
# tail(raw)

clean_hist_muni <- function(raw, municipality_clean) {
  
  yyyy <- raw$year[1]
  dir_clean <- paste0("./data/municipality/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  ## 2. standardize colnames  ---------------------------------------------------
  municipalities <- rename_cols_geobr(raw, dicionario_municipality) |> 
    dplyr::select(code_muni, name_muni) |> 
    mutate(code_state = substr(code_muni, 1, 2)) |> 
    filter(code_muni != 0)
  

  ## 1. Fix known municipality name issues -------------------------------------

  if (yyyy %in% c(1872, 1900, 1920)) {
    raw$name_muni[raw$code_muni == 2306405] <- "Itapipoca"
  }
  if (yyyy == 1872) {
    raw$name_muni[raw$code_muni == 2407401] <- "Martins"
  }
  # raw$code_muni[raw$name_muni == "ITAPIPOCA"] <- 2306405


  ## 2b. Dissolve fragmentos de mesmo code_muni (ilhas/territorios disjuntivos)
  ## Preserva litigios (code_muni == 0) intactos, pois sao poligonos distintos
  ## que nao devem ser unidos entre si.
    # we don't use the dissolve_polygons_no_split() function here because it removes holes
    # and some municipalities do have holes that must be kept
  non_liti <- raw[!is.na(raw$code_muni) & raw$code_muni != 0, ]
  liti     <- raw[is.na(raw$code_muni) | raw$code_muni == 0, ]
  if (nrow(non_liti) > 0) {
    
    non_liti <- harmonize_projection(non_liti)
    all_cols <- names(non_liti)
    cols_union <- all_cols[!grepl("geometry", all_cols)]
    non_liti <- duckspatial::ddbs_union_agg(
      x =  non_liti,
      by =  cols_union
    ) |>
      duckspatial::ddbs_collect()
    
    
  }
  raw <- dplyr::bind_rows(non_liti, liti)


  ## 3. Harmonize --------------------------------------------------------------

  temp_sf <- harmonize_geobr(
    temp_sf        = raw,
    year           = yyyy,
    add_state      = TRUE,
    state_column   = "code_muni",
    add_region     = TRUE,
    region_column  = "code_muni",
    add_snake_case = TRUE,
    snake_colname  = "name_muni",
    projection_fix = TRUE,
    encoding_utf8  = TRUE,
    topology_fix   = TRUE,
    remove_z_dimension = TRUE,
    use_multipolygon   = TRUE
  )


  ## 5. Column order -----------------------------------------------------------

  # identifica casos de litigio
  temp_sf <- temp_sf |> 
    mutate( litigio = ifelse(code_muni==0, TRUE, FALSE),
            code_muni = ifelse(code_muni==0, NA, code_muni),
            code_state = ifelse(code_state==0, NA, code_state),
            code_region = ifelse(code_region==0, NA, code_region),
            )
  
  
  temp_sf <- temp_sf |>
    dplyr::select(code_muni, name_muni,
                  code_state, abbrev_state, name_state,
                  code_region, name_region,
                  year, litigio, geometry)

  # sort by key columns
  temp_sf <- temp_sf |> 
    dplyr::arrange(code_state, code_muni)
  
  ## 6. Validate ---------------------------------------------------------------

  stopifnot(!is.na(sf::st_crs(temp_sf)))
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")

  ## 7. Simplify + save --------------------------------------------------------

  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)

  write_geobr_parquet(
    sf_obj = temp_sf,
    path = paste0(dir_clean, "/municipalities_", yyyy, ".parquet")
    )

  write_geobr_parquet(
    sf_obj = temp_sf_simplified,
    path = paste0(dir_clean, "/municipalities_", yyyy, "_simplified.parquet")
    )

  files <- list.files(dir_clean, pattern = ".parquet$",
                      full.names = TRUE, recursive = TRUE)
  return(files)
}
