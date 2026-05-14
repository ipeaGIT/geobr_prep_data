#> DATASET: Arranjos Populacionais e Concentracoes Urbanas
#> Source: IBGE
# - https://www.ibge.gov.br/geociencias/organizacao-do-territorio/divisao-regional/15782-arranjos-populacionais-e-concentracoes-urbanas-do-brasil.html
# - https://geoftp.ibge.gov.br/organizacao_do_territorio/divisao_regional/arranjos_populacionais
#> Scale: 1:25.000
#> Format: MDB (MS Access) with multiple spatial layers
#> CRS: SIRGAS 2000
#> Year: 2015 (static, based on Census 2010 data)
#> Encoding: UTF-8
#>
#> The ZIP contains a single MDB file: ArranjosPopulacionais_2ed.mdb
#> Key layers:
#>   - ComposicaoRecortes_01_PorMunicipio (5567 municipalities, 44 columns)
#>   - ArranjosPopulacionais_01 (294 dissolved arrangements)
#>   - ConcentracoesUrbanas_ConsideradasAnalise_01 (185 dissolved concentrations)
#>
#> For pop_arrangements: use ComposicaoRecortes_01_PorMunicipio, subset rows
#>   where CodArranjoPop is not NA (1243 municipalities)
#> For urban_concentrations: use ConcentracoesUrbanas_ConsideradasAnalise_01
#>   (185 pre-dissolved concentration polygons)
#>   
#>   Defnições do IBGE
#>   
#>   Os Arranjos Populacionais acima de 100 000 habitantes possuem a urbanização 
#>   como principal processo indutor da integração dos municípios. Como estes estão 
#>   diretamente relacionados ao fenômeno urbano e suas dinâmicas, decidiu-se 
#>   nomeá-los de Concentrações Urbanas. As concentrações urbanas foram assim 
#>   definidas: Municípios Isolados e Arranjos Populacionais, ambos com 
#>   população acima de 100 000 habitantes.
#>   
#>   São consideradas médias concentrações urbanas os municípios isolados e os 
#>   arranjos populacionais acima de 100 000 a 750 000 habitantes.
#>   
#>   São consideradas grandes concentrações urbanas os arranjos populacionais 
#>   acima de 750 000 habitantes e os municípios isolados (que não formam 
#>   arranjos) de mesma faixa populacional.


# Download the data  -----------------------------------------------------------
download_poparrangements <- function(year){
  
  ## 0. URL (same ZIP as pop_arrangements) ------------------------------------
  if (year==2010) {
    file_url <- paste0(
      "https://geoftp.ibge.gov.br/organizacao_do_territorio/",
      "divisao_regional/arranjos_populacionais/base_de_dados_2ed/",
      "ArranjosPopulacionais_mbd_2ed.zip"
    )
  }
  
  ## 1. Create temp folders ---------------------------------------------------
  zip_dir <- paste0(tempdir(), "/urban_concentrations/")
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  
  out_zip <- paste0(zip_dir, "unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  
  ## 2. Download --------------------------------------------------------------
  zip_file <- download_file_geobr(
    file_url = file_url, 
    dest_dir = zip_dir
  )
  
  ## 3. Unzip -----------------------------------------------------------------
  files <- unzip_geobr(
    zip_dir = zip_dir, 
    out_zip = out_zip
  )
  
  ## 4. Find the MDB file -----------------------------------------------------
  mdb_file <- list.files(
    out_zip, pattern = "\\.mdb$",
    full.names = TRUE, recursive = TRUE
  ) # |> fs::path()
  
  if (length(mdb_file) == 0) {
    stop("No .mdb file found in the unzipped directory")
  }
  
  ## 5. Read the per-municipality layer ---------------------------------------
  
  # sf::st_layers(mdb_file)
  
  raw <- sf::st_read(
    mdb_file[1],
    layer = "ComposicaoRecortes_01_PorMunicipio",
    quiet = TRUE,
    stringsAsFactors = FALSE
  )
  
  raw <- raw |> 
    janitor::clean_names() |> 
    sf::st_drop_geometry()
  
  
  raw$year <- year
  
  return(raw)
}

# Clean the data  --------------------------------------------------------------
# raw <- tar_read(poparrangements_raw)
# municipality_clean <- tar_read(municipality_clean)
clean_poparrangements <- function(raw, municipality_clean){
  
  yyyy <- raw$year[1]
  
  
  ## 0. Create clean directory ------------------------------------------------
  dir_clean <- paste0("./data/pop_arrangements/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)
  
  
  ## 1. Rename columns to geobr standard -------------------------------------
  if( yyyy == 2010) {
    
    temp_df <- raw |>
      dplyr::select(
        code_muni                     = cod_munic,
        name_muni                     = nom_munic,
        pop_total_2010                = pop_tot2010,
        pop_urban_2010                = pess2010urbano,
        pop_rural_2010                = pess2010rural,
        code_pop_arrangement          = cod_arranjo_pop,
        name_pop_arrangement          = nome_arranjo_pop,
        code_urban_concentration_big  = cod_grande_conc_urbana,
        name_urban_concentration_big  = nome_grande_conc_urbana,
        code_urban_concentration_mid  = cod_media_conc_urbana,
        name_urban_concentration_mid  = nome_media_conc_urbana,
        cod_arranjo_fronteirico
        )
    
  }
  

  # identifica se esta em fronteira internacionais
  data.table::setDT(temp_df)
  temp_df[, international_boder := ifelse(is.na(cod_arranjo_fronteirico), 0, 1) ]
  temp_df[, cod_arranjo_fronteirico := NULL ]
  
  # create unique code for urban concentration areas
  temp_df[, code_urban_concentration := fifelse(is.na(code_urban_concentration_big),
                                                code_urban_concentration_mid,
                                                code_urban_concentration_big)]
  
  temp_df[, name_urban_concentration := fifelse(is.na(name_urban_concentration_big),
                                                name_urban_concentration_mid,
                                                name_urban_concentration_big)]
  
  # drop old columns
  temp_df[, c('name_urban_concentration_mid', 'name_urban_concentration_big',
              'code_urban_concentration_mid', 'code_urban_concentration_big') := NULL]
  
  
  # code cols to numeric
  temp_df <- code_cols_to_numeric(temp_df)
  
  
  # Keep only municipalities in a pop arrangements or urban concentration area
  temp_df2 <- temp_df |>
    dplyr::filter(!is.na(code_pop_arrangement) | !is.na(code_urban_concentration)) |> 
    unique()
  
  # checagem de valores
  # 294 arranjos populacionais
  unique(temp_df$code_pop_arrangement) |> length() ==
  unique(temp_df2$code_pop_arrangement) |> length()
  
  # checagem de valores
  # 187 areas de concentracao urbanas
  # isso difere da publicacao oficial, que dizia 184 (78 + 80 + 26) 
  # deve ter sido atualizacao da 2a edicao
  unique(temp_df$code_urban_concentration) |> length() ==
  unique(temp_df2$code_urban_concentration) |> length()
  
  
  ## 5. Convert columns to proper types --------------------------------------
  temp_df2[, pop_total_2010 := as.numeric(pop_total_2010)]
  temp_df2[, pop_urban_2010 := as.numeric(pop_urban_2010)]
  temp_df2[, pop_rural_2010 := as.numeric(pop_rural_2010)]
  
  # bring geometries back 
  munis <- municipality_clean[municipality_clean %like% 2010]
  munis <-  munis[!munis %like% "simplified"]
  
  munis <- read_geoparquet(munis)
  
  temp_sf <- left_join(munis, temp_df2)
  
  # Keep only municipalities in a pop arrangements or urban concentration area
  temp_sf <- temp_sf |>
    dplyr::filter(!is.na(code_pop_arrangement) | !is.na(code_urban_concentration)) |> 
    unique()
  
  ## 6. Harmonize (state info, projection, topology, etc.) -------------------
  temp_sf <- harmonize_geobr(
    temp_sf            = temp_sf,
    year               = yyyy,
    add_state          = FALSE,
    # state_column       = "code_state",
    add_region         = FALSE,
    # region_column      = "code_state",
    add_snake_case     = FALSE,
    # snake_colname      = c("name_muni", "name_urban_concentration"),
    projection_fix     = TRUE,
    encoding_utf8      = TRUE,
    topology_fix       = TRUE,
    remove_z_dimension = TRUE,
    use_multipolygon   = TRUE
  )
  
  ## 9. Select and reorder columns -------------------------------------------
  temp_sf <- temp_sf |>
    dplyr::select(
      code_muni, 
      name_muni,
      code_pop_arrangement, 
      name_pop_arrangement,
      code_urban_concentration, 
      name_urban_concentration,
      international_boder,
      pop_total_2010, 
      pop_urban_2010, 
      pop_rural_2010,
      code_state, abbrev_state, name_state,
      code_region, name_region,
      year, geometry
    )
  
  # sort rows
  temp_sf <- temp_sf |>
    dplyr::arrange(code_state, code_pop_arrangement, code_urban_concentration)
  
  
  ## 10. Validate -------------------------------------------------------------
  stopifnot(sf::st_crs(temp_sf)$epsg == 4674)
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")
  stopifnot(is.numeric(temp_sf$code_state))
  stopifnot(is.numeric(temp_sf$code_muni))
  stopifnot(is.numeric(temp_sf$code_urban_concentration))
  stopifnot(is.character(temp_sf$abbrev_state))
  
  
  ## 11. Simplified version ---------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 12. Save parquet ---------------------------------------------------------
  write_geobr_parquet(
    temp_sf,
    paste0(dir_clean, "/poparrangements_", yyyy, ".parquet")
    
    )

  write_geobr_parquet(
    temp_sf_simplified,
    paste0(dir_clean,"/poparrangements_", yyyy, "_simplified", ".parquet")
    )

  ## 13. Return file paths ----------------------------------------------------
  files <- list.files(path = dir_clean, pattern = "\\.parquet$",
                      recursive = TRUE, full.names = TRUE)
  return(files)
}


