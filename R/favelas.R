#> DATASET: Favelas e Comunidades Urbanas (FCU)
#> Source: IBGE - Censo Demografico 2022
#> https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Favelas_e_comunidades_urbanas_Resultados_do_universo/arquivos_vetoriais/
#> Metadata:
# Titulo: Favelas e Comunidades Urbanas - Resultados do Universo
# Titulo alternativo: favelas
# Frequencia de atualizacao: decenal (Censo)
#
# Forma de apresentacao: Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: Poligonos das Favelas e Comunidades Urbanas do Censo 2022.
# Informacao do Sistema de Referencia: SIRGAS 2000 (EPSG:4674)
# Observacoes: Dados disponiveis apenas para 2022 (Censo 2022).
#   Arquivo utilizado: poligonos_FCUs_shp.zip
#   Colunas originais: cd_fcu, nm_fcu, cd_uf, nm_uf, sigla_uf, cd_mun, nm_mun
#   12348 feicoes, MULTIPOLYGON


# Download the data  -----------------------------------------------------------
download_favela <- function(year) {

  ## 0. Set up the download links -----------------------------------------------

  base <- "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Favelas_e_comunidades_urbanas_Resultados_do_universo/arquivos_vetoriais/"

  ftp_link <- switch(as.character(year),
    "2022" = paste0(base, "poligonos_FCUs_shp.zip"),
    stop(paste("Ano", year, "nao suportado para favelas"))
  )

  ## 1. Create temp folders -----------------------------------------------------

  zip_dir <- paste0(tempdir(), "/favelas/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)

  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)

  ## 2. Download Raw data -------------------------------------------------------

  file_raw <- download_file_geobr(
    file_url = ftp_link, 
    dest_dir = zip_dir
  )
  
  
  ## 3. Unzip Raw data ----------------------------------------------------------

  unzip_geobr(zip_dir = zip_dir,
              out_zip = out_zip)

  ## 4. Read shapefile ----------------------------------------------------------

  favela_raw <- sf::st_read(
    out_zip,
    quiet = TRUE,
    stringsAsFactors = FALSE,
    options = "ENCODING=UTF-8"
  )

  favela_raw$year <- year

  return(favela_raw)
}

# Clean the data  --------------------------------------------------------------
# favela_raw <- tar_read(favela_raw, 1)
clean_favela <- function(favela_raw) {

  ## 0. Create folder to save clean data ----------------------------------------
  yyyy <- favela_raw$year[1]
  dir_clean <- paste0("./data/favelas/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  ## 1. Rename and select columns -----------------------------------------------

  temp_sf <- favela_raw |>
    dplyr::rename(
      code_favela  = cd_fcu,
      name_favela  = nm_fcu,
      code_state   = cd_uf,
      name_state   = nm_uf,
      abbrev_state = sigla_uf,
      code_muni    = cd_mun,
      name_muni    = nm_mun
    )


  ## 3. Apply harmonize geobr cleaning ------------------------------------------

  temp_sf <- harmonize_geobr(
    temp_sf        = temp_sf,
    year           = yyyy,
    add_state      = TRUE,
    state_column = "code_state",
    add_region     = TRUE,
    region_column  = "code_state",
    add_snake_case = TRUE,
    snake_colname  = c("name_favela", "name_muni", "name_state"),
    projection_fix = TRUE,
    encoding_utf8  = TRUE,
    topology_fix   = TRUE,
    remove_z_dimension = TRUE,
    use_multipolygon   = TRUE
  )




  ## 4. Reorder columns (geometry always last) ----------------------------------

  temp_sf <- temp_sf |>
    dplyr::select(
      code_muni, name_muni,
      code_favela, name_favela,
      code_state, abbrev_state, name_state,
      code_region, name_region,
      year, geometry
    )


  # sort by key columns
  temp_sf <- temp_sf |>
    dplyr::arrange(code_state, code_muni, code_favela)


  ## 5. Validate ----------------------------------------------------------------

  stopifnot(is.numeric(temp_sf$code_favela))
  stopifnot(is.numeric(temp_sf$code_muni))
  stopifnot(is.numeric(temp_sf$code_state))
  stopifnot(is.numeric(temp_sf$code_region))
  stopifnot(is.character(temp_sf$name_favela))
  stopifnot(is.character(temp_sf$abbrev_state))
  stopifnot(all(nchar(temp_sf$abbrev_state) == 2))
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")

  ## 6. Lighter version ---------------------------------------------------------

  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)

  ## 7. Save datasets  ----------------------------------------------------------

  write_geobr_parquet(
    temp_sf,
    paste0(dir_clean, "/favelas_", yyyy, ".parquet"))

  write_geobr_parquet(
    temp_sf_simplified,
    paste0(dir_clean, "/favelas_", yyyy, "_simplified.parquet"))

  ## 8. Return file list --------------------------------------------------------

  files <- list.files(path = dir_clean,
                      pattern = ".parquet",
                      recursive = TRUE,
                      full.names = TRUE)

  return(files)
}
