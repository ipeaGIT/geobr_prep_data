#> DATASET: urbanized areas
#> Source: IBGE - https://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/
#> Metadata:
# Título: Áreas Urbanizadas do Brasil
# Título alternativo: urban_area
# Frequência de atualização: irregular (2005, 2015)
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: 2005 - WINDOWS-1252, 2015 - UTF-8
#
# Resumo: Polígonos das áreas urbanizadas brasileiras.
# Informação do Sistema de Referência: SIRGAS 2000

# Download the data  -----------------------------------------------------------
download_urbanarea <- function(year) {

  ## 0. Generate the correct URL -----------------------------------------------

  base <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/"

  ftp_link <- switch(as.character(year),
    "2005" = paste0(base, "2005/areas_urbanizadas_do_Brasil_2005_shapes.zip"),
    "2015" = paste0(base, "2015/Shape/AreasUrbanizadasDoBrasil_2015.zip"),
    "2019" = paste0(base, "2019/Shapefile/AreasUrbanizadas2019_Brasil.zip"),
    stop(paste("Ano", year, "não suportado para urban_area"))
  )

  # baseano <- paste0(base, year, "/")
  # h <- list_folders(baseano)

  
  ## 1. Create temp folder -----------------------------------------------------

  zip_dir <- paste0(tempdir(), "/urban_area/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)

  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)

  ## 2. Download Raw data ------------------------------------------------------

  file_raw <- fs::file_temp(tmp_dir = zip_dir, ext = "zip")

  httr::GET(url = ftp_link,
            httr::progress(),
            httr::write_disk(path = file_raw, overwrite = TRUE),
            httr::timeout(300))

  ## 3. Unzip Raw data ---------------------------------------------------------

  unzip_geobr(
    zip_dir = zip_dir,
    out_zip = out_zip
    )

  shp_files <- list.files(out_zip, pattern = "\\.shp$", full.names = TRUE, recursive = TRUE)
  
  ## 4. Read and merge shapefiles ----------------------------------------------
  
  encode <- dplyr::case_when(
    year == 2005 ~ "ENCODING=WINDOWS-1252",
    year > 2005 ~ "ENCODING=UTF-8"
  )

  # ler e empilhar arquivos
  # 2005: 3 shapefiles com colunas diferentes
  # 2015: 2 shapefiles com colunas diferentes
  urbanarea_raw <- readmerge_geobr(dirname(shp_files), encoding = encode)

  urbanarea_raw$year <- year
  
  return(urbanarea_raw)
}

# Clean the data  --------------------------------------------------------------
# urbanconcentrations_clean <- tar_read(urbanconcentrations_clean)
# urbanarea_raw <- tar_read(urbanarea_raw, 2)

clean_urbanarea <- function(urbanarea_raw, urbanconcentrations_clean) {

  ## 0. Create folder to save clean data ---------------------------------------
  yyyy <- urbanarea_raw$year[1]
  dir_clean <- paste0("./data/urban_area/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  ## 1. Rename and select columns (year-specific) ------------------------------

  if (yyyy == 2005) {
    temp_sf <- urbanarea_raw |>
      dplyr::mutate(type = "Área urbanizada") |>
      dplyr::select(
        code_muni    = geocodigo,
        name_muni    = nome_munic,
        # code_urb     = GEOC_URB,
        abbrev_state = uf,
        # pop_2005     = POP_2005,
        type,
        density      = tipo,
        # area_km2     = Area_Km2,
        geometry     = geometry
      )
  }

  if (yyyy == 2015) {
    # Os 2 shapefiles 2015 tem coluna de nome com grafia diferente
    # (NomeConcUr vs NomConcUrb); coalesce recupera o valor correto.
    # NOTA: code_muni aqui e o codigo da AGLOMERACAO URBANA (CodConcUrb),
    # nao do municipio — mantido por compatibilidade com usuarios downstream.
    # code_state sera derivado dos 2 primeiros digitos de code_muni (linha ~155).
    temp_sf <- urbanarea_raw |>
      dplyr::mutate(
        name_muni = dplyr::coalesce(nome_conc_ur, nom_conc_urb)
      ) |>
      dplyr::select(
        code_muni = cod_conc_urb,
        name_muni,
        type    = tipo,
        density = densidade,
        geometry
      )
  }

  if (yyyy == 2019) {
    temp_sf <- urbanarea_raw |>
      dplyr::select(
        type         = tipo,
        density      = densidade,
        # comparacao   = Comparacao,
        geometry     = geometry
      )
  }

  ## 2. Add state/region info --------------------------------------------------

  # 2019 nao vem com code_muni/name_muni: deriva via centroide x municipio
  if (yyyy == 2019) {

    # filtra municipality_clean (vetor de paths) ao ano relevante
    muni_files <- urbanconcentrations_clean[
      grepl(as.character(yyyy), urbanconcentrations_clean) &
      !grepl("simplified", urbanconcentrations_clean)
    ]

    muni <- arrow::open_dataset(muni_files) |>
      sf::st_as_sf() |>
      dplyr::select(code_muni, name_muni)

    # identifica qual municipio que cai o centroide de cada mancha
    temp_sf$tempid <- 1:nrow(temp_sf)
    temp_centroids <- duckspatial::ddbs_centroid(temp_sf)
    temp_centroids <- duckspatial::ddbs_join(x = temp_centroids, y = muni) |>
      duckspatial::ddbs_collect() |>
      dplyr::select(tempid, code_muni, name_muni) |>
      sf::st_drop_geometry()

    temp_sf <- left_join(temp_sf,temp_centroids) |>
      dplyr::select(-tempid)

  }

  # add state info
  temp_sf$code_state <- substr(temp_sf$code_muni, 1, 2)


  ## 3. Apply harmonize geobr cleaning -----------------------------------------

  temp_sf <- harmonize_geobr(
    temp_sf = temp_sf,
    year = yyyy,
    add_state = TRUE, 
    state_column = "code_state",
    add_region = TRUE, 
    region_column = "code_state",
    add_snake_case = TRUE,
    snake_colname = "name_muni",
    projection_fix = TRUE,
    encoding_utf8 = TRUE,
    topology_fix = TRUE,
    remove_z_dimension = TRUE,
    use_multipolygon = TRUE
  )

  
  ## 3c. Enforce column order (geometry always last)
  temp_sf <- temp_sf |>
    dplyr::select(code_muni, name_muni, type, density,
                  code_state, abbrev_state, name_state,
                  code_region, name_region,
                  year, geometry)
  

  # sort by key columns
  temp_sf <- temp_sf |> 
    dplyr::arrange(code_state, code_muni)
  
  
  
  ## 3d. Validate
  stopifnot(is.numeric(temp_sf$code_state))
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")

  ## 4. Lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 50)

  ## 5. Save datasets  ---------------------------------------------------------

  write_geobr_parquet(
    sf_obj = temp_sf, 
    path = paste0(dir_clean, "/urbanareas_", yyyy, ".parquet"))

  write_geobr_parquet(
    sf_obj = temp_sf_simplified,
    path = paste0(dir_clean, "/urbanareas_", yyyy, "_simplified.parquet"))

  ## 6. Return file list -------------------------------------------------------

  files <- list.files(path = dir_clean,
                      pattern = ".parquet",
                      recursive = TRUE,
                      full.names = TRUE)

  return(files)
}
