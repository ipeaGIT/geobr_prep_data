#> DATASET: statistical grid 2010, 2022
#> Source: ###### IBGE - ftp://geoftp.ibge.gov.br/recortes_para_fins_estatisticos/grade_estatistica/censo_2010/
#########: scale 1:5.000.000
#> Metadata: #####
# Título: Grade Estatística
# Título alternativo: Statistical Grid 2010 Census
# Frequência de atualização: Ocasionalmente
#
# Forma de apresentação: #####Shape
# Linguagem: Pt-BR
# Character set: UTF-8
#
# Resumo: ##### Poligonos e Pontos do biomas brasileiros.
# Informações adicionais: Dados produzidos pelo IBGE, e utilizados na elaboracao do shape da base estatística com a melhor base oficial disponível.
# Propósito: Disponibilização da grade estatística do Brasil.
#
# Estado: Em desenvolvimento
# Palavras-chaves descritivas: ****
# Informação do Sistema de Referência: #####SIRGAS 2000
#
# Observações:
# Anos disponíveis: 2010, 2022
# Este dataset não está salvando em parquet e o harmonize geobr demora muito.

# Download the data  ----
download_statsgrid <- function(year) {
  
  ## 0. Get the correct url and file names (UPDATE YEAR) -----------------------

  if (year == 2010) {
    url = paste0(
      "https://geoftp.ibge.gov.br/recortes_para_fins_estatisticos/",
      "grade_estatistica/censo_",
      year,
      "/"
    )
  }

  if (year == 2022) {
    url = paste0(
      "https://geoftp.ibge.gov.br/recortes_para_fins_estatisticos/",
      "grade_estatistica/censo_",
      year,
      "/grade_estatistica/"
    )
  }

  ## 1. Create temp folder -----------------------------------------------------

  zip_dir <- paste0(tempdir(), "/statsgrid/", year)
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)

  file_raw <- fs::file_temp(tmp_dir = zip_dir, ext = fs::path_ext(url))

  ## 2. Generate file names (CHANGE PROCESSING HERE) ---------------------------

  page <- rvest::read_html(url)

  # Extrai todos os links (tags <a>) e pega o atributo 'href'
  all_links <- page |> rvest::html_nodes("a") |> rvest::html_attr("href")
  filenames <- all_links[grepl("\\.zip$", all_links, ignore.case = TRUE)]
  filenames

  # filenames <- filenames[1:3]
  # filenames

  #### Create direction for each download

  ## 3. Download Raw data ------------------------------------------------------

  all_urls <- paste0(url, filenames)
  
  file_raw <- download_file_geobr(
    file_url = all_urls,
    dest_dir = zip_dir
  )
  
  
  ## 4. Unzip Raw data ---------------------------------------------------------

  # directory of zips
  zip_names <- list.files(zip_dir, pattern = "\\.zip", full.names = TRUE)

  # unzip folder
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)

  unzip_geobr(
    zip_dir = zip_dir,
    out_zip = out_zip
  )

  ## 5. Bind Raw data together -------------------------------------------------

  statsgrid_raw <- readmerge_geobr(
    folder_path = out_zip,
    encoding = "ENCODING=WINDOWS-1252"
  )
  

  statsgrid_raw <- statsgrid_raw |> 
    janitor::clean_names()
  
  # add year column
  statsgrid_raw$year <- year

  # dplyr::glimpse(statsgrid_raw)
  
  return(statsgrid_raw)
}

# Clean the data ---------------------------------------------------------------
# statsgrid_raw <- tar_read(statsgrid_raw, 1)
# municipality_files <- tar_read(municipality_clean)
clean_statsgrid <- function(statsgrid_raw, municipality_files) {
  
  ## 0. Create folder to save clean data ---------------------------------------
  yyyy <- statsgrid_raw$year[1]
  dir_clean <- paste0("./data/statistical_grid/", yyyy)
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)

  # read municipality data set 
  municipality_files <- municipality_files[!grepl("simplified", municipality_files)]
  municipality_files <- municipality_files[data.table::like(municipality_files, yyyy)]
  all_munis_sf <- read_geoparquet(municipality_files)
  
  # drop unnecessary columns
  statsgrid_raw <- statsgrid_raw |> 
    dplyr::select(-any_of(c("shape_leng", "shape_area")))
  
  
  ## 1. add muni and state colnames ------------------------------------------------------------

  add_muni_cols <- function(df, all_munis){

    # creates a duckdb
    conn <- duckspatial::ddbs_create_conn()
    on.exit(duckspatial::ddbs_stop_conn(conn), add = TRUE)

    # write sf to duckdb
    duckspatial::ddbs_register_table(
      conn = conn, 
      data = all_munis, 
      name = "munis", 
      overwrite = TRUE
      )
  
    duckspatial::ddbs_write_table(
      conn = conn, 
      data = df, 
      name = "statsgrid_raw", 
      overwrite = TRUE
    )
  
    # centroid of grid cells
    duckspatial::ddbs_centroid(
      x = "statsgrid_raw", 
      name = "grid_centroid", 
      conn = conn
      )
    
    # spatial join
    duckspatial::ddbs_join(
      conn = conn,
      x = "grid_centroid",
      y = "munis",
      join = "intersects",
      name = "table_join"
      ) 
    
     # WITH tj AS (
     #   SELECT id_unico, code_muni, name_muni, code_state, abbrev_state 
     #   FROM table_join
     # )
    query <- glue::glue(
    "CREATE OR REPLACE TEMP VIEW output AS
      SELECT
        statsgrid_raw.*,
        tj.code_muni,
        tj.name_muni,
        tj.code_state,
        tj.abbrev_state
     FROM statsgrid_raw
     LEFT JOIN table_join as tj
       ON statsgrid_raw.id_unico = tj.id_unico;"
    )
    
    DBI::dbExecute(conn, query)
    statsgrid_raw2 <- duckspatial::ddbs_read_table(conn, name = "output")

    return(statsgrid_raw2)
  }

  all_quad <- split(statsgrid_raw, f = statsgrid_raw$quadrante) |>
    purrr::map(
      .progress = TRUE,
      .f = function(x, all_munis){
        add_muni_cols(df = x, all_munis = all_munis_sf)
      }
    )
  
  # une tudo
  all_quad <- dplyr::bind_rows(all_quad)
  
  
  ## 2. Prep colnames ------------------------------------------------------------
  
  if (yyyy == 2010) {
    statsgrid_clean <- all_quad |>
      dplyr::select(
        code_muni,
        name_muni,
        id_unico,
        nome_1km,
        nome_5km,
        nome_10km,
        nome_50km,
        nome_100km,
        nome_500km,
        pop_total = pop,
        pop_fem = fem,
        pop_masc = masc,
        dom_ocu = dom_ocu,
        code_state,
        abbrev_state,
        quadrante,
        year,
        geometry
      )
  }

  if (yyyy == 2022) {
    statsgrid_clean <- all_quad |>
      dplyr::select(
        code_muni,
        name_muni,
        id_unico,
        nome_1km,
        nome_5km,
        nome_10km,
        nome_50km,
        nome_100km,
        nome_500km,
        pop_total = total,
        total_dom,
        code_state,
        abbrev_state,
        quadrante,
        year,
        geometry
      )
  }

  ## 3. Apply harmonize geobr cleaning -----------------------------------------

  temp_sf <- harmonize_geobr(
    temp_sf = statsgrid_clean,
    add_state = F,
    add_region = F,
    add_snake_case = F,
    #snake_colname = snake_colname,
    projection_fix = T, #
    encoding_utf8 = F,
    topology_fix = F, #
    remove_z_dimension = T,
    use_multipolygon = F
  )

  # sort by key columns
  temp_sf <- temp_sf |> 
    dplyr::arrange(code_state, code_muni, nome_1km)
  
  
  ## 4. Save results  ----------------------------------------------------------

  # Save in parquet
  arrow::write_parquet(
    x = temp_sf,
    sink = paste0(dir_clean, "/statsgrid_", yyyy, ".parquet"),
    compression = 'zstd',
    compression_level = 7
  )

  ## 5. Create the files for geobr index  --------------------------------------

  files <- list.files(
    path = dir_clean,
    pattern = ".parquet",
    recursive = TRUE,
    full.names = TRUE
  )

  return(files)
}


# a <- read_geoparquet("data/statistical_grid/2022/statsgrid_2022.parquet")
# 
# arrow::write_parquet(
#   x = a,
#   sink = paste0(dir_clean, "/statsgrid_", yyyy, "_22.parquet"),
#   compression = 'zstd',
#   compression_level = 22
# )
# library(geoarrow)
# 
# tictoc::tic()
# arrow::open_dataset("data/statistical_grid/2022/statsgrid_2022_22.parquet") |> 
#   sf::st_as_sf()
# tictoc::toc()
# 
# # original
# 477.42 sec elapsed
# 542.22 sec elapsed