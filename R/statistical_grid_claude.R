# #> DATASET: statistical grid 2010, 2022
# #> Source: IBGE - https://geoftp.ibge.gov.br/recortes_para_fins_estatisticos/grade_estatistica/
# #> Metadata:
# # Titulo: Grade Estatistica
# # Titulo alternativo: Statistical Grid
# # Frequencia de atualizacao: Ocasionalmente (censos)
# #
# # Forma de apresentacao: Shape
# # Linguagem: Pt-BR
# # Character set: UTF-8
# #
# # Resumo: Grade estatistica regular do Brasil (quadriculas de 1km e
# #          agregacoes em 5km, 10km, 50km, 100km e 500km).
# # Informacao do Sistema de Referencia: SIRGAS 2000 (EPSG:4674)
# #
# # Anos disponiveis: 2010, 2022
# #
# # Estrutura FTP:
# #   2010: censo_2010/grade_id{XX}.zip  (56 arquivos por quadrante)
# #   2022: censo_2022/grade_estatistica/grade_id{XX}.zip (56 arquivos)
# #
# # NOTA: Downloads grandes (~2-6GB total por ano). Cada zip contem um
# #       shapefile por quadrante da articulacao nacional.
# #       Para evitar OOM, download retorna caminhos e clean processa
# #       quadrante a quadrante.
# 
# # Download the data  -----------------------------------------------------------
# download_statsgrid <- function(year) {
# 
#   ## 0. Build base URL per year -----------------------------------------------
# 
#   base_url <- switch(as.character(year),
#     "2010" = paste0(
#       "https://geoftp.ibge.gov.br/recortes_para_fins_estatisticos/",
#       "grade_estatistica/censo_2010/"
#     ),
#     "2022" = paste0(
#       "https://geoftp.ibge.gov.br/recortes_para_fins_estatisticos/",
#       "grade_estatistica/censo_2022/grade_estatistica/"
#     ),
#     stop(paste("Ano", year, "nao suportado para statistical_grid"))
#   )
# 
#   ## 1. Create temp folder ----------------------------------------------------
# 
#   zip_dir <- paste0(tempdir(), "/statsgrid/", year)
#   dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
# 
#   out_zip <- paste0(zip_dir, "/unzipped/")
#   dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
# 
#   ## 2. Scrape file list from FTP directory ------------------------------------
# 
#   page <- rvest::read_html(base_url)
#   all_links <- page |> rvest::html_nodes("a") |> rvest::html_attr("href")
#   filenames <- all_links[grepl("\\.zip$", all_links, ignore.case = TRUE)]
# 
#   message("Encontrados ", length(filenames), " arquivos para download (year=", year, ")")
# 
#   ## 3. Download zip files in parallel -----------------------------------------
# 
#   urls      <- paste0(base_url, filenames)
#   destfiles <- paste0(zip_dir, filenames)
# 
#   # Filter out already-downloaded files
#   already_ok <- file.exists(destfiles) & file.size(destfiles) > 1000
#   if (any(already_ok)) {
#     message("Pulando ", sum(already_ok), " arquivos ja baixados")
#   }
# 
#   if (any(!already_ok)) {
#     message("Baixando ", sum(!already_ok), " arquivos em paralelo...")
#     results <- curl::multi_download(
#       urls      = urls[!already_ok],
#       destfiles = destfiles[!already_ok],
#       progress  = TRUE,
#       timeout   = 600
#     )
#     failed <- results[results$success == FALSE, ]
#     if (nrow(failed) > 0) {
#       warning("Falha ao baixar ", nrow(failed), " arquivo(s): ",
#               paste(basename(failed$destfile), collapse = ", "))
#     }
#   }
# 
#   ## 4. Unzip all files -------------------------------------------------------
# 
#   unzip_geobr(
#     zip_dir = zip_dir,
#     out_zip = out_zip
#   )
# 
#   ## 5. Return list of shapefile paths (NOT the data) -------------------------
#   # To avoid OOM, we return paths. clean_statsgrid reads them one by one.
# 
#   shp_files <- list.files(out_zip, pattern = "\\.shp$",
#                           full.names = TRUE, recursive = TRUE)
# 
#   message("Encontrados ", length(shp_files), " shapefiles para processar")
# 
#   # Return a lightweight list with paths and year
#   result <- list(
#     shp_files = shp_files,
#     year      = year
#   )
# 
#   return(result)
# }
# 
# # Clean the data ---------------------------------------------------------------
# clean_statsgrid <- function(statsgrid_raw, year) {
# 
#   ## 0. Create folder to save clean data --------------------------------------
# 
#   dir_clean <- paste0("./data/statistical_grid/", year)
#   dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)
# 
#   ## 1. Extract shapefile paths from download result --------------------------
# 
#   shp_files <- statsgrid_raw$shp_files
# 
#   message("Processando ", length(shp_files), " quadrantes para ano ", year)
# 
#   ## 2. Process each quadrant shapefile individually --------------------------
#   # Read, clean, and accumulate. Process in batches to limit memory.
# 
#   all_chunks <- list()
# 
#   for (i in seq_along(shp_files)) {
#     shp_path <- shp_files[i]
#     message("Processando quadrante ", i, "/", length(shp_files), ": ",
#             basename(shp_path))
# 
#     chunk <- tryCatch({
#       sf::st_read(shp_path, quiet = TRUE, stringsAsFactors = FALSE,
#                   options = "ENCODING=UTF-8")
#     }, error = function(e) {
#       warning("Erro ao ler ", shp_path, ": ", conditionMessage(e))
#       return(NULL)
#     })
# 
#     if (is.null(chunk) || nrow(chunk) == 0) next
# 
#     # Standardize column names
#     chunk <- chunk |> janitor::clean_names()
# 
#     # Select and rename columns (year-specific)
#     if (year == 2010) {
#       chunk <- chunk |>
#         dplyr::select(
#           id_unico,
#           nome_1km,
#           nome_5km,
#           nome_10km,
#           nome_50km,
#           nome_100km,
#           nome_500km,
#           pop_total  = pop,
#           pop_fem    = fem,
#           pop_masc   = masc,
#           dom_ocu    = dom_ocu,
#           quadrante,
#           geometry
#         )
#     }
# 
#     if (year == 2022) {
#       chunk <- chunk |>
#         dplyr::select(
#           id_unico,
#           nome_1km,
#           nome_5km,
#           nome_10km,
#           nome_50km,
#           nome_100km,
#           nome_500km,
#           pop_total  = total,
#           total_dom,
#           quadrante,
#           geometry
#         )
#     }
# 
#     all_chunks[[i]] <- chunk
#     rm(chunk)
#   }
# 
#   ## 3. Bind all quadrants ----------------------------------------------------
# 
#   message("Unindo todos os quadrantes...")
#   temp_sf <- dplyr::bind_rows(all_chunks)
#   rm(all_chunks)
#   gc()
# 
#   ## 4. Ensure numeric types for population columns ---------------------------
# 
#   numeric_cols <- intersect(
#     c("pop_total", "pop_fem", "pop_masc", "dom_ocu", "total_dom"),
#     names(temp_sf)
#   )
#   for (col in numeric_cols) {
#     temp_sf[[col]] <- as.numeric(temp_sf[[col]])
#   }
# 
#   ## 5. Apply harmonize_geobr ------------------------------------------------
#   # No state/region columns (grid cells are not admin units)
#   # No snake_case (column names already standardized)
#   # No topology_fix (regular grid cells are valid by construction)
#   # use_multipolygon = FALSE (grid cells should NOT be dissolved)
# 
#   temp_sf <- harmonize_geobr(
#     temp_sf            = temp_sf,
#     year               = year,
#     add_state          = FALSE,
#     add_region         = FALSE,
#     add_snake_case     = FALSE,
#     projection_fix     = TRUE,
#     encoding_utf8      = FALSE,
#     topology_fix       = FALSE,
#     remove_z_dimension = TRUE,
#     use_multipolygon   = FALSE
#   )
# 
#   ## 5b. Convert to MULTIPOLYGON without dissolve
#   temp_sf <- sf::st_cast(temp_sf, "MULTIPOLYGON")
# 
#   ## 5c. Ensure sf class and CRS after harmonize (known issue)
#   temp_sf <- dplyr::ungroup(temp_sf)
#   if (!inherits(temp_sf, "sf")) temp_sf <- sf::st_as_sf(temp_sf)
#   if (is.na(sf::st_crs(temp_sf))) sf::st_crs(temp_sf) <- 4674
# 
#   ## 5d. Reorder columns (year + geometry last)
#   temp_sf <- temp_sf |>
#     dplyr::relocate(geometry, .after = dplyr::last_col())
# 
#   ## 6. Validate --------------------------------------------------------------
# 
#   stopifnot(!is.na(sf::st_crs(temp_sf)))
#   stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
#   stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")
# 
# 
#   ## 8. Save datasets ---------------------------------------------------------
# 
#   write_geobr_parquet(
#     temp_sf,
#     paste0(dir_clean, "/statistical_grid_", year, ".parquet"))
# 
#   rm(temp_sf)
#   gc()
# 
#   ## 9. Return file list ------------------------------------------------------
# 
#   files <- list.files(
#     path       = dir_clean,
#     pattern    = ".parquet",
#     recursive  = TRUE,
#     full.names = TRUE
#   )
# 
#   return(files)
# }
