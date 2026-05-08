#> DATASET: weighting areas (areas de ponderacao)
#> Source: IBGE - https://geoftp.ibge.gov.br/recortes_para_fins_estatisticos/malha_de_areas_de_ponderacao/
#> Metadata:
# Titulo: Areas de Ponderacao
# Frequencia de atualizacao: Censal (2010)
# Forma de apresentacao: Shapefile per-state
# Linguagem: Pt-BR
# Character set: UTF-8
# Informacao do Sistema de Referencia: SIRGAS 2000

# # Download the data  -----------------------------------------------------------

# os dados disponiveis no ftp estao com erros
# a construcao de areas de pond pela juncao de setores censitarios eh
# problematica por conta de descontinuidades causadas por setores sem populacao
# POR ISSO, aqui usamos uma estrategia simples que eh baixar uma versao legado
# das areas de pond de 2010

download_weightarea <- function(year) { # year = 2010
  
  file_url <- "https://github.com/ipeaGIT/geobr_prep_data/releases/download/legacy/weightingarea_ibge_2010.parquet"
  
  file <- download_file_geobr(file_url)
  
  weightarea_raw <- arrow::open_dataset(file) |>
    sf::st_as_sf()
  
  weightarea_raw <- weightarea_raw |>
    dplyr::rename(geometry = geom) |>
    sf::st_set_geometry("geometry")
  
  weightarea_raw$year <- year
  
  return(weightarea_raw)
  
}


# download_weightarea <- function(year) {
# 
#   ## get URLs for all 27 states -------------------------------------------
# 
#   if (year==2010){
#     
#     # areas originais
#     base_url <- "https://geoftp.ibge.gov.br/recortes_para_fins_estatisticos/malha_de_areas_de_ponderacao/censo_demografico_2010/"
# 
#     files <- list_folders(base_url)
#     files <- files[ files %like% ".zip"]
#     state_files <- files[! files%like% "__.zip"]
#     urls <- paste0(base_url, state_files)
#     
#     # areas redesenhadas para alguns municipios
#     base_url2 <- "https://geoftp.ibge.gov.br/recortes_para_fins_estatisticos/malha_de_areas_de_ponderacao/censo_demografico_2010/municipios_areas_redefinidas/"
#     files2 <- list_folders(base_url2)
#     state_files2 <- files2[ files2 %like% ".zip"]
#     urls2 <- paste0(base_url2, state_files2)
#     
#     }
#   
#   
#   
#   ## 2. Create temp directories ------------------------------------------------
# 
#   tmp_dir <- paste0(tempdir(), "/weighting_area")
#   zip_dir <- paste0(tmp_dir, "/zips")
#   shp_dir <- paste0(tmp_dir, "/shps")
#   dir.create(zip_dir, recursive = TRUE, showWarnings = FALSE)
#   dir.create(shp_dir, recursive = TRUE, showWarnings = FALSE)
# 
# 
#   ## 3. Download states sequentially -------------------------------------------
#   destfiles <- paste0(zip_dir, "/", state_files)
#   
#   downloaded_files <- curl::multi_download(
#     urls = urls,
#     destfiles = destfiles,
#     resume = TRUE,
#     progress = FALSE
#   )
# 
#   
#   # Retry in anything failed
#   if(any(isFALSE(downloaded_files$success))){
# 
#     downloaded_files <- curl::multi_download(
#       urls = urls,
#       destfiles = destfiles,
#       resume = TRUE,
#       progress = FALSE
#     )    
#   }
# 
#   # baixa tambem areas redesenhadas
#   if(year==2010) {
#     destfiles2 <- paste0(zip_dir, "/", state_files2)
#     
#     downloaded_files <- curl::multi_download(
#       urls = urls2,
#       destfiles = destfiles2,
#       resume = TRUE,
#       progress = FALSE
#     )
#   }
# 
#   ## 4. Unzip Raw data ---------------------------------------------------------
#   
#   # aqui a gente nao usa unzip_geobr() pq tem caracter especial no nome de arquivo
#   out_zip <- paste0(zip_dir, "/unzipped/")
#   dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
#   dir.exists(out_zip)
#   
#   for (i in seq_along(destfiles)) {
#     # system unzip handles non-UTF-8 filenames in ZIPs (e.g. "Rondônia")
#     # that utils::unzip() rejects with "string multibyte inválida"
#     system2("unzip", args = c("-joq", shQuote(destfiles[i]), "-d", shQuote(out_zip)),
#             stdout = FALSE, stderr = FALSE)
# 
#     }
# 
#   # baixa tambem areas redesenhadas
#   if(year==2010) {
#     
#     out_zip2 <- paste0(zip_dir, "/unzipped2/")
#     dir.create(out_zip2, showWarnings = FALSE, recursive = TRUE)
#     
#     for (i in seq_along(destfiles2)) {
#       # system unzip handles non-UTF-8 filenames in ZIPs (e.g. "Rondônia")
#       # that utils::unzip() rejects with "string multibyte inválida"
#       system2("unzip", args = c("-joq", shQuote(destfiles[i]), "-d", shQuote(out_zip2)),
#               stdout = FALSE, stderr = FALSE)
#       }
#     
#     shp_files2 <- list.files(path = out_zip2, pattern = ".shp", full.names = T)
#     
#   }
#   
#   ## 6. Bind Raw data together -------------------------------------------------
#   
#   encoding <- ifelse(year==2010, "ENCODING=WINDOWS-1252", "UTF-8")
#   
#   shp_files <- list.files(path = out_zip, pattern = ".shp", full.names = T)
#   shp_files <- shp_files[!data.table::like(shp_files, ".prj")]
#   
#   weightarea_raw1 <- lapply(
#     X = shp_files, 
#     FUN = function(shp_file){
#       sf_data <- sf::st_read(shp_file, quiet = TRUE, stringsAsFactors = FALSE,
#                              options = encoding)
#       
#       sf_data <- harmonize_projection(sf_data)
#       return(sf_data)
#     } 
#     ) |> 
#     dplyr::bind_rows() |> 
#     janitor::clean_names()
#   
#   # 2010 leitura de areas redefinidas
#   if (year==2010) {
#     
#     weightarea_raw2 <- lapply(
#       X = shp_files2, 
#       FUN = function(shp_file){ # shp_file = shp_files2[28]
#         message(shp_file)
#         sf_data <- sf::st_read(shp_file, quiet = TRUE, stringsAsFactors = FALSE,
#                                options = encoding)
#         
#         sf_data <- harmonize_projection(sf_data)
#         return(sf_data)
#       } 
#     ) |> 
#       dplyr::bind_rows() |> 
#       janitor::clean_names()  
#     }
#   
#   
#   # Rename weighting area code (column name varies) and fix NAs
#   # e substituir areas originais pelas areas redefinidas
#   if (year == 2010){
#     
#     sf_data1 <- weightarea_raw1 |>
#       dplyr::select(area_pond , cd_aponde, cd_a_ponde, geometry)
#     
#     sf_data2 <- weightarea_raw2 |>
#       dplyr::select(cd_aponde, cd_a_ponde, geometry)
#     
#     # remove areas with code NA
#     sf_data1 <- sf_data1 |> 
#       mutate(code_weighting = ifelse(is.na(area_pond), cd_a_ponde, area_pond)) |> 
#       mutate(code_weighting = ifelse(is.na(code_weighting), cd_aponde, code_weighting)) |> 
#       mutate(code_muni = substr(code_weighting, 1, 7))
#     
#     sf_data2 <- sf_data2 |> 
#       mutate(code_weighting = ifelse(is.na(cd_aponde), cd_a_ponde, cd_aponde)) |> 
#       mutate(code_muni = substr(code_weighting, 1, 7))
#     
#     # substituir
#     weightarea_raw <- sf_data1 |>
#       filter(! code_muni %in% unique(sf_data2$code_muni)) |> # remover municipios antigos
#       dplyr::rows_append(sf_data2) |>                        # adicionar os novos
#       dplyr::select(code_weighting, geometry)
#     
#     # checa se o numero de areas substituidas esta correto
#     stopifnot(nrow(weightarea_raw) == nrow(weightarea_raw1))
#     
#     }
#   
# 
#   # add year
#   weightarea_raw$year <- year
# 
#   
#   if (year == 2022){
#     stop()
#   }
# 
# 
#   return(weightarea_raw)
# }

# Clean the data  --------------------------------------------------------------
# weightarea_raw <- tar_read(weightarea_raw, 1)
# years_weightarea <- tar_read(years_weightarea)
# censustract_clean <- tar_read(censustract_2010_clean)
clean_weightarea <- function(weightarea_raw) {

  yyyy <- weightarea_raw$year[1]
  dir_clean <- paste0("./data/weighting_area/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  # # drop simplified geometries
  # censustract_clean <- censustract_clean[grepl(2010, censustract_clean)]
  # censustract_clean <- censustract_clean[!grepl("simplified", censustract_clean)]
  # 
  # # open census tracts
  # temp_tracts <- arrow::open_dataset(censustract_clean) |> 
  #   sf::st_as_sf()
  # 
  # 
  # # dissolve polygons
  # # for some reason, using ddbs union on the whole table crashes R
  # # so we use the split strategy
  # weighting_areas <- dissolve_polygons_split(
  #   mysf = temp_tracts, 
  #   group_column = "code_weighting"
  #   )
  # 
  # # recover other columns
  # temp <- temp_tracts |> 
  #   sf::st_drop_geometry() |> 
  #   dplyr::select(code_weighting, code_muni, name_muni, code_state) |> 
  #   unique() |> 
  #   dplyr::filter(!is.na(code_weighting))
  # 
  # weighting_areas <- left_join(weighting_areas, temp)
  
  # get code cols to numeric
  weighting_areas <- code_cols_to_numeric(weightarea_raw)    
  
  
  ## 4.  Harmonize  -----------------
  
  temp_sf <- harmonize_geobr(
      temp_sf        = weighting_areas,
      year           = yyyy,
      add_state      = T,
      state_column   = "code_state",
      add_region     = T,
      region_column  = "code_state",
      add_snake_case = T,
      snake_colname  = "name_muni",
      projection_fix = TRUE,
      encoding_utf8  = TRUE,
      topology_fix   = TRUE,
      remove_z_dimension = TRUE,
      use_multipolygon   = TRUE
    )


  # Column order
  temp_sf <- temp_sf |>
      dplyr::select(code_weighting, 
                    code_muni, name_muni,
                    code_state, abbrev_state, name_state,
                    code_region, name_region,
                    year, geometry
                    )
  
  # sort by key columns
  temp_sf <- temp_sf |> 
    dplyr::arrange(code_state, code_muni, code_weighting)
  
  # Simplify
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 10)
  
  
  
  # save   -----------------
  write_geobr_parquet(
    sf_obj = temp_sf, 
    path = paste0(dir_clean, "/weightingareas_", yyyy, ".parquet")
  )
  
  write_geobr_parquet(
    sf_obj = temp_sf_simplified, 
    path = paste0(dir_clean,"/weightingareas_", yyyy, "_simplified", ".parquet")
  )
  

  all_files <- list.files(path = dir_clean, 
                          pattern = ".parquet", 
                          recursive = TRUE, 
                          full.names = TRUE)
  
  return(all_files)
}
