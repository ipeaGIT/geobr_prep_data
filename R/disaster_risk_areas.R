#> DATASET: disaster_risk_areas 2010
#> Source: IBGE - ftp://geoftp.ibge.gov.br/organizacao_do_territorio/tipologias_do_territorio/populacao_em_areas_de_risco_no_brasil
#> Metadata:
# Titulo: disaster_risk_areas
# Titulo alternativo: Areas de risco de desastres naturais 2010
# Frequencia de atualizacao: ?
#
# Forma de apresentação: Shape
# Linguagem: Pt-BR
# Character set: Utf-8
#
# Resumo: Polígonos de áreas de risco de desastres de natureza hidro-climatológicas.
# Informações adicionais: Dados produzidos conjuntamente por IBGE e CEMADEN


# Download the data  -----------------------------------------------------------
download_riskdisasterareas <- function(year){ # year = 2010
  
  ## 0. Generate the correct ftp link (UPDATE YEAR HERE) -----------------------
  
  if (year==2010){
    ftp_link <- paste0("https://geoftp.ibge.gov.br/organizacao_do_territorio/",
                        "tipologias_do_territorio/",
                        "populacao_em_areas_de_risco_no_brasil/base_de_dados/",
                        "PARBR2018_BATER.zip")
    }
  
  ## 1. Create temp folder -----------------------------------------------------
  
  zip_dir <- paste0(tempdir(), "/disaster_risk_areas/")
  dir.create(zip_dir, showWarnings = FALSE, recursive = TRUE)
  dir.exists(zip_dir)
  

  ## 2. Download Raw data ------------------------------------------------------
  file_raw <- download_file_geobr(
    file_url = ftp_link, 
    dest_dir = zip_dir
    )
  
  ## 3. Unzip Raw data ---------------------------------------------------------
  
  ### unzip folder
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  files <- unzip_geobr(zip_dir = zip_dir, out_zip = out_zip)
  
  ## 4. Bind Raw data together -------------------------------------------------
  
  riskdisasterareas_raw <- readmerge_geobr(out_zip)
  

  ## 6. Show result ------------------------------------------------------------
  
  riskdisasterareas_raw$year <- year
  
  riskdisasterareas_raw <- riskdisasterareas_raw |> 
    janitor::clean_names()
  

  return(riskdisasterareas_raw)
  
}

# Clean the data  --------------------------------------------------------------
# riskdisasterareas_raw <- tar_read(riskdisasterareas_raw)
clean_riskdisasterareas <- function(riskdisasterareas_raw){
  
  yyyy <- riskdisasterareas_raw$year[1]
  
  ## 0. Create folder to save clean data ---------------------------------------
  
  dir_clean <- "./data/disaster_risk_areas/"
  dir.create(dir_clean, recursive = T, showWarnings = FALSE)
  dir.exists(dir_clean)
  
  ## 1. Rename collumns, reorder and adjust ------------------------------------
  
  ## 1. standardize colnames  ---------------------------------------------------
  riskdisasterareas <- rename_cols_geobr(riskdisasterareas_raw, dicionario_municipality) |> 
    mutate(code_state = substr(code_muni, 1, 2)) |> 
    filter(code_muni != 0)
  
  riskdisasterareas <- riskdisasterareas |>
    dplyr::select(-id, -geo_uf, -area_geo, -num)
    # dplyr::relocate(code_muni, 
    #          name_muni, 
    #          code_state, 
    #          abbrev_state, 
    #          name_state,
    #          code_region, 
    #          name_region,
    #          geo_bater, origem , acuracia, obs, num
    #          )
  
  # head(riskdisasterareas)
    
  ## 3. Apply harmonize geobr cleaning -----------------------------------------
  
  temp_sf <- harmonize_geobr(
    temp_sf = riskdisasterareas,
    add_state = T, 
    state_column = "code_state",
    add_region = T,
    region_column = "code_state",
    add_snake_case = F,
    #snake_colname = snake_colname,
    projection_fix = T,
    encoding_utf8 = T,
    topology_fix = T,
    remove_z_dimension = T,
    use_multipolygon = T
  )
  
  # glimpse(temp_sf)
  
  ## 4. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
  
  ## 5. Save datasets  ---------------------------------------------------------
  
  # Save in parquet
  write_geobr_parquet(
    temp_sf,
    paste0(dir_clean, "/disasterriskareas_", yyyy, ".parquet")
    )

  write_geobr_parquet(
    temp_sf_simplified,
    paste0(dir_clean,"/disasterriskareas_", yyyy, "_simplified.parquet")
    )
  
  ## 6. Create the files for geobr index  --------------------------------------
  
  files <- list.files(path = dir_clean, 
                      pattern = ".parquet", 
                      recursive = TRUE, 
                      full.names = TRUE)
  
  return(files)
}


