#> DATASET: Health Regions
#> Source: DATASUS FTP — ftp://ftp.datasus.gov.br/territorio/mapas/
#> Metadata:
# Titulo: Regioes de Saude do SUS
# Titulo alternativo: health regions
# Frequencia de atualizacao:
#
# Forma de apresentacao: .MAP binario DATASUS (leitura via read_datasus_map)
# Linguagem: Pt-BR
# Character set: WINDOWS-1252 (.MAP)
# Character set: 2005 - WINDOWS-1252
#                2015 - UTF-8

# Resumo: Criado a partir do Decreto n. 7508 de junho de 2011, em substituicao aos
# # Colegiados de Gestao Regional (oriundos do Pacto pela Saude), o CIR a um colegiado
# # no qual participam as Secretarias Municipais de Saude, de uma dada regiao, e a Secretaria
# # de Estado de saude com o objetivo de promover a gestao colaborativa no setor saude do estado.
#
# Estado: Em desenvolvimento
# Palavras chaves descritivas: CIR; RAS; SUS
# Informacao do Sistema de Referencia: DATASUS - SIRGAS 2000
#
# Nota: DATASUS FTP disponibiliza formato .MAP binario proprietario.
#       Leitura via read_datasus_map() em support_harmonize_geobr.R (leitor puro R).



# !!!!!!!!!!!!!!!!!!!!!!! antes de 2023, usamos os poligonos das regioes de saude

# Download the data  -----------------------------------------------------------
download_healthregions_old <- function(year){ # year = 2013
  
  dest_dir <- paste0(tempdir(), "/health_region/", year)
  dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
  

  ## 1. Build DATASUS FTP URL
  ftp_url <- paste0("ftp://ftp.datasus.gov.br/territorio/mapas/br_mapas_", year, ".zip")

  ## 2. Download to temp dir
  dest_zip <- paste0(dest_dir, "/br_mapas_", year, ".zip")


  ## 2. Download Raw data ------------------------------------------------------

  file_raw <- download_file_geobr(
    file_url = ftp_url,
    dest_dir = dest_dir
  )


  if (!file.exists(dest_zip) || file.size(dest_zip) < 1000) {
    stop(paste("Download falhou para health_region ano", year,
               "- arquivo nao encontrado ou vazio. URL:", ftp_url))
  }

  ## 3. unzip
  files <- unzip_geobr(
    zip_dir = dest_dir, 
    out_zip = dest_dir
    )
  
  ## 4. Read .MAP binary with pure-R reader
  # The .MAP reader returns code_health_region, name_health_region, geometry
  
  file_microregioes <-  files[files %like% 'br_regsaud.MAP']
  microhealthregions_raw <- read_datasus_map(file_microregioes)
  
  file_macroregioes <-   files[files %like% 'br_macsaud.MAP']
  macrohealthregions_raw <- read_datasus_map(file_macroregioes)
  
  
  # Rename columns to match expected names
  names(microhealthregions_raw)[1:2] <- c("code_health_region", "name_health_region")
  names(macrohealthregions_raw)[1:2] <- c("code_health_macroregion", "name_health_macroregion")
  
  microhealthregions_raw$year <- year
  macrohealthregions_raw$year <- year
  
  healthregions_raw <- list(microhealthregions_raw, macrohealthregions_raw)
  
  return(healthregions_raw)
}

# Clean the data  --------------------------------------------------------------
# healthregions_raw <- tar_read(healthregions_raw_old, 1)
# municipality_clean <- tar_read(municipality_clean)
# hist_muni_clean <- tar_read(hist_muni_clean)

clean_healthregions_old <- function(healthregions_raw, municipality_clean, hist_muni_clean){
  
  ## 0. Create folder to save clean data ---------------------------------------
  yyyy <- healthregions_raw[[1]]$year[1]
  dir_clean <- paste0("./data/health_region/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  # load muni geometries
  temp_yyyy <- ifelse(yyyy %in% c(1994, 1997), 1991, yyyy)
  
  munis <- c(hist_muni_clean, municipality_clean)
  munis <- munis[grep(temp_yyyy, munis)]
  munis <- munis[!grepl('simplified', munis)] |>
    arrow::open_dataset() |>
    sf::st_as_sf() |>
    dplyr::mutate(
      code_muni6 = substring(code_muni, 1, 6) |> as.numeric()
      ) |> 
    dplyr::select(- year)
    

  # find munis in each region and macro region with join operation
  find_muni_health_match <- function(healthregions, munis) { # healthregions <- healthregions_raw[[1]]
    
    munis <- harmonize_projection(munis)
    healthregions <- harmonize_projection(healthregions)
    
    munis_centroids <- munis |> 
      dplyr::select(dplyr::any_of(c('code_muni', 'code_muni6'))) |> 
      duckspatial::ddbs_centroid(method = "surface") |>
      dplyr::filter(!is.na(code_muni)) |> 
      dplyr::filter(code_muni < 9000000) |> 
      dplyr::filter(code_muni > 1000000)
    
    
    # munis_centroids <- duckspatial::ddbs_collect(munis_centroids)
    # mapview::mapview(healthregions) + munis +munis_centroids
    
    muni_health <- duckspatial::ddbs_join(munis_centroids, healthregions) |> 
      duckspatial::ddbs_collect() |> 
      dplyr::select(-code_muni6) |> 
      sf::st_drop_geometry()
    
    return(muni_health)
  }
  
  micro <- find_muni_health_match(healthregions_raw[[1]], munis)
  macro <- find_muni_health_match(healthregions_raw[[2]], munis)
    
    
  # join mmicro and macro regions
  healthregions <- dplyr::left_join(
    x = munis,
    y = micro
    ) |>
    dplyr::left_join(y = macro)
    
    
  ## 3. Apply harmonize geobr cleaning
    temp_sf <- harmonize_geobr(
      temp_sf = healthregions,
      year = yyyy,
      add_state = F,
      # state_column = "code_state",
      add_region = F,
      # region_column = "code_state",
      add_snake_case = TRUE,
      snake_colname = c("name_health_region" , "name_health_macroregion"),
      projection_fix = TRUE,
      encoding_utf8 = TRUE,
      topology_fix = TRUE,
      remove_z_dimension = TRUE,
      use_multipolygon = TRUE
    )
    
    
  # clean result
    temp_sf <- temp_sf |> 
    dplyr::filter(!is.na(code_muni)) |> 
    dplyr::filter(!is.na(name_health_region))
  
  # Enforce column order (year and geometry always last)
  temp_sf <- temp_sf |>
    dplyr::relocate(
      code_muni, name_muni,
      dplyr::any_of(c('code_health_region', 'name_health_region', 'code_health_macroregion', 'name_health_macroregion'))
                    ) |>
    dplyr::relocate(year, geometry, .after = dplyr::last_col())
  
  
  ## 7. Sort
  temp_sf <- temp_sf |>
    dplyr::arrange(code_state, code_muni, code_health_region)
  

  ## 4. lighter version --------------------------------------------------------
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)

  ## 5. Save datasets  ---------------------------------------------------------
  
  write_geobr_parquet(
    sf_obj = temp_sf,
    path = paste0(dir_clean, "/healthregions_", yyyy, ".parquet")
    )

  write_geobr_parquet(
    sf_obj= temp_sf_simplified,
    path = paste0(dir_clean, "/healthregions_", yyyy, "_simplified.parquet")
    )

  ## 6. Create the files for geobr index  --------------------------------------

  files <- list.files(path = dir_clean,
                      pattern = ".parquet",
                      recursive = TRUE,
                      full.names = TRUE)

  return(files)
}

