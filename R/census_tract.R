#> DATASET: census tracts (setores censitarios)
#> Source: IBGE
#>   2010: https://geoftp.ibge.gov.br/.../censo_2010/setores_censitarios_shp/
#>   2022: https://geoftp.ibge.gov.br/.../censo_2022/setores/gpkg/BR/BR_setores_CD2022.gpkg
#> Metadata:
# Titulo: Setores Censitarios
# Frequencia de atualizacao: Censal (2010, 2022)
# Linguagem: Pt-BR
# Informacao do Sistema de Referencia: SIRGAS 2000

# Download the data  -----------------------------------------------------------
download_censustract <- function(year) {

  if (year == 2000) return(download_censustract_2000(year))
  if (year == 2007) return(download_censustract_2007(year))
  if (year == 2010) return(download_censustract_2010(year))
  if (year == 2022) return(download_censustract_2022(year))
  stop(paste("Ano", year, "nao suportado para census_tract"))
}

# Clean the data  --------------------------------------------------------------
# raw <- tar_read(censustract_2022_raw)
# raw <- tar_read(censustract_2010_raw)
clean_censustract <- function(raw) {

  # 2000 uses a list return type (urban + rural + censobr tabular)
  if (is.list(raw) && !inherits(raw, "sf") && !is.null(raw$year) && raw$year[1] == 2000) {
    return(clean_censustract_2000(raw))
  }

  # 2007 follows the same list-return pattern (urban + rural + metadata)
  if (is.list(raw) && !inherits(raw, "sf") && !is.null(raw$year) && raw$year[1] == 2007) {
    return(clean_censustract_2007(raw))
  }

  yyyy <- raw$year[1]
  dir_clean <- paste0("./data/census_tract/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  ## 1. Normalize columns (year-specific) --------------------------------------

  if (yyyy == 2010) {
    temp_sf <- raw |>
      dplyr::select(
        code_tract       = cd_geocodi,
        zone             = tipo,
        code_muni        = cd_geocodm,
        name_muni        = nm_municip,
        name_neighborhood = nm_bairro,
        code_neighborhood = cd_geocodb,
        code_subdistrict = cd_geocods,
        name_subdistrict = nm_subdist,
        code_district    = cd_geocodd,
        name_district    = nm_distrit,
        year
      )
  } else if (yyyy == 2022) {
    temp_sf <- raw |>
      dplyr::select(
        code_tract       = cd_setor,
        code_muni        = cd_mun,
        name_muni        = nm_mun,
        name_neighborhood = nm_bairro,
        code_neighborhood = cd_bairro,
        code_favela = cd_fcu,
        name_favela = nm_fcu,
        code_urban_concentration = cd_concurb,
        name_urban_concentration = nm_concurb,
        # cd_aglom 
        # nm_aglom
        code_subdistrict = cd_subdist,
        name_subdistrict = nm_subdist,
        code_district    = cd_dist,
        name_district    = nm_dist,
        zone             = situacao,
        code_state       = cd_uf,
        year
      )
  }

  ## 2. Remove lakes -----------------------------------------------------------

  temp_sf <- dplyr::filter(temp_sf,
                           code_muni != 430000100000000,
                           code_muni != 430000200000000)

  # add code of weighting areas ------------------------------------------------
  if (yyyy == 2010) {
    
    # download cross-walk
    area_pond_ftp <- "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Resultados_Gerais_da_Amostra/Microdados/Documentacao.zip"
    area_pond_temp_dir <- tempdir()
    download_file_geobr(
      file_url = area_pond_ftp, 
      dest_dir = area_pond_temp_dir)
    
    files <- unzip_geobr(
      zip_dir = area_pond_temp_dir, 
      out_zip = area_pond_temp_dir
      )
    
    files <- files[files %like% "Composi"]
    files <- files[files %like% ".txt"]
    cross_walk <- readr::read_tsv(
      files,
      locale = readr::locale(encoding = "UTF-16LE"),
      col_types = readr::cols(
        .default = readr::col_character()
      )
    )
    
    names(cross_walk) <- c("code_weighting", "code_tract")
    
    # add code code_weighting cols
    temp_sf <- left_join(temp_sf, cross_walk)
    
  }
  
  
  # fix code_neighborhood of 2010
  # issue https://github.com/ipeaGIT/geobr/issues/182
  # as.numeric() pra respeitar a regra code_* = numeric (CLAUDE.md). Validado
  # empiricamente em 2010+2022: 0 NAs criados, 10 digitos, max 5.218.300.013
  # (bem abaixo do limite double seguro 9.007e15).
  if (yyyy==2010) {
    temp_sf <- temp_sf |>
      mutate(code_neighborhood = as.numeric(paste0(
        code_muni,
        substr(code_neighborhood, nchar(code_neighborhood) - 2, nchar(code_neighborhood))
      ))
      )
  }
  
  ## 3. Type conversion --------------------------------------------------------

  # code cols to numeric
  temp_sf <- code_cols_to_numeric(temp_sf)  

  ## 4.  Harmonize  -----------------
  
  temp_sf <- harmonize_geobr(
      temp_sf        = temp_sf,
      year           = yyyy,
      add_state      = TRUE,
      state_column   = "code_muni",
      add_region     = TRUE,
      region_column  = "code_state",
      add_snake_case = TRUE,
      snake_colname  = "name_muni",
      projection_fix = TRUE,
      encoding_utf8  = TRUE,
      topology_fix   = TRUE,
      remove_z_dimension = TRUE,
      use_multipolygon   = TRUE
    )
  
  
  # code cols to numeric
  temp_sf <- code_cols_to_numeric(temp_sf)

  # Column order
  if (yyyy==2010){ # keep code_weighting col in 2010
    temp_sf <- temp_sf |>
        dplyr::select(
          code_tract, 
          code_muni, name_muni,
          code_neighborhood, name_neighborhood,
          code_district, name_district,
          code_subdistrict, name_subdistrict,
          code_weighting,
          zone,
          code_state, abbrev_state, name_state,
          code_region, name_region,
          year, 
          geometry
        )
    } else {
    temp_sf <- temp_sf |>
      dplyr::select(
        code_tract, 
        code_muni, name_muni,
        code_neighborhood, name_neighborhood,
        code_district, name_district,
        code_subdistrict, name_subdistrict,
        zone,
        code_state, abbrev_state, name_state,
        code_region, name_region,
        year, 
        geometry
      )
  }

    # Validate
    stopifnot(!is.na(sf::st_crs(temp_sf)))
    stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
    stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")

    # sort by key columns
    temp_sf <- temp_sf |> 
      dplyr::arrange(code_state, code_muni, code_tract)
    
    
    # Simplify
    temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 10)

    write_geobr_parquet(
      temp_sf,
      paste0(dir_clean, "/censustracts_", yyyy, ".parquet")
      )
    
    write_geobr_parquet(
      temp_sf_simplified,
      paste0(dir_clean,"/censustracts_", yyyy, "_simplified", ".parquet")
      )

    all_files <- list.files(path = dir_clean, 
                        pattern = ".parquet", 
                        recursive = TRUE, 
                        full.names = TRUE)
    
  return(all_files)
}


# ==============================================================================
# Download 2010: parallel per-state from IBGE FTP
# ==============================================================================
download_censustract_2010 <- function(year) {

  ufs <- c("ac","al","am","ap","ba","ce","df","es","go","ma","mg","ms","mt",
           "pa","pb","pe","pi","pr","rj","rn","ro","rr","rs","sc","se","sp","to")

  base_url <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2010/setores_censitarios_shp/"

  filenames <- paste0(ufs, "_setores_censitarios.zip")
  # GO has a typo in the filename (extra space)
  filenames[ufs == "go"] <- "go_setores%20_censitarios.zip"
  urls <- paste0(base_url, ufs, "/", filenames)

  ## 1. Create temp directories
  tmp_dir <- paste0(tempdir(), "/census_tract_2010")
  zip_dir <- paste0(tmp_dir, "/zips")
  shp_dir <- paste0(tmp_dir, "/shps")
  dir.create(zip_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(shp_dir, recursive = TRUE, showWarnings = FALSE)

  destfiles <- paste0(zip_dir, "/", ufs, "_setores_censitarios.zip")

  ## 2. Download states sequentially (IBGE throttles parallel connections)
  message("[census_tract 2010] Baixando 27 estados sequencialmente...")

  for (i in seq_along(ufs)) {
    if (file.exists(destfiles[i]) && file.size(destfiles[i]) > 1000) {
      message(sprintf("[census_tract 2010] %s (%d/%d) ja existe, pulando",
                      toupper(ufs[i]), i, length(ufs)))
      next
    }
    message(sprintf("[census_tract 2010] Baixando %s (%d/%d)...",
                    toupper(ufs[i]), i, length(ufs)))
    tryCatch(
      download.file(urls[i], destfiles[i], mode = "wb", quiet = TRUE),
      error = function(e) warning(paste("Download falhou para", ufs[i], ":", conditionMessage(e)))
    )
  }

  # Check results
  ok <- file.exists(destfiles) & file.size(destfiles) > 1000
  message(sprintf("[census_tract 2010] Download: %d/%d OK", sum(ok), length(ufs)))

  # Retry failed states with increasing delays (up to 3 rounds)
  for (round in 1:3) {
    ok <- file.exists(destfiles) & file.size(destfiles) > 1000
    if (all(ok)) break
    failed_idx <- which(!ok)
    delay <- round * 3
    message(sprintf("[census_tract 2010] Retry round %d: %d estados (delay %ds)...",
                    round, length(failed_idx), delay))
    Sys.sleep(delay)
    for (i in failed_idx) {
      message(sprintf("[census_tract 2010]   Retry %s...", toupper(ufs[i])))
      tryCatch(
        download.file(urls[i], destfiles[i], mode = "wb", quiet = TRUE),
        error = function(e) warning(paste("Retry falhou para", ufs[i]))
      )
    }
  }

  ok <- file.exists(destfiles) & file.size(destfiles) > 1000
  message(sprintf("[census_tract 2010] FINAL: %d/%d OK", sum(ok), length(ufs)))
  if (!all(ok)) {
    stop(paste("Downloads falharam para:", paste(ufs[!ok], collapse = ", ")))
  }

  ## 3. Unzip data

  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(out_zip, showWarnings = FALSE, recursive = TRUE)
  dir.exists(out_zip)
  
  unzip_geobr(zip_dir = zip_dir, out_zip = out_zip)
  
  ## 3. read all states
  sf_data <- readmerge_geobr(folder_path = out_zip, 
                             encoding = "ENCODING=WINDOWS-1252")
  
    
  # Standardize column names (vary between states)
  sf_data$year <- year
  sf_data <- janitor::clean_names(sf_data)

  return(sf_data)
}


# ==============================================================================
# Download 2022: direct GPKG from IBGE
# ==============================================================================
download_censustract_2022 <- function(year) {

  ftp_link <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2022/setores/gpkg/BR/BR_setores_CD2022.gpkg"

  message("[census_tract 2022] Baixando GPKG do IBGE...")
  tmp <- tempfile(fileext = ".gpkg")
  httr::GET(url = ftp_link, httr::progress(),
            httr::write_disk(path = tmp, overwrite = TRUE),
            httr::timeout(600))

  message("[census_tract 2022] Lendo GPKG...")
  raw <- sf::st_read(tmp, quiet = TRUE)
  message(sprintf("[census_tract 2022] Total: %d rows", nrow(raw)))

  # Standardize column names (vary between states)
  raw$year <- year
  raw <- janitor::clean_names(raw)

  return(raw)
}


# ==============================================================================
# Download 2000: urban (per municipality) + rural (per state) + censobr tabular
# ==============================================================================
# Year 2000 is published by IBGE in two incompatible cartographic products that
# are complementary and disjoint:
#   - setor_urbano: ~1058 municipalities in UTM SAD69 (zone varies by UF),
#     large scale (1:5k-1:10k). ~157k sectors.
#   - setor_rural: 27 state-level shapefiles in SAD69 geographic (no .prj),
#     small scale (1:500k). ~58k sectors.
# Sum = 215,811 sectors = total official 2000 Census (via censobr tabular).
#
# This function also downloads the censobr tabular parquet (GitHub Release),
# which provides all the metadata (name_muni, name_district, name_subdistrict,
# name_neighborhood, situacao) that the raw shapefiles lack.
download_censustract_2000 <- function(year) {

  stopifnot(year == 2000)

  # --- URLs ---
  ftp_urb <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2000/setor_urbano/"
  ftp_rur <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2000/setor_rural/projecao_geografica/censo_2000/e500_arcview_shp/uf/"
  censobr_url <- "https://github.com/ipeaGIT/censobr/releases/download/v0.5.0/2000_tracts_Basico_v0.5.0.parquet"

  ufs <- c("ac","al","am","ap","ba","ce","df","es","go","ma","mg","ms","mt",
           "pa","pb","pe","pi","pr","rj","rn","ro","rr","rs","sc","se","sp","to")

  # --- Persistent dirs (survive across R sessions so targets can resume) ---
  tmp     <- "./data-raw/census_tract/2000"
  zip_urb <- paste0(tmp, "/zips_urbano")
  zip_rur <- paste0(tmp, "/zips_rural")
  shp_urb <- paste0(tmp, "/shps_urbano")
  shp_rur <- paste0(tmp, "/shps_rural")
  for (d in c(zip_urb, zip_rur, shp_urb, shp_rur)) {
    dir.create(d, recursive = TRUE, showWarnings = FALSE)
  }

  # --- 1. Censobr tabular parquet (single file, ~14 MB) ---
  censobr_path <- paste0(tmp, "/censobr_tracts_2000.parquet")
  if (!file.exists(censobr_path) || file.size(censobr_path) < 1e6) {
    message("[census_tract 2000] Baixando censobr tabular (~14 MB)...")
    tryCatch(
      download.file(censobr_url, censobr_path, mode = "wb", quiet = TRUE),
      error = function(e) stop("Censobr download falhou: ", conditionMessage(e))
    )
  } else {
    message("[census_tract 2000] Censobr tabular ja em cache, pulando.")
  }

  # --- 2. Rural (27 ZIPs, sequential + retry rounds) ---
  message("[census_tract 2000] === RURAL ===")
  rural_urls <- paste0(ftp_rur, ufs, "/", ufs, "_setores_censitarios.zip")
  rural_dest <- paste0(zip_rur, "/", ufs, "_setores_censitarios.zip")

  for (i in seq_along(ufs)) {
    if (file.exists(rural_dest[i]) && file.size(rural_dest[i]) > 1000) {
      next
    }
    message(sprintf("[census_tract 2000 rural] Baixando %s (%d/%d)...",
                    toupper(ufs[i]), i, length(ufs)))
    tryCatch(
      download.file(rural_urls[i], rural_dest[i], mode = "wb", quiet = TRUE),
      error = function(e) warning(paste("Rural download falhou:", ufs[i], ":",
                                        conditionMessage(e)))
    )
  }

  # Retry rounds (same pattern as 2010)
  for (round in 1:3) {
    ok <- file.exists(rural_dest) & file.size(rural_dest) > 1000
    if (all(ok)) break
    failed <- which(!ok)
    delay <- round * 3
    message(sprintf("[census_tract 2000 rural] Retry round %d: %d estados (delay %ds)...",
                    round, length(failed), delay))
    Sys.sleep(delay)
    for (i in failed) {
      tryCatch(
        download.file(rural_urls[i], rural_dest[i], mode = "wb", quiet = TRUE),
        error = function(e) warning(paste("Retry falhou:", ufs[i]))
      )
    }
  }

  ok <- file.exists(rural_dest) & file.size(rural_dest) > 1000
  if (!all(ok)) {
    stop(paste("Downloads rurais falharam para:",
               paste(ufs[!ok], collapse = ", ")))
  }
  message(sprintf("[census_tract 2000 rural] FINAL: %d/%d OK", sum(ok), length(ufs)))

  unzip_geobr(zip_dir = zip_rur, out_zip = shp_rur)

  # --- 3. Urban (1058 ZIPs, parallel via curl::multi_download) ---
  message("[census_tract 2000] === URBANO ===")
  message("[census_tract 2000 urbano] Listando municipios por UF...")

  # Known naming exceptions: some municipalities' ZIP file has a different
  # name than the standard {code_muni}.zip. Listed as a lookup of exceptions.
  zipname_exceptions <- c(
    "3300704" = "3300704_2000.zip"  # Sao Goncalo, RJ
  )

  urb_urls <- character()
  urb_names <- character()
  for (uf in ufs) {
    idx_url <- paste0(ftp_urb, uf, "/")
    munis <- tryCatch({
      html <- rvest::read_html(idx_url)
      hrefs <- rvest::html_attr(rvest::html_elements(html, "a"), "href")
      hrefs <- grep("^[0-9]{7}/?$", hrefs, value = TRUE)
      sub("/$", "", hrefs)
    }, error = function(e) {
      warning(sprintf("Falha ao listar UF %s: %s", uf, conditionMessage(e)))
      character()
    })
    if (length(munis) > 0) {
      zipnames <- ifelse(munis %in% names(zipname_exceptions),
                         unname(zipname_exceptions[munis]),
                         paste0(munis, ".zip"))
      urb_urls <- c(urb_urls,
                    paste0(ftp_urb, uf, "/", munis, "/", zipnames))
      # For local filename, always use {code_muni}.zip to keep things uniform
      urb_names <- c(urb_names, paste0(munis, ".zip"))
    }
  }
  message(sprintf("[census_tract 2000 urbano] Encontrados %d ZIPs urbanos",
                  length(urb_urls)))

  urb_dest <- paste0(zip_urb, "/", urb_names)

  # Parallel download with resume support
  message(sprintf("[census_tract 2000 urbano] Baixando em paralelo..."))
  curl::multi_download(urls      = urb_urls,
                       destfiles = urb_dest,
                       resume    = TRUE)

  # Retry any failures (up to 3 rounds)
  for (round in 1:3) {
    ok <- file.exists(urb_dest) & file.size(urb_dest) > 1000
    if (all(ok)) break
    failed <- which(!ok)
    message(sprintf("[census_tract 2000 urbano] Retry round %d: %d files...",
                    round, length(failed)))
    Sys.sleep(round * 2)
    curl::multi_download(urls      = urb_urls[failed],
                         destfiles = urb_dest[failed],
                         resume    = TRUE)
  }

  ok <- file.exists(urb_dest) & file.size(urb_dest) > 1000
  message(sprintf("[census_tract 2000 urbano] FINAL: %d/%d OK",
                  sum(ok), length(urb_dest)))
  if (sum(!ok) > 0) {
    warning(sprintf("[census_tract 2000 urbano] %d downloads falharam (continuando)",
                    sum(!ok)))
  }

  # Known filename quirk: RJ municipality 3300704 has a file named
  # 3300704_2000.zip inside its folder. Handled transparently by curl
  # (we request {code_muni}.zip directly, so quirk only matters for the
  # unzipped filename). See ainda_sem_targets/prep_census_tract.R:246-256.

  unzip_geobr(zip_dir = zip_urb, out_zip = shp_urb)

  # --- 4. Return paths (list, not sf) ---
  list(
    year         = year,
    shp_urb_dir  = shp_urb,
    shp_rur_dir  = shp_rur,
    censobr_path = censobr_path
  )
}


# Expand range notation "120033605000001-0003" -> c("...001","...002","...003")
expand_range <- function(geocodigo) {
  parts <- strsplit(geocodigo, "-")[[1]]
  if (length(parts) < 2) return(geocodigo)
  prefix <- parts[1]; suffix <- parts[2]
  nd <- nchar(suffix)
  base <- substr(prefix, 1, nchar(prefix) - nd)
  s <- as.integer(substr(prefix, nchar(prefix) - nd + 1, nchar(prefix)))
  e <- as.integer(suffix)
  sprintf(paste0(base, "%0", nd, "d"), s:e)
}

# ==============================================================================
# CompatMalhas helpers for census_tract 2000 range notation partitioning
# ==============================================================================

# Weight map for graph edge confidence (CompatMalhas metodo → numeric weight)
compat_weight_map <- c(
  "Manutenção"              = 1.0,
  "Manutenção desassociada" = 0.9,
  "Divisão"                 = 0.8,
  "Sobreposição (-50m)"     = 0.5,
  "Sobreposição (-20m)"     = 0.4,
  "Sobreposição (0m)"       = 0.3
)

# Prepare GeoPackages for CompatMalhas Python (1 per UF: 2000 + 2010)
prepare_compat_gpkgs <- function(rur_sf_all, urb_sf_all, tracts_2010, dir_compat) {
  dir.create(dir_compat, recursive = TRUE, showWarnings = FALSE)

  # Determine UFs from rural shapefiles
  uf_codes <- unique(substr(rur_sf_all$code_tract, 1, 2))
  uf_codes <- uf_codes[!is.na(uf_codes) & nchar(uf_codes) == 2]

  for (uf in uf_codes) {
    path_2000 <- file.path(dir_compat, paste0("setores_2000_", uf, ".gpkg"))
    path_2010 <- file.path(dir_compat, paste0("setores_2010_", uf, ".gpkg"))

    # Skip if already generated
    if (file.exists(path_2000) && file.exists(path_2010)) next

    # 2000: combine urban + rural for this UF
    rur_uf <- rur_sf_all[substr(rur_sf_all$code_tract, 1, 2) == uf, ]
    rur_uf$ID <- rur_uf$code_tract  # keeps range notation for ranges
    rur_out <- rur_uf[, c("ID", "geometry")]

    if (!is.null(urb_sf_all) && nrow(urb_sf_all) > 0) {
      urb_uf <- urb_sf_all[substr(urb_sf_all$code_tract, 1, 2) == uf, ]
      if (nrow(urb_uf) > 0) {
        urb_uf$ID <- urb_uf$code_tract
        urb_out <- urb_uf[, c("ID", "geometry")]
        all_2000 <- rbind(urb_out, rur_out)
      } else {
        all_2000 <- rur_out
      }
    } else {
      all_2000 <- rur_out
    }

    # 2010: extract tracts for this UF
    t2010_uf <- tracts_2010[substr(as.character(tracts_2010$code_tract), 1, 2) == uf, ]
    t2010_uf$ID <- as.character(t2010_uf$code_tract)
    t2010_out <- t2010_uf[, c("ID", "geometry")]

    sf::st_write(all_2000, path_2000, delete_dsn = TRUE, quiet = TRUE)
    sf::st_write(t2010_out, path_2010, delete_dsn = TRUE, quiet = TRUE)
    message(sprintf("[compat] UF %s: 2000=%d, 2010=%d features", uf,
                    nrow(all_2000), nrow(t2010_out)))
  }

  uf_codes
}

# Run CompatMalhas Python for 1 UF (with file-based cache)
run_compat_uf <- function(uf_code, dir_compat) {
  csv_path <- file.path(dir_compat, paste0("compat_", uf_code,
                         "_C2000_", uf_code, ".csv"))
  if (file.exists(csv_path)) {
    message(sprintf("[compat] UF %s: cached (skip Python)", uf_code))
    return(invisible(TRUE))
  }

  python_bin <- "C:/Users/antro/AppData/Local/Programs/Python/Python312/python.exe"
  script <- "scripts/compat_2000_2010.py"
  message(sprintf("[compat] UF %s: running CompatMalhas Python...", uf_code))
  exit <- system2(python_bin, args = c(script, uf_code),
                  stdout = "", stderr = "")
  if (exit != 0) {
    warning(paste("[compat] CompatMalhas failed for UF", uf_code))
    return(invisible(FALSE))
  }
  message(sprintf("[compat] UF %s: done", uf_code))
  invisible(TRUE)
}

# Read CompatMalhas output for 1 UF (CSVs + graph edges)
read_compat_uf <- function(uf_code, dir_compat) {
  c2000 <- read.delim(file.path(dir_compat, paste0("compat_", uf_code,
                       "_C2000_", uf_code, ".csv")))
  c2010 <- read.delim(file.path(dir_compat, paste0("compat_", uf_code,
                       "_C2010_", uf_code, ".csv")))
  gpkg <- file.path(dir_compat, paste0("compat_", uf_code, "_MCA.gpkg"))
  layers <- sf::st_layers(gpkg)
  edge_layer <- layers$name[grep("edges", layers$name)]
  edges <- sf::st_drop_geometry(sf::st_read(gpkg, layer = edge_layer, quiet = TRUE))
  list(c2000 = c2000, c2010 = c2010, edges = edges)
}

# Combined method: partition 1 range polygon using graph + code + weights
partition_range_combined <- function(range_poly, range_id, valid_codes,
                                      tracts_2010_uf, compat) {
  sf::sf_use_s2(FALSE)

  # ==================================================================
  # Code-first partition: start from 2000 codes (censobr), find 2010 geometry.
  # Returns exactly length(valid_codes) rows — one per censobr code.
  # ==================================================================

  # --- Phase 1: Build geometry pool (all 2010 fragments in range) ---

  # Collect graph edges for later identity assignment
  range_edges <- compat$edges[compat$edges$A.nome == range_id, ]
  indiv_edges <- compat$edges[compat$edges$A.nome %in% valid_codes &
                              !grepl("-", compat$edges$A.nome), ]

  # Phase 1: Build geometry pool — ALL 2010 tracts that spatially intersect
  # the range polygon. Pre-filter with st_intersects for performance, then clip.
  candidates_idx <- sf::st_intersects(sf::st_geometry(range_poly),
                                       sf::st_geometry(tracts_2010_uf))[[1]]
  if (length(candidates_idx) == 0) return(NULL)
  candidates <- tracts_2010_uf[candidates_idx, ]

  pool_clipped <- tryCatch({
    p <- sf::st_intersection(candidates, sf::st_geometry(range_poly))
    p <- sf::st_collection_extract(p, "POLYGON")
    sf::st_make_valid(p[!sf::st_is_empty(p), ])
  }, error = function(e) {
    warning(sprintf("[partition] st_intersection failed for %s: %s",
                    range_id, conditionMessage(e)))
    NULL
  })
  if (is.null(pool_clipped) || nrow(pool_clipped) == 0) return(NULL)
  pool_clipped$b_code <- as.character(pool_clipped$code_tract)

  # --- Phase 2: Assign each valid_code to a 2010 fragment ---

  assignments <- data.frame(valid_code = character(), b_code = character(),
                            method = character(), confidence = numeric(),
                            stringsAsFactors = FALSE)

  # Strategy A: direct code match (same code in 2010)
  for (vc in valid_codes) {
    if (vc %in% pool_clipped$b_code) {
      assignments <- rbind(assignments,
        data.frame(valid_code = vc, b_code = vc, method = "direct_match",
                   confidence = 1.0, stringsAsFactors = FALSE))
    }
  }

  # Strategy B: individual graph edges (A.nome = valid_code → B.nome in pool)
  remaining <- setdiff(valid_codes, assignments$valid_code)
  if (length(remaining) > 0 && nrow(indiv_edges) > 0) {
    indiv_edges$weight <- compat_weight_map[indiv_edges$metodo]
    indiv_edges$weight[is.na(indiv_edges$weight)] <- 0.1
    for (vc in remaining) {
      vc_edges <- indiv_edges[indiv_edges$A.nome == vc, ]
      available <- vc_edges[vc_edges$B.nome %in% pool_clipped$b_code, ]
      if (nrow(available) > 0) {
        best <- available[which.max(available$weight), ]
        assignments <- rbind(assignments,
          data.frame(valid_code = vc, b_code = best$B.nome,
                     method = paste0("graph_", tolower(gsub(" .*", "", best$metodo))),
                     confidence = best$weight, stringsAsFactors = FALSE))
      }
    }
  }

  # Strategy C: range edges + code proximity
  remaining <- setdiff(valid_codes, assignments$valid_code)
  if (length(remaining) > 0 && nrow(range_edges) > 0) {
    avail_b <- range_edges$B.nome[range_edges$B.nome %in% pool_clipped$b_code]
    if (length(avail_b) > 0) {
      nd <- nchar(strsplit(range_id, "-")[[1]][2])
      for (vc in remaining) {
        vc_num <- as.integer(substr(vc, nchar(vc) - nd + 1, nchar(vc)))
        b_nums <- as.integer(substr(avail_b, nchar(avail_b) - nd + 1, nchar(avail_b)))
        best_idx <- which.min(abs(b_nums - vc_num))
        assignments <- rbind(assignments,
          data.frame(valid_code = vc, b_code = avail_b[best_idx],
                     method = "range_proximity", confidence = 0.3,
                     stringsAsFactors = FALSE))
      }
    }
  }

  # Strategy D: unresolved (centroid seed for Voronoi)
  remaining <- setdiff(valid_codes, assignments$valid_code)
  for (vc in remaining) {
    assignments <- rbind(assignments,
      data.frame(valid_code = vc, b_code = NA_character_,
                 method = "unresolved", confidence = 0.0,
                 stringsAsFactors = FALSE))
  }

  # --- Phase 3: Build geometry from assignments ---

  result_list <- vector("list", nrow(assignments))
  for (i in seq_len(nrow(assignments))) {
    vc <- assignments$valid_code[i]
    bc <- assignments$b_code[i]
    if (is.na(bc)) {
      # Unresolved: centroid seed — Voronoi will assign area
      result_list[[i]] <- sf::st_sf(
        code_tract = vc, match_method = "unresolved", match_confidence = 0.0,
        geometry = sf::st_centroid(sf::st_geometry(range_poly)))
    } else {
      frag <- pool_clipped[pool_clipped$b_code == bc, ]
      result_list[[i]] <- sf::st_sf(
        code_tract = vc, match_method = assignments$method[i],
        match_confidence = assignments$confidence[i],
        geometry = sf::st_union(sf::st_geometry(frag)))
    }
  }

  result <- do.call(rbind, result_list)
  result[, c("code_tract", "match_method", "match_confidence", "geometry")]
}

# Resolve spatial overlaps in unified layer by priority cookie-cutter
# Priority: urbano(1) > rural(2) > compat_partitioned(3) > rural_aggregated(4)
# Per-municipality: for each priority level, clip against union of all higher levels.
# Uses row-by-row clipping to avoid length mismatches from st_collection_extract.
fix_spatial_overlaps <- function(uni_sf) {
  sf::sf_use_s2(FALSE)
  original_crs <- sf::st_crs(uni_sf)

  # Project to SIRGAS/Brazil Polyconic for reliable GEOS operations
  uni_sf <- sf::st_transform(uni_sf, 5880)
  uni_sf <- sf::st_make_valid(uni_sf)

  priority_map <- c("urbano" = 1L, "rural" = 2L,
                     "compat_partitioned" = 3L, "rural_aggregated" = 4L)
  uni_sf$.priority <- priority_map[uni_sf$geom_source]
  uni_sf$.priority[is.na(uni_sf$.priority)] <- 5L

  munis <- unique(uni_sf$code_muni)

  # Helper: clip one geometry by mask, with progressive fallback
  clip_one <- function(gi, mask, crs) {
    empty <- sf::st_sfc(sf::st_multipolygon(), crs = crs)
    # Attempt 1: direct
    res <- tryCatch(sf::st_difference(gi, mask), error = function(e) NULL)
    if (is.null(res)) {
      # Attempt 2: make_valid + snap (tiny negative buffer cleans slivers)
      gi2 <- sf::st_buffer(sf::st_make_valid(gi), -0.01)
      gi2 <- sf::st_buffer(gi2, 0.01)
      mask2 <- sf::st_buffer(sf::st_make_valid(mask), -0.01)
      mask2 <- sf::st_buffer(mask2, 0.01)
      res <- tryCatch(sf::st_difference(gi2, mask2), error = function(e) NULL)
    }
    if (is.null(res) || all(sf::st_is_empty(res))) return(empty)
    sf::st_cast(sf::st_union(sf::st_make_valid(res)), "MULTIPOLYGON")
  }

  result_list <- lapply(munis, function(cm) {
    sub <- uni_sf[uni_sf$code_muni == cm, ]
    if (nrow(sub) <= 1) return(sub)

    priorities <- sort(unique(sub$.priority))
    if (length(priorities) <= 1) return(sub)

    for (p in priorities[-1]) {
      higher_idx <- which(sub$.priority < p)
      if (length(higher_idx) == 0) next

      mask <- sf::st_make_valid(
        sf::st_union(sf::st_geometry(sub[higher_idx, ]))
      )

      this_idx <- which(sub$.priority == p)
      for (i in this_idx) {
        gi <- sf::st_geometry(sub[i, ])
        if (!sf::st_intersects(gi, mask, sparse = FALSE)[1, 1]) next
        sf::st_geometry(sub)[i] <- clip_one(gi, mask, sf::st_crs(sub))
      }
    }

    sub[!sf::st_is_empty(sub), ]
  })

  out <- do.call(rbind, result_list)
  out$.priority <- NULL
  out <- sf::st_make_valid(out)
  sf::st_transform(out, original_crs)
}

# ==============================================================================
# Helpers for census_tract 2000 (shared by urbano/rural/unified targets)
# ==============================================================================

# Progress logging: writes directly to file, bypassing callr+crew subprocess layers
log_progress_2000 <- function(msg) {
  log_file <- "./logs/census_tract_2000_progress.log"
  dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)
  cat(sprintf("[%s] %s\n", format(Sys.time(), "%H:%M:%S"), msg),
      file = log_file, append = TRUE)
}

# Read censobr tabular metadata (shared by urbano and rural)
read_censobr_2000 <- function(censobr_path) {
  arrow::read_parquet(censobr_path) |>
    dplyr::select(
      code_tract, code_muni, name_muni,
      code_district, name_district,
      code_subdistrict, name_subdistrict,
      code_neighborhood, name_neighborhood,
      situacao
    ) |>
    dplyr::mutate(code_tract = as.character(code_tract))
}

# Common cleaning for 2000: join censobr, harmonize, reorder columns
clean_one_2000 <- function(shp_sf, label, meta) {
  yyyy <- 2000

  message(sprintf("[census_tract 2000 %s] Join com censobr...", label))
  shp_sf <- dplyr::left_join(shp_sf, meta, by = "code_tract")

  # Fallback 1: derive code_muni from code_tract
  missing_muni_code <- is.na(shp_sf$code_muni) & !is.na(shp_sf$code_tract)
  n_missing_code <- sum(missing_muni_code)
  if (n_missing_code > 0) {
    shp_sf$code_muni[missing_muni_code] <- suppressWarnings(
      as.numeric(substr(shp_sf$code_tract[missing_muni_code], 1, 7))
    )
    message(sprintf("[census_tract 2000 %s] %d rows sem censobr: code_muni derivado do code_tract",
                    label, n_missing_code))
  }

  # Fallback 2: fill name_muni from municipality 2000 parquet
  missing_name <- is.na(shp_sf$name_muni) & !is.na(shp_sf$code_muni)
  n_missing_name <- sum(missing_name)
  if (n_missing_name > 0) {
    muni_path <- "./data/municipality/2000/municipalities_2000.parquet"
    if (file.exists(muni_path)) {
      muni_lookup <- read_geoparquet(muni_path) |>
        sf::st_drop_geometry() |>
        dplyr::select(code_muni, name_muni_fallback = name_muni) |>
        dplyr::distinct(code_muni, .keep_all = TRUE)
      shp_sf <- shp_sf |>
        dplyr::left_join(muni_lookup, by = "code_muni",
                         relationship = "many-to-one") |>
        dplyr::mutate(name_muni = dplyr::coalesce(name_muni, name_muni_fallback)) |>
        dplyr::select(-name_muni_fallback)
      n_filled <- n_missing_name - sum(is.na(shp_sf$name_muni))
      message(sprintf("[census_tract 2000 %s] name_muni fallback: %d/%d preenchidos",
                      label, n_filled, n_missing_name))
    }
  }

  n_na <- sum(is.na(shp_sf$name_muni))
  message(sprintf("[census_tract 2000 %s] name_muni final: %d/%d (%.3f%% NA)",
                  label, nrow(shp_sf) - n_na, nrow(shp_sf), 100 * n_na / nrow(shp_sf)))

  # Derive zone
  shp_sf <- shp_sf |>
    dplyr::mutate(
      zone = dplyr::case_when(
        situacao %in% c(1, 2, 3)        ~ "Urbana",
        situacao %in% c(4, 5, 6, 7, 8)  ~ "Rural",
        zone_src == "urbano_shp"         ~ "Urbana",
        zone_src == "rural_shp"          ~ "Rural",
        TRUE                             ~ NA_character_
      )
    )
  shp_sf$zone_src <- NULL
  shp_sf$situacao <- NULL
  shp_sf$year <- yyyy

  shp_sf <- harmonize_geobr(
    temp_sf = shp_sf, year = yyyy,
    add_state = TRUE, state_column = "code_muni",
    add_region = TRUE, region_column = "code_state",
    add_snake_case = FALSE, projection_fix = TRUE, encoding_utf8 = TRUE,
    topology_fix = TRUE, remove_z_dimension = TRUE,
    use_multipolygon = FALSE  # skip to_multipolygon dissolve (each tract is unique)
  )
  # Manual cast: extract polygons from any GEOMETRYCOLLECTION artifacts,
  # then cast to MULTIPOLYGON. This avoids the slow group_by/summarise path
  # in to_multipolygon() that triggers when mixed geometry types exist.
  shp_sf <- sf::st_collection_extract(shp_sf, "POLYGON")
  shp_sf <- sf::st_cast(shp_sf, "MULTIPOLYGON")

  shp_sf <- code_cols_to_numeric(shp_sf)

  shp_sf <- shp_sf |>
    dplyr::select(
      code_tract, code_muni, name_muni,
      code_neighborhood, name_neighborhood,
      code_district, name_district,
      code_subdistrict, name_subdistrict,
      zone,
      dplyr::any_of(c("geom_source", "original_id", "n_tracts_inside", "match_method", "match_confidence")),
      code_state, abbrev_state, name_state,
      code_region, name_region,
      year, geometry
    )

  stopifnot(!is.na(sf::st_crs(shp_sf)))
  stopifnot(sf::st_crs(shp_sf)$epsg == 4674)
  stopifnot(all(sf::st_geometry_type(shp_sf) == "MULTIPOLYGON"))
  stopifnot(names(shp_sf)[ncol(shp_sf)] == "geometry")

  dplyr::arrange(shp_sf, code_state, code_muni, code_tract)
}

# Write full + simplified pair of parquets
write_trio_2000 <- function(sf_obj, dir_clean, suffix) {
  path_full <- paste0(dir_clean, "/censustracts_2000", suffix, ".parquet")
  path_simp <- paste0(dir_clean, "/censustracts_2000", suffix, "_simplified.parquet")
  write_geobr_parquet(sf_obj, path_full)
  write_geobr_parquet(simplify_temp_sf(sf_obj, tolerance = 10), path_simp)
  c(path_full, path_simp)
}

# ==============================================================================
# Clean 2000 URBANO: read urban shapefiles, harmonize, write parquets
# ==============================================================================
clean_censustract_2000_urbano <- function(raw) {

  dir_clean <- "./data/census_tract/2000"
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  log_progress_2000("=== clean_censustract_2000_urbano() started ===")

  # Read censobr metadata
  log_progress_2000("urbano: Reading censobr tabular...")
  meta <- read_censobr_2000(raw$censobr_path)

  # Read urban shapefiles
  log_progress_2000("urbano: Reading urban shapefiles...")
  urb_files <- list.files(raw$shp_urb_dir, pattern = "\\.shp$",
                          full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
  log_progress_2000(sprintf("urbano: %d shapefiles found", length(urb_files)))

  urb_list <- purrr::map(urb_files, function(f) {
    prj_candidates <- c(sub("\\.shp$", ".prj", f, ignore.case = TRUE),
                        sub("\\.shp$", ".PRJ", f, ignore.case = TRUE))
    prj_file <- prj_candidates[file.exists(prj_candidates)][1]
    zone <- if (!is.na(prj_file)) detect_utm_zone_from_prj(prj_file) else NA_integer_
    x <- sf::st_read(f, quiet = TRUE, stringsAsFactors = FALSE, options = "ENCODING=IBM437")
    names(x) <- tolower(names(x))
    if (!is.na(zone)) {
      sf::st_crs(x) <- sf::st_crs(29170L + zone)
    }
    x <- sf::st_transform(x, 4674)
    id_col <- grep("^id_?$", names(x), value = TRUE)[1]
    if (is.na(id_col)) id_col <- "id_"
    x$code_tract <- as.character(x[[id_col]])
    x$zone_src <- "urbano_shp"
    x$geom_source <- "urbano"
    x$n_tracts_inside <- NA_real_
    x$match_method <- NA_character_
    x$match_confidence <- NA_real_
    x[, c("code_tract", "zone_src", "geom_source", "n_tracts_inside",
          "match_method", "match_confidence")]
  })
  urb_sf <- do.call(rbind, urb_list)
  if (attr(urb_sf, "sf_column") != "geometry") {
    names(urb_sf)[names(urb_sf) == attr(urb_sf, "sf_column")] <- "geometry"
    sf::st_geometry(urb_sf) <- "geometry"
  }
  log_progress_2000(sprintf("urbano: %d features combined", nrow(urb_sf)))

  # Clean
  log_progress_2000(sprintf("urbano: clean_one_2000 (%d features)...", nrow(urb_sf)))
  urb_clean <- clean_one_2000(urb_sf, "urbano", meta)
  log_progress_2000(sprintf("urbano: Done (%d features)", nrow(urb_clean)))

  # Write
  files <- write_trio_2000(urb_clean, dir_clean, "_urbano")
  log_progress_2000(sprintf("urbano: %d parquets written", length(files)))
  return(files)
}

# ==============================================================================
# Clean 2000 RURAL: read rural shapefiles, harmonize (faithful to IBGE source)
# ==============================================================================
# Keeps range notation as-is (no CompatMalhas splitting here).
# Ranges are collapsed to first code, with geom_source = "rural_aggregated".
# The splitting/partitioning logic lives in clean_censustract_2000_unified().
clean_censustract_2000_rural <- function(raw) {

  dir_clean <- "./data/census_tract/2000"
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  log_progress_2000("=== clean_censustract_2000_rural() started ===")

  # Read censobr metadata
  log_progress_2000("rural: Reading censobr tabular...")
  meta <- read_censobr_2000(raw$censobr_path)

  # Read rural shapefiles
  log_progress_2000("rural: Reading rural shapefiles...")
  rur_files <- list.files(raw$shp_rur_dir, pattern = "\\.shp$",
                          full.names = TRUE, recursive = TRUE, ignore.case = TRUE)

  rur_raw_list <- purrr::map(rur_files, function(f) {
    x <- sf::st_read(f, quiet = TRUE, stringsAsFactors = FALSE, options = "ENCODING=IBM437")
    names(x) <- tolower(names(x))
    if (is.na(sf::st_crs(x))) sf::st_crs(x) <- sf::st_crs(4618)
    x <- sf::st_transform(x, 4674)
    x <- sf::st_make_valid(x)
    geo_col <- grep("^geocodigo$", names(x), value = TRUE)[1]
    if (is.na(geo_col)) geo_col <- "geocodigo"
    x$code_tract <- as.character(x[[geo_col]])
    x$zone_src <- "rural_shp"
    x[, c("code_tract", "zone_src", "geometry")]
  })
  rur_raw_sf <- do.call(rbind, rur_raw_list)
  if (attr(rur_raw_sf, "sf_column") != "geometry") {
    names(rur_raw_sf)[names(rur_raw_sf) == attr(rur_raw_sf, "sf_column")] <- "geometry"
    sf::st_geometry(rur_raw_sf) <- "geometry"
  }
  n_ranges <- sum(grepl("-", rur_raw_sf$code_tract))
  log_progress_2000(sprintf("rural: %d features (%d with range notation)", nrow(rur_raw_sf), n_ranges))

  # Tag range vs individual and preserve original range notation
  is_range <- grepl("-", rur_raw_sf$code_tract)
  rur_raw_sf$geom_source <- ifelse(is_range, "rural_aggregated", "rural")
  rur_raw_sf$original_id <- ifelse(is_range, rur_raw_sf$code_tract, NA_character_)
  rur_raw_sf$n_tracts_inside <- ifelse(
    is_range,
    sapply(rur_raw_sf$code_tract, function(gc) length(expand_range(gc))),
    NA_real_
  )
  # Collapse range to first code (e.g. "120020305000001-0003" -> "120020305000001")
  rur_raw_sf$code_tract <- sub("-.*$", "", rur_raw_sf$code_tract)
  rur_raw_sf$match_method <- NA_character_
  rur_raw_sf$match_confidence <- NA_real_

  # Drop duplicates from range collapse
  dup_idx <- which(duplicated(rur_raw_sf$code_tract))
  if (length(dup_idx) > 0) rur_raw_sf <- rur_raw_sf[-dup_idx, ]

  log_progress_2000(sprintf("rural: clean_one_2000 (%d features)...", nrow(rur_raw_sf)))
  rur_clean <- clean_one_2000(rur_raw_sf, "rural", meta)
  log_progress_2000(sprintf("rural: Done (%d features)", nrow(rur_clean)))

  files <- write_trio_2000(rur_clean, dir_clean, "_rural")
  log_progress_2000(sprintf("rural: %d parquets written", length(files)))
  return(files)
}

# Voronoi gap filling: inflate partitions to fill gaps within range polygon
# Each partition "inflates" equidistantly until meeting its neighbor.
# With 1 partition: the entire range polygon becomes the geometry.
fill_gaps_voronoi <- function(partitions, range_polygon) {
  sf::sf_use_s2(FALSE)

  # Edge case: single partition → just use the range polygon as geometry

  if (nrow(partitions) == 1) {
    result <- partitions
    sf::st_geometry(result) <- sf::st_geometry(range_polygon)
    result <- sf::st_make_valid(result)
    return(sf::st_cast(result, "MULTIPOLYGON"))
  }

  tryCatch({
    centroids <- sf::st_centroid(sf::st_geometry(partitions))
    pts_union <- sf::st_union(centroids)
    voronoi_raw <- sf::st_voronoi(pts_union, envelope = sf::st_geometry(range_polygon))
    voronoi_polys <- sf::st_collection_extract(voronoi_raw, "POLYGON")
    voronoi_clipped <- sf::st_intersection(voronoi_polys, sf::st_geometry(range_polygon))
    voronoi_clipped <- sf::st_collection_extract(voronoi_clipped, "POLYGON")
    # Match each Voronoi cell to the nearest partition centroid
    nearest_idx <- sf::st_nearest_feature(sf::st_centroid(voronoi_clipped), centroids)
    # Build result: each cell inherits attributes from matching partition
    result <- partitions[nearest_idx, ]
    sf::st_geometry(result) <- voronoi_clipped
    # Dissolve cells with same code_tract (multiple Voronoi cells can map to same partition)
    result <- result |>
      dplyr::group_by(code_tract, match_method, match_confidence) |>
      dplyr::summarise(geometry = sf::st_union(geometry), .groups = "drop")
    result <- sf::st_make_valid(result)
    sf::st_cast(result, "MULTIPOLYGON")
  }, error = function(e) {
    warning(sprintf("[voronoi] Failed: %s. Returning original partitions.", conditionMessage(e)))
    partitions
  })
}

# ==============================================================================
# Clean 2000 UNIFIED: combine urbano + rural, split ranges using 2010 + compat
# ==============================================================================
# Steps:
#   1. Read urbano + rural (with original_id preserved)
#   2. Partition rural_aggregated ranges using CompatMalhas + 2010 reference
#   3. Voronoi gap filling for each partitioned range
#   4. Merge everything (urbano + rural individual + partitions + remaining aggregated)
#   5. Overlap fix by UF (cookie-cutter with priority)
#   6. Write parquets
clean_censustract_2000_unified <- function(path_urbano, path_rural, path_2010, raw_2000 = NULL) {

  dir_clean <- "./data/census_tract/2000"
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  log_progress_2000("=== clean_censustract_2000_unified() started ===")

  # --- 1. Read inputs ---
  urb_full <- path_urbano[!grepl("_simplified", path_urbano)]
  rur_full <- path_rural[!grepl("_simplified", path_rural)]
  path_full_2010 <- path_2010[!grepl("_simplified", path_2010)]

  log_progress_2000("unified: Reading parquets...")
  urb_clean <- read_geoparquet(urb_full)
  rur_clean <- read_geoparquet(rur_full)
  tracts_2010 <- read_geoparquet(path_full_2010)
  tracts_2010$code_tract <- as.character(tracts_2010$code_tract)

  # Separate rural into individuals and ranges
  rur_indiv <- dplyr::filter(rur_clean, geom_source == "rural")
  rur_agg <- dplyr::filter(rur_clean, geom_source == "rural_aggregated")
  n_agg <- nrow(rur_agg)
  log_progress_2000(sprintf("unified: %d rural individual, %d rural_aggregated (to partition)",
                            nrow(rur_indiv), n_agg))

  # --- 2. Partition ranges using CompatMalhas ---
  if (n_agg > 0 && !is.null(tracts_2010)) {
    sf::sf_use_s2(FALSE)

    # Load censobr as ground truth: which code_tracts actually exist in 2000
    censobr_codes <- NULL
    if (!is.null(raw_2000) && !is.null(raw_2000$censobr_path)) {
      censobr_codes <- tryCatch({
        ct <- arrow::read_parquet(raw_2000$censobr_path,
                                  col_select = "code_tract", as_data_frame = TRUE)
        codes_chr <- as.character(ct$code_tract) |> unique()
        log_progress_2000(sprintf("unified: censobr ground truth loaded (%d unique code_tracts)",
                                  length(codes_chr)))
        codes_chr
      }, error = function(e) {
        log_progress_2000(sprintf("unified: WARNING — censobr failed: %s", conditionMessage(e)))
        NULL
      })
    } else {
      log_progress_2000("unified: WARNING — raw_2000 not provided, no censobr ground truth")
    }

    # Prepare GPKGs for CompatMalhas (uses clean parquets as input)
    dir_compat <- "./data-raw/census_tract/compat"
    dir.create(dir_compat, recursive = TRUE, showWarnings = FALSE)

    # Build 2000 sf with range notation IDs for GPKG generation
    rur_for_gpkg <- rur_clean
    rur_for_gpkg$ID <- ifelse(!is.na(rur_for_gpkg$original_id),
                               rur_for_gpkg$original_id,
                               as.character(rur_for_gpkg$code_tract))
    urb_for_gpkg <- urb_clean
    urb_for_gpkg$ID <- as.character(urb_for_gpkg$code_tract)

    log_progress_2000("unified: Preparing GPKGs for CompatMalhas...")
    uf_codes <- unique(substr(as.character(rur_agg$code_tract), 1, 2))
    for (uf in uf_codes) {
      path_2000_gpkg <- file.path(dir_compat, paste0("setores_2000_", uf, ".gpkg"))
      path_2010_gpkg <- file.path(dir_compat, paste0("setores_2010_", uf, ".gpkg"))
      if (file.exists(path_2000_gpkg) && file.exists(path_2010_gpkg)) next

      all_2000_uf <- rbind(
        urb_for_gpkg[substr(as.character(urb_for_gpkg$code_tract), 1, 2) == uf, c("ID", "geometry")],
        rur_for_gpkg[substr(as.character(rur_for_gpkg$code_tract), 1, 2) == uf, c("ID", "geometry")]
      )
      t2010_uf <- tracts_2010[substr(as.character(tracts_2010$code_tract), 1, 2) == uf, ]
      t2010_uf$ID <- as.character(t2010_uf$code_tract)
      sf::st_write(all_2000_uf, path_2000_gpkg, delete_dsn = TRUE, quiet = TRUE)
      sf::st_write(t2010_uf[, c("ID", "geometry")], path_2010_gpkg, delete_dsn = TRUE, quiet = TRUE)
    }

    # Run CompatMalhas Python for each UF (with cache)
    log_progress_2000("unified: Running CompatMalhas per UF...")
    for (uf in uf_codes) {
      log_progress_2000(sprintf("unified: CompatMalhas UF %s...", uf))
      run_compat_uf(uf, dir_compat)
    }
    n_compat_ok <- sum(file.exists(file.path(dir_compat,
      paste0("compat_", uf_codes, "_C2000_", uf_codes, ".csv"))))
    log_progress_2000(sprintf("unified: CompatMalhas done. %d/%d UFs with data",
                              n_compat_ok, length(uf_codes)))

    # Partition each range
    log_progress_2000(sprintf("unified: Partitioning %d ranges...", n_agg))
    compat_cache <- list()
    partitioned_list <- list()
    still_aggregated <- list()

    for (i in seq_len(n_agg)) {
      if (i %% 500 == 0 || i == n_agg) {
        log_progress_2000(sprintf("unified: range %d/%d", i, n_agg))
      }
      row <- rur_agg[i, ]
      range_code <- row$original_id
      if (is.na(range_code)) {
        still_aggregated[[length(still_aggregated) + 1]] <- row
        next
      }

      codes <- expand_range(range_code)
      # Filter by censobr ground truth: only codes that actually exist in 2000
      if (!is.null(censobr_codes)) {
        codes <- codes[codes %in% censobr_codes]
        if (length(codes) == 0) {
          still_aggregated[[length(still_aggregated) + 1]] <- row
          next
        }
      }
      uf <- substr(range_code, 1, 2)
      csv_file <- file.path(dir_compat, paste0("compat_", uf, "_C2000_", uf, ".csv"))

      if (!file.exists(csv_file)) {
        still_aggregated[[length(still_aggregated) + 1]] <- row
        next
      }

      if (is.null(compat_cache[[uf]])) {
        compat_cache[[uf]] <- read_compat_uf(uf, dir_compat)
      }

      t2010_uf <- tracts_2010[substr(as.character(tracts_2010$code_tract), 1, 2) == uf, ]
      parts <- partition_range_combined(
        range_poly = row, range_id = range_code,
        valid_codes = codes, tracts_2010_uf = t2010_uf,
        compat = compat_cache[[uf]]
      )

      if (!is.null(parts) && nrow(parts) > 0) {
        # Voronoi gap filling: inflate all partitions to cover entire range
        # (new partition_range_combined returns 1 row per valid_code, no NAs)
        parts_filled <- fill_gaps_voronoi(parts, row)

        # Copy metadata from the original range row
        for (col in c("code_muni", "name_muni", "code_state", "abbrev_state",
                       "name_state", "code_region", "name_region", "year",
                       "code_neighborhood", "name_neighborhood",
                       "code_district", "name_district",
                       "code_subdistrict", "name_subdistrict", "zone")) {
          if (col %in% names(row)) {
            parts_filled[[col]] <- as.vector(row[[col]])
          }
        }
        parts_filled$geom_source <- "compat_partitioned"
        parts_filled$original_id <- range_code
        parts_filled$n_tracts_inside <- NA_real_
        parts_filled$code_tract <- as.numeric(parts_filled$code_tract)

        partitioned_list[[length(partitioned_list) + 1]] <- parts_filled
      } else {
        still_aggregated[[length(still_aggregated) + 1]] <- row
      }
    }

    n_partitioned <- sum(sapply(partitioned_list, nrow))
    n_still_agg <- length(still_aggregated)
    log_progress_2000(sprintf("unified: %d partitioned features, %d still aggregated",
                              n_partitioned, n_still_agg))

    # Combine partitioned + still aggregated
    if (length(partitioned_list) > 0) {
      partitioned_sf <- dplyr::bind_rows(partitioned_list)

      # Rural individual has priority over compat — if a code_tract
      # exists as rural (original IBGE geometry), discard the compat version
      n_before <- nrow(partitioned_sf)
      partitioned_sf <- partitioned_sf |>
        dplyr::filter(!code_tract %in% rur_indiv$code_tract)
      log_progress_2000(sprintf(
        "unified: post-partition cleanup: %d -> %d features",
        n_before, nrow(partitioned_sf)))
    } else {
      partitioned_sf <- rur_agg[0, ]
    }
    if (length(still_aggregated) > 0) {
      still_agg_sf <- dplyr::bind_rows(still_aggregated)
    } else {
      still_agg_sf <- rur_agg[0, ]
    }

    rur_processed <- dplyr::bind_rows(rur_indiv, partitioned_sf, still_agg_sf)
  } else {
    rur_processed <- rur_clean
  }

  # --- 3. Merge urbano + rural (urban priority for duplicate code_tract) ---
  log_progress_2000("unified: Merging (urban > rural for overlaps)...")
  rur_only <- dplyr::filter(rur_processed, !code_tract %in% urb_clean$code_tract)
  n_overlap <- nrow(rur_processed) - nrow(rur_only)
  uni_clean <- dplyr::bind_rows(urb_clean, rur_only) |>
    dplyr::arrange(code_state, code_muni, code_tract)

  n_dup <- sum(duplicated(uni_clean$code_tract))
  if (n_dup > 0) {
    warning(sprintf("[census_tract 2000] %d code_tract duplicados no unified", n_dup))
  }
  log_progress_2000(sprintf("unified: %d features (urb=%d, rur=%d, overlap=%d)",
                            nrow(uni_clean), nrow(urb_clean), nrow(rur_only), n_overlap))
  log_progress_2000(sprintf("unified: geom_source: %s",
                            paste(names(table(uni_clean$geom_source)),
                                  table(uni_clean$geom_source), sep="=", collapse=", ")))

  # --- 4. Overlap fix by UF (cookie-cutter with priority) ---
  # TODO: implement per-UF overlap fix (fix_spatial_overlaps redesigned)
  # For now, overlaps remain in the data.

  # --- 5. Write ---
  log_progress_2000(sprintf("unified: Writing parquets (%d features)...", nrow(uni_clean)))
  files <- write_trio_2000(uni_clean, dir_clean, "")
  log_progress_2000(sprintf("=== DONE: %d parquets written ===", length(files)))
  return(files)
}

# ==============================================================================
# Clean 2000 (legacy monolith — DEPRECATED, kept for reference)
# Use clean_censustract_2000_urbano/rural/unified instead.
# ==============================================================================
clean_censustract_2000 <- function(raw) {

  yyyy <- 2000
  dir_clean <- paste0("./data/census_tract/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  log_progress <- log_progress_2000  # alias for backward compat
  log_progress("=== clean_censustract_2000() started (LEGACY) ===")

  # --- 0. Load 2010 tracts as spatial reference for partitioning ranges ---
  log_progress("Step 0: Loading 2010 tracts as spatial reference...")
  path_2010 <- "./data/census_tract/2010/censustracts_2010.parquet"
  if (file.exists(path_2010)) {
    message("[census_tract 2000] Carregando setores 2010 como referencia espacial...")
    tracts_2010 <- read_geoparquet(path_2010)
    tracts_2010$code_tract <- as.character(tracts_2010$code_tract)
    message(sprintf("[census_tract 2000] 2010: %d setores carregados", nrow(tracts_2010)))
  } else {
    warning("[census_tract 2000] Parquet 2010 nao encontrado; ranges serao mantidas agregadas")
    tracts_2010 <- NULL
  }

  # --- 1. Read censobr tabular (source of name_muni, name_district, etc.) ---
  log_progress("Step 1: Reading censobr tabular...")
  message("[census_tract 2000] Lendo censobr tabular...")
  meta <- arrow::read_parquet(raw$censobr_path) |>
    dplyr::select(
      code_tract, code_muni, name_muni,
      code_district, name_district,
      code_subdistrict, name_subdistrict,
      code_neighborhood, name_neighborhood,
      situacao
    ) |>
    dplyr::mutate(code_tract = as.character(code_tract))
  message(sprintf("[census_tract 2000] Censobr: %d setores tabulares", nrow(meta)))

  # --- 2. Read urban shapefiles (UTM SAD69 per UF -> EPSG:4674) ---
  log_progress("Step 2: Reading urban shapefiles...")
  message("[census_tract 2000] Lendo shapefiles urbanos...")
  urb_files <- list.files(raw$shp_urb_dir, pattern = "\\.shp$",
                          full.names = TRUE, recursive = TRUE,
                          ignore.case = TRUE)
  message(sprintf("[census_tract 2000] %d shapefiles urbanos encontrados",
                  length(urb_files)))

  urb_list <- purrr::map(urb_files, function(f) {
    # Try multiple .prj extension cases (shapefiles come with .PRJ on Windows)
    prj_candidates <- c(sub("\\.shp$", ".prj", f, ignore.case = TRUE),
                        sub("\\.shp$", ".PRJ", f, ignore.case = TRUE))
    prj_file <- prj_candidates[file.exists(prj_candidates)][1]
    zone <- if (!is.na(prj_file)) detect_utm_zone_from_prj(prj_file) else NA_integer_

    x <- sf::st_read(f, quiet = TRUE, stringsAsFactors = FALSE,
                     options = "ENCODING=IBM437")
    names(x) <- tolower(names(x))

    # Force-assign SAD69 UTM south zone CRS (overwrites whatever sf parsed)
    if (!is.na(zone)) {
      epsg_sad69 <- 29170L + zone
      sf::st_crs(x) <- sf::st_crs(epsg_sad69)
    } else {
      warning(sprintf("[census_tract 2000 urbano] .prj sem zona UTM: %s",
                      basename(f)))
    }

    x <- sf::st_transform(x, 4674)

    # Find the ID column (shapefile uses "ID_" -> tolower gives "id_")
    id_col <- grep("^id_?$", names(x), value = TRUE)[1]
    if (is.na(id_col)) id_col <- "id_"
    x$code_tract <- as.character(x[[id_col]])
    x$zone_src <- "urbano_shp"
    x$geom_source <- "urbano"
    x$n_tracts_inside <- NA_real_
    x$match_method <- NA_character_
    x$match_confidence <- NA_real_

    x[, c("code_tract", "zone_src", "geom_source", "n_tracts_inside",
          "match_method", "match_confidence")]
  })
  urb_sf <- do.call(rbind, urb_list)
  # Standardize geometry column name
  if (attr(urb_sf, "sf_column") != "geometry") {
    names(urb_sf)[names(urb_sf) == attr(urb_sf, "sf_column")] <- "geometry"
    sf::st_geometry(urb_sf) <- "geometry"
  }
  log_progress(sprintf("Step 2: Done. %d urban features", nrow(urb_sf)))
  message(sprintf("[census_tract 2000] Urbano combinado: %d features",
                  nrow(urb_sf)))

  # --- 3. Read rural shapefiles (SAD69 geographic -> EPSG:4674) ---
  log_progress("Step 3: Reading rural shapefiles...")
  message("[census_tract 2000] Lendo shapefiles rurais...")
  rur_files <- list.files(raw$shp_rur_dir, pattern = "\\.shp$",
                          full.names = TRUE, recursive = TRUE,
                          ignore.case = TRUE)
  message(sprintf("[census_tract 2000] %d shapefiles rurais encontrados",
                  length(rur_files)))

  # --- 3b. Prepare and run CompatMalhas for all UFs ---
  # Read rural shapefiles first (keeping original geocodigo with range notation)
  rur_raw_list <- purrr::map(rur_files, function(f) {
    x <- sf::st_read(f, quiet = TRUE, stringsAsFactors = FALSE,
                     options = "ENCODING=IBM437")
    names(x) <- tolower(names(x))
    if (is.na(sf::st_crs(x))) sf::st_crs(x) <- sf::st_crs(4618)
    x <- sf::st_transform(x, 4674)
    x <- sf::st_make_valid(x)
    geo_col <- grep("^geocodigo$", names(x), value = TRUE)[1]
    if (is.na(geo_col)) geo_col <- "geocodigo"
    x$code_tract <- as.character(x[[geo_col]])  # preserves range notation
    x$zone_src <- "rural_shp"
    x[, c("code_tract", "zone_src", "geometry")]
  })
  rur_raw_sf <- do.call(rbind, rur_raw_list)
  if (attr(rur_raw_sf, "sf_column") != "geometry") {
    names(rur_raw_sf)[names(rur_raw_sf) == attr(rur_raw_sf, "sf_column")] <- "geometry"
    sf::st_geometry(rur_raw_sf) <- "geometry"
  }
  message(sprintf("[census_tract 2000] Rural raw: %d features (%d with range)",
                  nrow(rur_raw_sf), sum(grepl("-", rur_raw_sf$code_tract))))

  # Prepare GPKGs and run Python CompatMalhas per UF
  log_progress(sprintf("Step 3b: Preparing GPKGs + running CompatMalhas for %d UFs...",
                       length(unique(substr(rur_raw_sf$code_tract, 1, 2)))))
  dir_compat <- "./data-raw/census_tract/compat"
  uf_codes_list <- prepare_compat_gpkgs(rur_raw_sf, urb_sf, tracts_2010, dir_compat)
  for (uf in uf_codes_list) {
    log_progress(sprintf("Step 3b: CompatMalhas UF %s...", uf))
    run_compat_uf(uf, dir_compat)
  }
  n_compat_ok <- sum(file.exists(file.path(dir_compat,
    paste0("compat_", uf_codes_list, "_C2000_", uf_codes_list, ".csv"))))
  log_progress(sprintf("Step 3b: Done. %d/%d UFs with compat data",
                       n_compat_ok, length(uf_codes_list)))

  # --- 3c. Process each rural polygon with combined method ---
  log_progress(sprintf("Step 3c: Processing %d rural polygons with combined method...",
                       nrow(rur_raw_sf)))
  # Cache compat data per UF to avoid re-reading
  compat_cache <- list()

  n_rur_total <- nrow(rur_raw_sf)
  rur_list <- lapply(seq_len(n_rur_total), function(i) {
    if (i %% 1000 == 0 || i == n_rur_total) {
      log_progress(sprintf("Step 3c: rural polygon %d/%d", i, n_rur_total))
    }
    gc <- rur_raw_sf$code_tract[i]
    codes <- expand_range(gc)

    if (length(codes) == 1 && !grepl("-", gc)) {
      # Individual tract (no range)
      row <- rur_raw_sf[i, c("code_tract", "zone_src", "geometry")]
      row$geom_source <- "rural"
      row$n_tracts_inside <- NA_real_
      row$match_method <- NA_character_
      row$match_confidence <- NA_real_
      return(row)
    }

    # Range notation: use combined method (graph + code + weights)
    uf <- substr(gc, 1, 2)
    csv_file <- file.path(dir_compat, paste0("compat_", uf, "_C2000_", uf, ".csv"))

    if (!file.exists(csv_file) || is.null(tracts_2010)) {
      # Fallback: keep aggregated
      row <- rur_raw_sf[i, c("zone_src", "geometry")]
      row$code_tract <- codes[1]
      row$geom_source <- "rural_aggregated"
      row$n_tracts_inside <- length(codes)
      row$match_method <- NA_character_
      row$match_confidence <- NA_real_
      return(row)
    }

    # Load compat data (cached per UF)
    if (is.null(compat_cache[[uf]])) {
      compat_cache[[uf]] <<- read_compat_uf(uf, dir_compat)
    }

    # Filter 2010 tracts for this UF
    t2010_uf <- tracts_2010[substr(as.character(tracts_2010$code_tract), 1, 2) == uf, ]

    parts <- partition_range_combined(
      range_poly = rur_raw_sf[i, ],
      range_id = gc,
      expanded_codes = codes,
      tracts_2010_uf = t2010_uf,
      compat = compat_cache[[uf]]
    )

    if (!is.null(parts) && nrow(parts) > 0) {
      parts$zone_src <- "rural_shp"
      parts$geom_source <- "compat_partitioned"
      parts$n_tracts_inside <- NA_real_
      parts[, c("code_tract", "zone_src", "geom_source", "n_tracts_inside",
                "match_method", "match_confidence", "geometry")]
    } else {
      # Fallback: keep aggregated
      row <- rur_raw_sf[i, c("zone_src", "geometry")]
      row$code_tract <- codes[1]
      row$geom_source <- "rural_aggregated"
      row$n_tracts_inside <- length(codes)
      row$match_method <- NA_character_
      row$match_confidence <- NA_real_
      row
    }
  })

  rur_sf <- do.call(rbind, rur_list)
  if (attr(rur_sf, "sf_column") != "geometry") {
    names(rur_sf)[names(rur_sf) == attr(rur_sf, "sf_column")] <- "geometry"
    sf::st_geometry(rur_sf) <- "geometry"
  }
  # Drop exact code_tract duplicates (prefer compat_partitioned over rural)
  dup_idx <- which(duplicated(rur_sf$code_tract))
  if (length(dup_idx) > 0) {
    message(sprintf("[census_tract 2000] Rural: removendo %d duplicatas",
                    length(dup_idx)))
    rur_sf <- rur_sf[-dup_idx, ]
  }
  message(sprintf("[census_tract 2000] Rural combinado: %d features",
                  nrow(rur_sf)))

  # --- 4. Common cleaning: join with censobr, harmonize, reorder cols ---
  clean_one <- function(shp_sf, label) {

    message(sprintf("[census_tract 2000 %s] Join com censobr...", label))

    # LEFT JOIN with censobr tabular to get all metadata
    shp_sf <- dplyr::left_join(shp_sf, meta, by = "code_tract")

    # Fallback 1: if censobr missed some rows, derive code_muni from code_tract
    # so harmonize_geobr can still add state/region info via code_muni.
    missing_muni_code <- is.na(shp_sf$code_muni) & !is.na(shp_sf$code_tract)
    n_missing_code <- sum(missing_muni_code)
    if (n_missing_code > 0) {
      shp_sf$code_muni[missing_muni_code] <- suppressWarnings(
        as.numeric(substr(shp_sf$code_tract[missing_muni_code], 1, 7))
      )
      message(sprintf("[census_tract 2000 %s] %d rows sem censobr: code_muni derivado do code_tract",
                      label, n_missing_code))
    }

    # Fallback 2: if name_muni is still NA, fill from municipality 2000 parquet
    # (local file produced by the municipality target). This covers the
    # ~44 cartographic setores that exist in the 1:500k rural mesh (published
    # 2002) but are not in the tabular Censo 2000 published by censobr.
    # IMPORTANT: municipality 2000 parquet has 1 duplicated code_muni
    # (3509908 Cananeia/Cananéia). We deduplicate the lookup to avoid
    # many-to-many joins that would inflate the row count.
    missing_name <- is.na(shp_sf$name_muni) & !is.na(shp_sf$code_muni)
    n_missing_name <- sum(missing_name)
    if (n_missing_name > 0) {
      muni_path <- "./data/municipality/2000/municipalities_2000.parquet"
      if (file.exists(muni_path)) {
        muni_lookup <- read_geoparquet(muni_path) |>
          sf::st_drop_geometry() |>
          dplyr::select(code_muni, name_muni_fallback = name_muni) |>
          dplyr::distinct(code_muni, .keep_all = TRUE)
        shp_sf <- shp_sf |>
          dplyr::left_join(muni_lookup, by = "code_muni",
                           relationship = "many-to-one") |>
          dplyr::mutate(
            name_muni = dplyr::coalesce(name_muni, name_muni_fallback)
          ) |>
          dplyr::select(-name_muni_fallback)
        n_filled <- n_missing_name - sum(is.na(shp_sf$name_muni))
        message(sprintf("[census_tract 2000 %s] name_muni fallback via municipality 2000: %d/%d preenchidos",
                        label, n_filled, n_missing_name))
      } else {
        message(sprintf("[census_tract 2000 %s] municipality 2000 parquet nao encontrado, pulando fallback",
                        label))
      }
    }

    # Report final name_muni match rate
    n_na <- sum(is.na(shp_sf$name_muni))
    pct_na <- n_na / nrow(shp_sf)
    message(sprintf("[census_tract 2000 %s] name_muni final: %d/%d (%.3f%% NA)",
                    label, nrow(shp_sf) - n_na, nrow(shp_sf), 100 * pct_na))
    if (pct_na > 0.05) {
      warning(sprintf("[census_tract 2000 %s] %.3f%% dos setores ainda sem name_muni",
                      label, 100 * pct_na))
    }

    # Derive zone from situacao (censobr) with fallback to shapefile source
    shp_sf <- shp_sf |>
      dplyr::mutate(
        zone = dplyr::case_when(
          situacao %in% c(1, 2, 3)             ~ "Urbana",
          situacao %in% c(4, 5, 6, 7, 8)        ~ "Rural",
          zone_src == "urbano_shp"              ~ "Urbana",
          zone_src == "rural_shp"               ~ "Rural",
          TRUE                                  ~ NA_character_
        )
      )

    # Drop helper columns
    shp_sf$zone_src <- NULL
    shp_sf$situacao <- NULL
    shp_sf$year <- yyyy

    # Apply harmonize_geobr (adds state/region/year, fixes topology + MULTIPOLYGON)
    shp_sf <- harmonize_geobr(
      temp_sf        = shp_sf,
      year           = yyyy,
      add_state      = TRUE,
      state_column   = "code_muni",
      add_region     = TRUE,
      region_column  = "code_state",
      add_snake_case = FALSE,
      projection_fix = TRUE,
      encoding_utf8  = TRUE,
      topology_fix   = TRUE,
      remove_z_dimension = TRUE,
      use_multipolygon   = TRUE
    )

    # Force code_* columns to numeric
    shp_sf <- code_cols_to_numeric(shp_sf)

    # Column order (2000-specific: includes geom_source, n_tracts_inside,
    # match_method, match_confidence)
    shp_sf <- shp_sf |>
      dplyr::select(
        code_tract,
        code_muni, name_muni,
        code_neighborhood, name_neighborhood,
        code_district, name_district,
        code_subdistrict, name_subdistrict,
        zone, geom_source, n_tracts_inside,
        dplyr::any_of(c("match_method", "match_confidence")),
        code_state, abbrev_state, name_state,
        code_region, name_region,
        year,
        geometry
      )

    # Validate
    stopifnot(!is.na(sf::st_crs(shp_sf)))
    stopifnot(sf::st_crs(shp_sf)$epsg == 4674)
    stopifnot(all(sf::st_geometry_type(shp_sf) == "MULTIPOLYGON"))
    stopifnot(names(shp_sf)[ncol(shp_sf)] == "geometry")

    dplyr::arrange(shp_sf, code_state, code_muni, code_tract)
  }

  log_progress(sprintf("Step 4a: clean_one(urbano) — %d features, harmonize_geobr...", nrow(urb_sf)))
  urb_clean <- clean_one(urb_sf, "urbano")
  log_progress(sprintf("Step 4a: Done (%d features)", nrow(urb_clean)))

  log_progress(sprintf("Step 4b: clean_one(rural) — %d features, harmonize_geobr...", nrow(rur_sf)))
  rur_clean <- clean_one(rur_sf, "rural")
  log_progress(sprintf("Step 4b: Done (%d features)", nrow(rur_clean)))

  # --- 5. Unified: urban takes priority over rural for overlapping codes ---
  log_progress("Step 5: Unifying urban + rural...")
  # Empirically, some code_tract values appear in both shapefiles (the rural
  # 1:500k version is a simplified low-res fallback of what the urban 1:5k
  # file maps at high resolution). We prefer the urban geometry.
  message("[census_tract 2000] Unificando urbano + rural (urbano > rural em overlaps)...")
  rur_only <- dplyr::filter(rur_clean, !code_tract %in% urb_clean$code_tract)
  n_overlap <- nrow(rur_clean) - nrow(rur_only)
  uni_clean <- dplyr::bind_rows(urb_clean, rur_only) |>
    dplyr::arrange(code_state, code_muni, code_tract)

  n_dup <- sum(duplicated(uni_clean$code_tract))
  if (n_dup > 0) {
    warning(sprintf("[census_tract 2000] %d code_tract duplicados no unified (provavelmente rural_aggregated)", n_dup))
  }
  message(sprintf("[census_tract 2000] Unified: %d features (urb=%d, rur=%d, overlap removido=%d)",
                  nrow(uni_clean), nrow(urb_clean), nrow(rur_only), n_overlap))
  message(sprintf("[census_tract 2000] geom_source: %s",
                  paste(names(table(uni_clean$geom_source)),
                        table(uni_clean$geom_source), sep="=", collapse=", ")))

  # --- 5b. Fix spatial overlaps (DISABLED — too slow for ~200k features) ---
  # TODO: revisit fix_spatial_overlaps() performance. Currently takes 4+ hours
  # and appears to hang on complex GEOS operations for large municipalities.
  # The function is defined above but not called. Overlaps remain in the data
  # (mainly urban vs rural at different scales). Investigate later.
  # uni_clean <- fix_spatial_overlaps(uni_clean)
  uni_clean <- dplyr::arrange(uni_clean, code_state, code_muni, code_tract)

  # --- 6. Write six parquets (3 full + 3 simplified) ---
  log_progress(sprintf("Step 6: Writing 6 parquets (%d unified features)...", nrow(uni_clean)))
  write_trio <- function(sf, suffix) {
    path_full <- paste0(dir_clean, "/censustracts_2000", suffix, ".parquet")
    path_simp <- paste0(dir_clean, "/censustracts_2000", suffix, "_simplified.parquet")
    write_geobr_parquet(sf, path_full)
    write_geobr_parquet(simplify_temp_sf(sf, tolerance = 10), path_simp)
    c(path_full, path_simp)
  }

  files <- c(
    write_trio(urb_clean, "_urbano"),
    write_trio(rur_clean, "_rural"),
    write_trio(uni_clean, "")
  )

  log_progress(sprintf("=== DONE: %d parquets written ===", length(files)))
  message(sprintf("[census_tract 2000] FINAL: %d arquivos gravados", length(files)))
  return(files)
}


# ==============================================================================
# Download 2007: Contagem da Populacao (urban + rural)
# ==============================================================================
# 2007 was an intermediate population count between the 2000 and 2010 censuses.
# IBGE published its census tract mesh with the same urban/rural separation
# pattern as 2000, but with improvements:
#
#   - RURAL: published at 1:2500 scale (vs 1:500k in 2000) in SIRGAS 2000
#     geographic projection (EPSG:4674 native, no reprojection needed).
#     Structure: setor_rural/shape/2500/geografica_sirgas2000/ufs/{uf}/
#     Files: {NN}se2500gsr.zip (SE = setor, 2500 scale, g=geo, sr=sirgas-rural)
#     Still contains range notation (NNNNN-MMMM) like 2000.
#
#   - URBAN: published per UF (not per municipality like 2000 did).
#     Structure: setor_urbano/{uf}/SHAPE_UTM_GEO_S69.zip
#     Contains subdirectories per municipality with {code_muni}.shp inside.
#     UTM SAD69 projection (zone varies by UF).
#
# Metadata enrichment: censobr does NOT publish tabular data for 2007. We fall
# back on the censobr 2010 parquet as proxy for name_muni/name_district/etc.,
# and use municipalities_2010.parquet for name_muni.
download_censustract_2007 <- function(year) {

  stopifnot(year == 2007)

  # --- URLs ---
  ftp_urb <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2007/setor_urbano/"
  ftp_rur <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2007/setor_rural/shape/2500/geografica_sirgas2000/ufs/"
  censobr_url <- "https://github.com/ipeaGIT/censobr/releases/download/v0.5.0/2010_tracts_Basico_v0.5.0.parquet"

  # IBGE UF code → 2-letter lowercase folder name
  uf_codes <- c("11"="ro","12"="ac","13"="am","14"="rr","15"="pa","16"="ap","17"="to",
                "21"="ma","22"="pi","23"="ce","24"="rn","25"="pb","26"="pe","27"="al",
                "28"="se","29"="ba","31"="mg","32"="es","33"="rj","35"="sp","41"="pr",
                "42"="sc","43"="rs","50"="ms","51"="mt","52"="go","53"="df")
  ufs_letters <- unname(uf_codes)
  uf_num_to_letter <- function(n) uf_codes[sprintf("%02d", n)]

  # --- Temp dirs (persistent) ---
  tmp     <- "./data-raw/census_tract/2007"
  zip_urb <- paste0(tmp, "/zips_urbano")
  zip_rur <- paste0(tmp, "/zips_rural")
  shp_urb <- paste0(tmp, "/shps_urbano")
  shp_rur <- paste0(tmp, "/shps_rural")
  for (d in c(zip_urb, zip_rur, shp_urb, shp_rur)) {
    dir.create(d, recursive = TRUE, showWarnings = FALSE)
  }

  # --- 1. Censobr 2010 tabular (proxy for metadata, since 2007 has no censobr) ---
  censobr_path <- paste0(tmp, "/censobr_tracts_2010.parquet")
  if (!file.exists(censobr_path) || file.size(censobr_path) < 1e6) {
    message("[census_tract 2007] Baixando censobr 2010 tabular (proxy para metadata)...")
    tryCatch(
      download.file(censobr_url, censobr_path, mode = "wb", quiet = TRUE),
      error = function(e) stop("Censobr download falhou: ", conditionMessage(e))
    )
  } else {
    message("[census_tract 2007] Censobr 2010 ja em cache, pulando.")
  }

  # --- 2. Rural (27 ZIPs, sequential + retry) ---
  message("[census_tract 2007] === RURAL (27 UFs, 1:2500 SIRGAS 2000) ===")
  uf_nums <- sprintf("%02d", as.integer(names(uf_codes)))
  rural_urls <- paste0(ftp_rur, uf_codes, "/", uf_nums, "se2500gsr.zip")
  rural_dest <- paste0(zip_rur, "/", uf_nums, "se2500gsr.zip")

  for (i in seq_along(uf_codes)) {
    if (file.exists(rural_dest[i]) && file.size(rural_dest[i]) > 1000) next
    message(sprintf("[census_tract 2007 rural] Baixando %s (%d/%d)...",
                    toupper(uf_codes[i]), i, length(uf_codes)))
    tryCatch(
      download.file(rural_urls[i], rural_dest[i], mode = "wb", quiet = TRUE),
      error = function(e) warning(paste("Rural download falhou:", uf_codes[i]))
    )
  }

  # Retry rounds
  for (round in 1:3) {
    ok <- file.exists(rural_dest) & file.size(rural_dest) > 1000
    if (all(ok)) break
    failed <- which(!ok)
    delay <- round * 3
    message(sprintf("[census_tract 2007 rural] Retry round %d: %d estados (delay %ds)...",
                    round, length(failed), delay))
    Sys.sleep(delay)
    for (i in failed) {
      tryCatch(
        download.file(rural_urls[i], rural_dest[i], mode = "wb", quiet = TRUE),
        error = function(e) warning(paste("Retry falhou:", uf_codes[i]))
      )
    }
  }

  ok <- file.exists(rural_dest) & file.size(rural_dest) > 1000
  if (!all(ok)) {
    stop(paste("Downloads rurais 2007 falharam para:",
               paste(uf_codes[!ok], collapse = ", ")))
  }
  message(sprintf("[census_tract 2007 rural] FINAL: %d/%d OK", sum(ok), length(uf_codes)))

  unzip_geobr(zip_dir = zip_rur, out_zip = shp_rur)

  # --- 3. Urban (27 ZIPs, sequential — 1 per UF, not per muni like 2000) ---
  message("[census_tract 2007] === URBANO (27 UFs) ===")
  urb_urls  <- paste0(ftp_urb, uf_codes, "/SHAPE_UTM_GEO_S69.zip")
  urb_dest  <- paste0(zip_urb, "/", uf_codes, "_SHAPE_UTM_GEO_S69.zip")

  for (i in seq_along(uf_codes)) {
    if (file.exists(urb_dest[i]) && file.size(urb_dest[i]) > 1000) next
    message(sprintf("[census_tract 2007 urbano] Baixando %s (%d/%d)...",
                    toupper(uf_codes[i]), i, length(uf_codes)))
    tryCatch(
      download.file(urb_urls[i], urb_dest[i], mode = "wb", quiet = TRUE),
      error = function(e) warning(paste("Urbano download falhou:", uf_codes[i]))
    )
  }

  # Retry rounds
  for (round in 1:3) {
    ok <- file.exists(urb_dest) & file.size(urb_dest) > 1000
    if (all(ok)) break
    failed <- which(!ok)
    delay <- round * 3
    message(sprintf("[census_tract 2007 urbano] Retry round %d: %d estados (delay %ds)...",
                    round, length(failed), delay))
    Sys.sleep(delay)
    for (i in failed) {
      tryCatch(
        download.file(urb_urls[i], urb_dest[i], mode = "wb", quiet = TRUE),
        error = function(e) warning(paste("Retry falhou:", uf_codes[i]))
      )
    }
  }

  ok <- file.exists(urb_dest) & file.size(urb_dest) > 1000
  message(sprintf("[census_tract 2007 urbano] FINAL: %d/%d OK", sum(ok), length(uf_codes)))
  if (!all(ok)) {
    warning(paste("Downloads urbanos 2007 falharam para:",
                  paste(uf_codes[!ok], collapse = ", ")))
  }

  # Unzip each UF's zip into a uf-specific subdir. Use Python for extraction
  # because IBGE zip files contain directory names with Latin-1 accented
  # characters (e.g., "Acaraú") that R's utils::unzip silently fails on Windows.
  for (i in seq_along(uf_codes)) {
    if (!file.exists(urb_dest[i])) next
    uf_out <- file.path(shp_urb, uf_codes[i])
    dir.create(uf_out, recursive = TRUE, showWarnings = FALSE)
    # Check if already extracted (at least 2 .shp files expected)
    n_existing <- length(list.files(uf_out, pattern = "\\.shp$",
                                    recursive = TRUE, ignore.case = TRUE))
    if (n_existing >= 2) next
    py_cmd <- sprintf(
      'python3 -c "import zipfile,os; z=zipfile.ZipFile(r\'%s\'); [setattr(i,\'filename\',i.filename.encode(\'cp437\').decode(\'latin-1\')) or z.extract(i,r\'%s\') for i in z.infolist()]; z.close()"',
      normalizePath(urb_dest[i], winslash = "/"),
      normalizePath(uf_out, winslash = "/", mustWork = FALSE)
    )
    tryCatch(
      system(py_cmd, intern = TRUE),
      error = function(e) warning(paste("Unzip falhou para", uf_codes[i]))
    )
  }

  # --- 4. Return list (same pattern as 2000) ---
  list(
    year         = year,
    shp_urb_dir  = shp_urb,
    shp_rur_dir  = shp_rur,
    censobr_path = censobr_path
  )
}


# ==============================================================================
# Clean 2007: harmonize urban + rural, produce 3 pairs of parquets
# ==============================================================================
clean_censustract_2007 <- function(raw) {

  yyyy <- 2007
  dir_clean <- paste0("./data/census_tract/", yyyy)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  # --- 1. Read censobr 2010 tabular (proxy for metadata) ---
  # censobr 2010 uses Basico_V1005 for situacao (1-8: urban/rural type)
  message("[census_tract 2007] Lendo censobr 2010 tabular (proxy)...")
  meta <- arrow::read_parquet(raw$censobr_path) |>
    dplyr::select(
      code_tract, code_muni, name_muni,
      code_district, name_district,
      code_subdistrict, name_subdistrict,
      code_neighborhood, name_neighborhood,
      situacao = Basico_V1005
    ) |>
    dplyr::mutate(code_tract = as.character(code_tract))
  message(sprintf("[census_tract 2007] Censobr 2010 proxy: %d rows", nrow(meta)))

  # --- 2. Read rural shapefiles (already in SIRGAS 2000, EPSG:4674) ---
  message("[census_tract 2007] Lendo shapefiles rurais...")
  rur_files <- list.files(raw$shp_rur_dir, pattern = "se2500gsr\\.shp$",
                          full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
  message(sprintf("[census_tract 2007] %d shapefiles rurais encontrados", length(rur_files)))

  rur_list <- purrr::map(rur_files, function(f) {
    x <- sf::st_read(f, quiet = TRUE, stringsAsFactors = FALSE,
                     options = "ENCODING=WINDOWS-1252")
    names(x) <- tolower(names(x))

    # Set CRS if missing (expected SIRGAS 2000 = EPSG:4674 for this dataset)
    if (is.na(sf::st_crs(x))) {
      sf::st_crs(x) <- sf::st_crs(4674)
    } else if (sf::st_crs(x)$epsg != 4674) {
      x <- sf::st_transform(x, 4674)
    }

    # Find the GEOCODIGO column (may be case-sensitive)
    geo_col <- grep("^geocodigo$", names(x), value = TRUE)[1]
    if (is.na(geo_col)) geo_col <- "geocodigo"

    # Collapse range notation (same logic as 2000 rural)
    x$code_tract <- sub("-.*$", "", as.character(x[[geo_col]]))
    x$zone_src <- "rural_shp"

    x[, c("code_tract", "zone_src")]
  })
  rur_sf <- do.call(rbind, rur_list)
  if (attr(rur_sf, "sf_column") != "geometry") {
    names(rur_sf)[names(rur_sf) == attr(rur_sf, "sf_column")] <- "geometry"
    sf::st_geometry(rur_sf) <- "geometry"
  }
  # Drop duplicates from range collapse
  dup_idx <- which(duplicated(rur_sf$code_tract))
  if (length(dup_idx) > 0) {
    message(sprintf("[census_tract 2007] Rural: removendo %d duplicatas pos-collapse",
                    length(dup_idx)))
    rur_sf <- rur_sf[-dup_idx, ]
  }
  message(sprintf("[census_tract 2007] Rural combinado: %d features", nrow(rur_sf)))

  # --- 3. Read urban shapefiles (UTM SAD69 per UF -> EPSG:4674) ---
  # IBGE 2007 urban ZIPs contain TWO versions per municipality:
  #   - {CODE}.SHP (original 2007, uppercase)
  #   - g{CODE}.shp (geocoded version 2008, lowercase with 'g' prefix)
  # We use only the geocoded (g*) version to avoid duplicates.
  message("[census_tract 2007] Lendo shapefiles urbanos...")
  urb_files <- list.files(raw$shp_urb_dir, pattern = "^g[0-9]+\\.shp$",
                          full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
  message(sprintf("[census_tract 2007] %d shapefiles urbanos encontrados", length(urb_files)))

  urb_list <- purrr::map(urb_files, function(f) {
    # Try multiple .prj extension cases
    prj_candidates <- c(sub("\\.shp$", ".prj", f, ignore.case = TRUE),
                        sub("\\.shp$", ".PRJ", f, ignore.case = TRUE))
    prj_file <- prj_candidates[file.exists(prj_candidates)][1]
    zone <- if (!is.na(prj_file)) detect_utm_zone_from_prj(prj_file) else NA_integer_

    x <- sf::st_read(f, quiet = TRUE, stringsAsFactors = FALSE,
                     options = "ENCODING=WINDOWS-1252")
    names(x) <- tolower(names(x))

    # Assign SAD69 UTM south if .prj indicated a UTM zone
    if (!is.na(zone)) {
      epsg_sad69 <- 29170L + zone
      sf::st_crs(x) <- sf::st_crs(epsg_sad69)
    } else if (is.na(sf::st_crs(x))) {
      # Fallback: leave NA and warn; many 2007 urban shps have no .prj
      warning(sprintf("[census_tract 2007 urbano] .prj sem zona: %s", basename(f)))
    }

    x <- sf::st_transform(x, 4674)

    # Urban shapefile uses "id_" column (same as 2000 urban).
    # Some shapefiles (e.g. g5104203, g4302303) have only geometry — no attributes.
    # For those, skip (no code_tract to assign).
    id_col <- grep("^id_?$", names(x), value = TRUE)[1]
    if (is.na(id_col)) {
      warning(sprintf("[census_tract 2007 urbano] sem ID_: %s (%d feats), pulando",
                      basename(f), nrow(x)))
      return(NULL)
    }
    x$code_tract <- as.character(x[[id_col]])
    x$zone_src <- "urbano_shp"

    x[, c("code_tract", "zone_src")]
  })
  urb_list <- Filter(Negate(is.null), urb_list)
  urb_sf <- do.call(rbind, urb_list)
  if (attr(urb_sf, "sf_column") != "geometry") {
    names(urb_sf)[names(urb_sf) == attr(urb_sf, "sf_column")] <- "geometry"
    sf::st_geometry(urb_sf) <- "geometry"
  }
  message(sprintf("[census_tract 2007] Urbano combinado: %d features", nrow(urb_sf)))

  # --- 4. Common cleaning: join with censobr 2010 proxy + harmonize ---
  clean_one <- function(shp_sf, label) {

    message(sprintf("[census_tract 2007 %s] Join com censobr 2010 (proxy)...", label))

    # LEFT JOIN with censobr 2010 tabular
    shp_sf <- dplyr::left_join(shp_sf, meta, by = "code_tract")

    # Fallback 1: derive code_muni from code_tract
    missing_muni_code <- is.na(shp_sf$code_muni) & !is.na(shp_sf$code_tract)
    if (sum(missing_muni_code) > 0) {
      shp_sf$code_muni[missing_muni_code] <- suppressWarnings(
        as.numeric(substr(shp_sf$code_tract[missing_muni_code], 1, 7))
      )
      message(sprintf("[census_tract 2007 %s] %d rows sem censobr: code_muni derivado",
                      label, sum(missing_muni_code)))
    }

    # Fallback 2: fill name_muni from municipalities_2010.parquet (closest to 2007)
    # Ensure code_muni is numeric for join compatibility
    shp_sf$code_muni <- as.numeric(shp_sf$code_muni)
    missing_name <- is.na(shp_sf$name_muni) & !is.na(shp_sf$code_muni)
    if (sum(missing_name) > 0) {
      muni_path <- "./data/municipality/2010/municipalities_2010.parquet"
      if (!file.exists(muni_path)) {
        muni_path <- "./data/municipality/2000/municipalities_2000.parquet"
      }
      if (file.exists(muni_path)) {
        # Only need code_muni + name_muni (no geometry needed)
        muni_lookup <- as.data.frame(arrow::read_parquet(muni_path,
          col_select = c("code_muni", "name_muni")))
        names(muni_lookup)[names(muni_lookup) == "name_muni"] <- "name_muni_fallback"
        muni_lookup$code_muni <- as.numeric(muni_lookup$code_muni)
        muni_lookup <- dplyr::distinct(muni_lookup, code_muni, .keep_all = TRUE)
        shp_sf <- shp_sf |>
          dplyr::left_join(muni_lookup, by = "code_muni",
                           relationship = "many-to-one") |>
          dplyr::mutate(
            name_muni = dplyr::coalesce(name_muni, name_muni_fallback)
          ) |>
          dplyr::select(-name_muni_fallback)
        message(sprintf("[census_tract 2007 %s] name_muni fallback via municipality: %d preenchidos",
                        label, sum(missing_name) - sum(is.na(shp_sf$name_muni))))
      }
    }

    # Report match rate
    n_na <- sum(is.na(shp_sf$name_muni))
    pct_na <- n_na / nrow(shp_sf)
    message(sprintf("[census_tract 2007 %s] name_muni: %d/%d (%.2f%% NA)",
                    label, nrow(shp_sf) - n_na, nrow(shp_sf), 100 * pct_na))

    # Derive zone (same logic as 2000)
    shp_sf <- shp_sf |>
      dplyr::mutate(
        zone = dplyr::case_when(
          situacao %in% c(1, 2, 3)             ~ "Urbana",
          situacao %in% c(4, 5, 6, 7, 8)        ~ "Rural",
          zone_src == "urbano_shp"              ~ "Urbana",
          zone_src == "rural_shp"               ~ "Rural",
          TRUE                                  ~ NA_character_
        )
      )
    shp_sf$zone_src <- NULL
    shp_sf$situacao <- NULL
    shp_sf$year <- yyyy

    # Pre-clean degenerate geometries BEFORE harmonize_geobr.
    # 2007 urban shapefiles have ~4.6% invalid geometries (degenerate polygons,
    # self-intersections, unclosed rings). lwgeom_make_valid with s2=OFF resolves
    # all but ~5 truly broken features (0.007% loss).
    n_before <- nrow(shp_sf)
    old_s2 <- sf::sf_use_s2()
    sf::sf_use_s2(FALSE)

    # Step 1: remove empties
    is_empty <- sf::st_is_empty(shp_sf)
    if (any(is_empty)) shp_sf <- shp_sf[!is_empty, ]

    # Step 2: lwgeom_make_valid on invalid features (chunked on sfc only)
    bad_ix <- which(!sf::st_is_valid(shp_sf))
    if (length(bad_ix) > 0) {
      message(sprintf("[census_tract 2007 %s] %d geometrias invalidas, corrigindo...",
                      label, length(bad_ix)))
      geom <- sf::st_geometry(shp_sf)
      bad_sfc <- geom[bad_ix]
      chunk_size <- 500L
      n_bad <- length(bad_sfc)
      for (start in seq(1L, n_bad, by = chunk_size)) {
        end <- min(start + chunk_size - 1L, n_bad)
        chunk <- bad_sfc[start:end]
        fixed <- tryCatch(
          lwgeom::lwgeom_make_valid(chunk),
          error = function(e) {
            # Fallback: fix one-by-one within failed chunk
            empty_geom <- sf::st_sfc(sf::st_polygon(), crs = sf::st_crs(geom))
            result <- chunk
            for (j in seq_along(chunk)) {
              result[j] <- tryCatch(
                lwgeom::lwgeom_make_valid(chunk[j]),
                error = function(e2) empty_geom
              )
            }
            result
          }
        )
        bad_sfc[start:end] <- fixed
      }
      geom[bad_ix] <- bad_sfc
      sf::st_geometry(shp_sf) <- geom
    }

    # Step 3: remove empties created by make_valid failures
    is_empty2 <- sf::st_is_empty(shp_sf)
    if (any(is_empty2)) shp_sf <- shp_sf[!is_empty2, ]

    # Step 4: extract POLYGON components from GEOMETRYCOLLECTIONs, re-validate,
    # and cast to MULTIPOLYGON so fix_topology() inside harmonize_geobr receives
    # homogeneous, valid geometry types
    shp_sf <- sf::st_collection_extract(shp_sf, "POLYGON")
    shp_sf <- sf::st_make_valid(shp_sf)
    is_empty3 <- sf::st_is_empty(shp_sf)
    if (any(is_empty3)) shp_sf <- shp_sf[!is_empty3, ]
    shp_sf <- sf::st_cast(shp_sf, "MULTIPOLYGON")

    sf::sf_use_s2(old_s2)
    n_after <- nrow(shp_sf)
    if (n_before != n_after) {
      message(sprintf("[census_tract 2007 %s] Pre-clean: %d -> %d (%d removidas)",
                      label, n_before, n_after, n_before - n_after))
    }

    # Now harmonize_geobr with standard flags (topology_fix + multipolygon ON)
    shp_sf <- harmonize_geobr(
      temp_sf        = shp_sf,
      year           = yyyy,
      add_state      = TRUE,
      state_column   = "code_muni",
      add_region     = TRUE,
      region_column  = "code_state",
      add_snake_case = FALSE,
      projection_fix = TRUE,
      encoding_utf8  = TRUE,
      topology_fix   = TRUE,
      remove_z_dimension = TRUE,
      use_multipolygon   = TRUE
    )

    shp_sf <- code_cols_to_numeric(shp_sf)

    # Column order (identical to 2000/2010/2022)
    shp_sf <- shp_sf |>
      dplyr::select(
        code_tract,
        code_muni, name_muni,
        code_neighborhood, name_neighborhood,
        code_district, name_district,
        code_subdistrict, name_subdistrict,
        zone,
        code_state, abbrev_state, name_state,
        code_region, name_region,
        year,
        geometry
      )

    stopifnot(!is.na(sf::st_crs(shp_sf)))
    stopifnot(sf::st_crs(shp_sf)$epsg == 4674)
    stopifnot(all(sf::st_geometry_type(shp_sf) == "MULTIPOLYGON"))
    stopifnot(names(shp_sf)[ncol(shp_sf)] == "geometry")

    dplyr::arrange(shp_sf, code_state, code_muni, code_tract)
  }

  urb_clean <- clean_one(urb_sf, "urbano")
  rur_clean <- clean_one(rur_sf, "rural")

  # --- 5. Unified: urban overlaps rural ---
  message("[census_tract 2007] Unificando urbano + rural (urbano > rural)...")
  rur_only <- dplyr::filter(rur_clean, !code_tract %in% urb_clean$code_tract)
  n_overlap <- nrow(rur_clean) - nrow(rur_only)
  uni_clean <- dplyr::bind_rows(urb_clean, rur_only) |>
    dplyr::arrange(code_state, code_muni, code_tract)

  stopifnot(anyDuplicated(uni_clean$code_tract) == 0)
  message(sprintf("[census_tract 2007] Unified: %d features (urb=%d, rur=%d, overlap=%d)",
                  nrow(uni_clean), nrow(urb_clean), nrow(rur_only), n_overlap))

  # --- 6. Write 6 parquets ---
  write_trio <- function(sf, suffix) {
    path_full <- paste0(dir_clean, "/censustracts_2007", suffix, ".parquet")
    path_simp <- paste0(dir_clean, "/censustracts_2007", suffix, "_simplified.parquet")
    write_geobr_parquet(sf, path_full)
    write_geobr_parquet(simplify_temp_sf(sf, tolerance = 10), path_simp)
    c(path_full, path_simp)
  }

  files <- c(
    write_trio(urb_clean, "_urbano"),
    write_trio(rur_clean, "_rural"),
    write_trio(uni_clean, "")
  )

  message(sprintf("[census_tract 2007] FINAL: %d arquivos gravados", length(files)))
  return(files)
}
