# reports/investigation/phase_a.R
#
# Phase A of census_tract 2000 quality investigation.
# Combines agents A1, A2, A3 into a single reproducible script.
#
# Usage:
#   "/c/Program Files/R/R-4.5.0/bin/Rscript.exe" reports/investigation/phase_a.R
#
# Outputs in reports/artifacts/investigation/ + reports/figures/investigation/

setwd("d:/Dropbox/Artigos/geobr_prep_data")

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(arrow)
  library(geoarrow)
  library(foreign)  # for DBF reading
})

source("R/support_harmonize_geobr.R")

out_art <- "reports/artifacts/investigation"
out_fig <- "reports/figures/investigation"
dir.create(out_art, recursive = TRUE, showWarnings = FALSE)
dir.create(out_fig, recursive = TRUE, showWarnings = FALSE)

log <- function(...) cat(sprintf(...), "\n")

log("===== PHASE A: CENSUS_TRACT 2000 QUALITY INVESTIGATION =====\n")

# =============================================================================
# SHARED DATA LOADING
# =============================================================================

log("[LOAD] Loading all parquets and censobr...")

# Ensure censobr cache exists
censobr_path <- "data-raw/census_tract/2000/censobr_tracts_2000.parquet"
if (!file.exists(censobr_path) || file.size(censobr_path) < 1e6) {
  dir.create(dirname(censobr_path), recursive = TRUE, showWarnings = FALSE)
  log("  downloading censobr parquet (~15 MB)...")
  download.file(
    "https://github.com/ipeaGIT/censobr/releases/download/v0.5.0/2000_tracts_Basico_v0.5.0.parquet",
    censobr_path, mode = "wb", quiet = TRUE
  )
}

censobr <- arrow::read_parquet(censobr_path)
censobr_codes <- as.character(censobr$code_tract)
log("  censobr: %d rows", nrow(censobr))

urb <- read_geoparquet("data/census_tract/2000/census_tracts_2000_urbano.parquet")
rur <- read_geoparquet("data/census_tract/2000/census_tracts_2000_rural.parquet")
uni <- read_geoparquet("data/census_tract/2000/census_tracts_2000.parquet")

urb_codes <- sprintf("%015.0f", urb$code_tract)
rur_codes <- sprintf("%015.0f", rur$code_tract)
uni_codes <- sprintf("%015.0f", uni$code_tract)

log("  urbano : %d rows", nrow(urb))
log("  rural  : %d rows", nrow(rur))
log("  unified: %d rows", nrow(uni))

# Ensure rural shapefiles are cached
rur_dir <- "data-raw/census_tract/2000/shps_rural"
rur_zip_dir <- "data-raw/census_tract/2000/zips_rural"
rur_shp_files <- list.files(rur_dir, pattern = "\\.shp$",
                            ignore.case = TRUE, full.names = TRUE)

if (length(rur_shp_files) < 27) {
  log("[LOAD] Downloading missing rural shapefiles...")
  dir.create(rur_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(rur_zip_dir, recursive = TRUE, showWarnings = FALSE)
  ufs <- c("ac","al","am","ap","ba","ce","df","es","go","ma","mg","ms","mt",
           "pa","pb","pe","pi","pr","rj","rn","ro","rr","rs","sc","se","sp","to")
  base <- paste0("https://geoftp.ibge.gov.br/organizacao_do_territorio/",
                 "malhas_territoriais/malhas_de_setores_censitarios__",
                 "divisoes_intramunicipais/censo_2000/setor_rural/",
                 "projecao_geografica/censo_2000/e500_arcview_shp/uf/")
  for (uf in ufs) {
    dst <- file.path(rur_zip_dir, paste0(uf, "_setores_censitarios.zip"))
    if (!file.exists(dst)) {
      url <- paste0(base, uf, "/", uf, "_setores_censitarios.zip")
      log("  downloading %s...", toupper(uf))
      tryCatch(download.file(url, dst, mode = "wb", quiet = TRUE),
               error = function(e) log("    failed: %s", conditionMessage(e)))
    }
    if (file.exists(dst)) {
      unzip(dst, exdir = rur_dir)
    }
  }
  rur_shp_files <- list.files(rur_dir, pattern = "\\.shp$",
                              ignore.case = TRUE, full.names = TRUE)
}
log("  %d rural shapefiles available", length(rur_shp_files))

# =============================================================================
# AGENT A1: Test hypothesis — 22k missing = absorbed range intermediaries
# =============================================================================

log("\n\n===== A1: Test absorption hypothesis =====\n")

log("[A1.1] Computing missing codes (censobr - unified)...")
missing_from_unified <- setdiff(censobr_codes, uni_codes)
n_missing <- length(missing_from_unified)
log("  N missing from unified: %d (expected ~22387)", n_missing)

log("[A1.2] Reading range notation from all rural shapefiles...")
all_ranges <- list()
for (f in rur_shp_files) {
  uf_code <- substr(basename(f), 1, 2)  # e.g. "11" for RO (not 2-letter UF)
  # Read via foreign::read.dbf for speed (no geometry needed here)
  dbf_file <- sub("\\.shp$", ".dbf", f, ignore.case = TRUE)
  # Find actual filename (windows shapefile case issues)
  dbf_real <- list.files(dirname(dbf_file),
                         pattern = paste0("^", basename(dbf_file), "$"),
                         ignore.case = TRUE, full.names = TRUE)[1]
  if (is.na(dbf_real)) dbf_real <- dbf_file
  dbf <- foreign::read.dbf(dbf_real, as.is = TRUE)
  # Find GEOCODIGO column (case insensitive)
  geo_col <- grep("^GEOCODIGO$", names(dbf), ignore.case = TRUE, value = TRUE)[1]
  if (is.na(geo_col)) next
  geo <- as.character(dbf[[geo_col]])
  is_range <- nchar(geo) == 20 & grepl("-", geo)
  if (sum(is_range) > 0) {
    all_ranges[[uf_code]] <- data.frame(
      uf_code_num = uf_code,
      geocodigo_original = geo[is_range],
      stringsAsFactors = FALSE
    )
  }
}
ranges_df <- do.call(rbind, all_ranges)
log("  Total range polygons in rural shapefiles: %d", nrow(ranges_df))

# Parse the ranges
ranges_df$start_code_15 <- substr(ranges_df$geocodigo_original, 1, 15)
ranges_df$end_suffix_4  <- substr(ranges_df$geocodigo_original, 17, 20)
ranges_df$prefix_11 <- substr(ranges_df$start_code_15, 1, 11)
ranges_df$start_num <- as.integer(substr(ranges_df$start_code_15, 12, 15))
ranges_df$end_num   <- as.integer(ranges_df$end_suffix_4)
ranges_df$K <- ranges_df$end_num - ranges_df$start_num + 1
# Drop any malformed
ranges_df <- ranges_df[!is.na(ranges_df$K) & ranges_df$K > 0, ]
log("  After parsing: %d valid ranges", nrow(ranges_df))
log("  K distribution: min=%d, median=%d, mean=%.1f, max=%d",
    min(ranges_df$K), median(ranges_df$K),
    mean(ranges_df$K), max(ranges_df$K))
log("  Sum of K:     %d  (total censobr codes these polygons represent)",
    sum(ranges_df$K))
log("  Sum of (K-1): %d  (intermediaries absorbed — the hypothesis target)",
    sum(ranges_df$K - 1))

log("[A1.3] Expanding ranges into all intermediary codes...")
expand_range <- function(prefix, start_num, end_num) {
  sprintf("%s%04d", prefix, start_num:end_num)
}
all_expanded <- unlist(Map(expand_range,
                           ranges_df$prefix_11,
                           ranges_df$start_num,
                           ranges_df$end_num))
n_expanded_total <- length(all_expanded)
n_expanded_unique <- length(unique(all_expanded))
log("  Total expanded codes: %d (unique: %d)",
    n_expanded_total, n_expanded_unique)

# First of range = codes that survive the collapse
first_of_range <- sprintf("%s%04d",
                          ranges_df$prefix_11, ranges_df$start_num)
first_of_range_unique <- unique(first_of_range)
log("  First-of-range codes (survive collapse): %d unique",
    length(first_of_range_unique))

# Intermediaries = all expanded MINUS the first of each range
intermediaries <- setdiff(all_expanded, first_of_range_unique)
log("  Intermediaries (absorbed, no geometry in unified): %d",
    length(intermediaries))

log("[A1.4] Testing hypothesis: overlap with missing_from_unified...")
overlap <- intersect(intermediaries, missing_from_unified)
n_overlap <- length(overlap)
pct_explained <- 100 * n_overlap / n_missing

log("  N missing total      : %d", n_missing)
log("  N intermediaries     : %d", length(intermediaries))
log("  N overlap (explained): %d", n_overlap)
log("  N NOT explained      : %d", n_missing - n_overlap)
log("  PCT explained        : %.2f%%", pct_explained)

if (pct_explained > 90) {
  log("  HYPOTHESIS CONFIRMED (>90%% of missing are range intermediaries)")
} else {
  log("  HYPOTHESIS REFUTED or partial (%.2f%% < 90%%)", pct_explained)
}

# Classify the missing 22k by cause
log("[A1.5] Classifying 22k missing by cause...")
missing_df <- data.frame(
  code_tract = missing_from_unified,
  stringsAsFactors = FALSE
)
missing_df$code_state <- as.integer(substr(missing_df$code_tract, 1, 2))
missing_df$code_muni  <- as.integer(substr(missing_df$code_tract, 1, 7))
missing_df$cause <- dplyr::case_when(
  missing_df$code_tract %in% intermediaries ~ "absorbed_by_range",
  missing_df$code_tract %in% rur_codes      ~ "in_rural_parquet_but_not_unified",
  TRUE                                       ~ "unknown"
)

cause_summary <- missing_df |>
  dplyr::count(cause) |>
  dplyr::mutate(pct = round(100 * n / sum(n), 2))
log("  Cause breakdown:")
for (i in seq_len(nrow(cause_summary))) {
  log("    %-40s %6d (%5.2f%%)",
      cause_summary$cause[i],
      cause_summary$n[i],
      cause_summary$pct[i])
}

write.csv(missing_df,
          file.path(out_art, "missing_from_unified.csv"),
          row.names = FALSE)

write.csv(cause_summary,
          file.path(out_art, "missing_cause_summary.csv"),
          row.names = FALSE)

# Save the math
math_df <- data.frame(
  metric = c("N_missing_from_unified",
             "N_ranges_in_rural_shapefile",
             "N_intermediaries_absorbed",
             "N_overlap_missing_intermediaries",
             "N_missing_unexplained",
             "pct_missing_explained_by_ranges"),
  value = c(n_missing,
            nrow(ranges_df),
            length(intermediaries),
            n_overlap,
            n_missing - n_overlap,
            round(pct_explained, 4))
)
write.csv(math_df,
          file.path(out_art, "range_absorption_math.csv"),
          row.names = FALSE)
log("  Saved: missing_from_unified.csv, missing_cause_summary.csv, range_absorption_math.csv")

# Ranges per UF
ranges_per_uf <- ranges_df |>
  dplyr::mutate(intermediaries_k_minus_1 = K - 1) |>
  dplyr::group_by(uf_code_num) |>
  dplyr::summarise(
    n_ranges = dplyr::n(),
    n_intermediaries_absorbed = sum(intermediaries_k_minus_1),
    K_max = max(K),
    K_median = median(K),
    .groups = "drop"
  ) |>
  dplyr::arrange(as.integer(uf_code_num))
write.csv(ranges_per_uf,
          file.path(out_art, "ranges_per_uf.csv"),
          row.names = FALSE)
log("  Saved: ranges_per_uf.csv")

# =============================================================================
# AGENT A2: Range notation vs urbano (P2.1, P2.2, P2.3)
# =============================================================================

log("\n\n===== A2: Range notation vs urbano =====\n")

log("[A2.1] Checking which expanded codes also exist in urbano shapefile...")
expanded_in_urbano <- intersect(all_expanded, urb_codes)
pct_exp_in_urb <- 100 * length(expanded_in_urbano) / n_expanded_total
log("  expanded_in_urbano: %d / %d (%.2f%%)",
    length(expanded_in_urbano), n_expanded_total, pct_exp_in_urb)

expanded_only_in_rural_range <- setdiff(all_expanded, urb_codes)
log("  expanded_only_in_rural_range: %d",
    length(expanded_only_in_rural_range))

log("[A2.2] First-of-range: which venceu the dedup (urbano vs rural)?")
first_in_urbano <- intersect(first_of_range_unique, urb_codes)
first_NOT_in_urbano <- setdiff(first_of_range_unique, urb_codes)
log("  First-of-range in urbano (dedup kept urbano): %d",
    length(first_in_urbano))
log("  First-of-range NOT in urbano (still rural aggregated): %d",
    length(first_NOT_in_urbano))

# Verify empirically: geometry size for first-of-range in unified
check_geom_sizes <- function(codes) {
  idx <- match(codes, uni_codes)
  idx <- idx[!is.na(idx)]
  if (length(idx) == 0) return(NA_real_)
  areas <- as.numeric(sf::st_area(uni[idx, ])) / 1e6
  median(areas, na.rm = TRUE)
}

med_urb_winner <- check_geom_sizes(first_in_urbano)
med_rur_residual <- check_geom_sizes(first_NOT_in_urbano)
log("  Median area (km2) of first-in-urbano     : %.4f (small = urbano won)",
    med_urb_winner)
log("  Median area (km2) of first-not-in-urbano : %.4f (big = rural collapsed)",
    med_rur_residual)

log("[A2.3] Counting sectors hidden in still-aggregated polygons...")
still_aggregated_ranges <- ranges_df[
  first_of_range %in% first_NOT_in_urbano, ]
n_hidden_sectors <- sum(still_aggregated_ranges$K - 1)
log("  Polygons still aggregated: %d", nrow(still_aggregated_ranges))
log("  Sectors hidden in those polygons: %d", n_hidden_sectors)

# Save the analysis
ranges_analysis <- ranges_df
ranges_analysis$first_code <- first_of_range
ranges_analysis$first_in_urbano <- first_of_range %in% first_in_urbano
ranges_analysis$still_aggregated <- !ranges_analysis$first_in_urbano

# Count expanded-in-urbano per range
exp_per_range_in_urb <- vapply(seq_len(nrow(ranges_df)), function(i) {
  expanded_this <- expand_range(ranges_df$prefix_11[i],
                                ranges_df$start_num[i],
                                ranges_df$end_num[i])
  sum(expanded_this %in% urb_codes)
}, integer(1))
ranges_analysis$n_expanded_in_urbano <- exp_per_range_in_urb

write.csv(ranges_analysis,
          file.path(out_art, "range_notation_analysis.csv"),
          row.names = FALSE)

still_agg_df <- ranges_analysis[ranges_analysis$still_aggregated, ]
write.csv(still_agg_df,
          file.path(out_art, "range_still_aggregated.csv"),
          row.names = FALSE)
log("  Saved: range_notation_analysis.csv, range_still_aggregated.csv")

# =============================================================================
# AGENT A3: SP capital gaps (P3, P4)
# =============================================================================

log("\n\n===== A3: SP capital gaps =====\n")

sf::sf_use_s2(FALSE)   # more forgiving with complex geometries

log("[A3.1] Loading SP capital unified + envelope...")
sp_uni <- uni |> dplyr::filter(code_muni == 3550308)
log("  sp_uni: %d features", nrow(sp_uni))

muni_2000 <- read_geoparquet("data/municipality/2000/municipalities_2000.parquet")
sp_muni <- muni_2000 |> dplyr::filter(code_muni == 3550308)
log("  sp_muni envelope: %d rows", nrow(sp_muni))

envelope_area <- as.numeric(sf::st_area(sp_muni)) / 1e6
log("  SP capital envelope area: %.2f km2", sum(envelope_area))

log("[A3.2] Computing gaps via st_difference(envelope, union(setores))...")
sp_uni_valid <- sf::st_make_valid(sp_uni)
setores_union <- sf::st_union(sp_uni_valid)
setores_union <- sf::st_make_valid(setores_union)
gaps_raw <- sf::st_difference(sp_muni$geometry, setores_union)

gaps_poly <- sf::st_cast(gaps_raw, "POLYGON", warn = FALSE)
gaps_sf <- sf::st_sf(gap_id = seq_along(gaps_poly),
                     geometry = gaps_poly)
gaps_sf$area_km2 <- as.numeric(sf::st_area(gaps_sf)) / 1e6
gaps_sf <- gaps_sf[order(-gaps_sf$area_km2), ]
gaps_sf$gap_id <- seq_len(nrow(gaps_sf))

log("  Total gap polygons: %d", nrow(gaps_sf))
log("  Total gap area: %.4f km2 (%.4f%% of envelope)",
    sum(gaps_sf$area_km2),
    100 * sum(gaps_sf$area_km2) / sum(envelope_area))

# Classification
gaps_sf$category <- dplyr::case_when(
  gaps_sf$area_km2 > 1    ~ "large",
  gaps_sf$area_km2 > 0.01 ~ "medium",
  TRUE                    ~ "small"
)
cat_summary <- gaps_sf |>
  sf::st_drop_geometry() |>
  dplyr::group_by(category) |>
  dplyr::summarise(n = dplyr::n(),
                   total_area_km2 = sum(area_km2),
                   max_area_km2 = max(area_km2),
                   .groups = "drop")
log("\n  Classification:")
print(cat_summary)

# Centroid of top 10 largest gaps (lat/lon for interpretation)
top10 <- gaps_sf |> dplyr::slice_head(n = 10)
top10_centroids <- sf::st_transform(top10, 4326) |>
  sf::st_centroid() |>
  sf::st_coordinates()
top10_df <- sf::st_drop_geometry(top10)
top10_df$centroid_lon <- top10_centroids[, 1]
top10_df$centroid_lat <- top10_centroids[, 2]

log("\n  Top 10 largest gaps:")
print(top10_df)

write.csv(top10_df,
          file.path(out_art, "sp_capital_top10_gaps.csv"),
          row.names = FALSE)
write.csv(sf::st_drop_geometry(gaps_sf),
          file.path(out_art, "sp_capital_gaps.csv"),
          row.names = FALSE)

# Visualization
log("[A3.3] Generating sp_capital_gaps.png...")
large_gaps <- gaps_sf |> dplyr::filter(category == "large")
medium_gaps <- gaps_sf |> dplyr::filter(category == "medium")

png(file.path(out_fig, "sp_capital_gaps.png"),
    width = 1200, height = 1000, res = 130)
par(mar = c(1, 1, 3, 1))
plot(sf::st_geometry(sp_muni), col = "#ffffff", border = "#333333",
     main = sprintf("SP capital 2000 - gaps\n%d setores, envelope %.0f km2, gaps %.4f km2 (%.4f%%)",
                    nrow(sp_uni), sum(envelope_area),
                    sum(gaps_sf$area_km2),
                    100 * sum(gaps_sf$area_km2) / sum(envelope_area)))
plot(sf::st_geometry(sp_uni_valid), col = "#aed6f1",
     border = "#00000010", add = TRUE)
if (nrow(medium_gaps) > 0) {
  plot(sf::st_geometry(medium_gaps), col = "#ffa500",
       border = "#ffa500", add = TRUE)
}
if (nrow(large_gaps) > 0) {
  plot(sf::st_geometry(large_gaps), col = "#c0392b",
       border = "#c0392b", add = TRUE)
}
legend("bottomleft", legend = c("setores",
                                sprintf("medium gap (0.01-1 km2): %d", nrow(medium_gaps)),
                                sprintf("large gap (>1 km2): %d", nrow(large_gaps))),
       fill = c("#aed6f1", "#ffa500", "#c0392b"), bty = "n", cex = 0.9)
dev.off()
log("  Saved: sp_capital_gaps.png")

# Match to 2010 for large gaps
if (nrow(large_gaps) > 0) {
  log("[A3.4] Matching large gaps to setores 2010...")
  d10 <- read_geoparquet("data/census_tract/2010/census_tracts_2010.parquet")
  sp_2010 <- d10 |> dplyr::filter(code_muni == 3550308)
  log("  sp_2010: %d features", nrow(sp_2010))

  for (i in seq_len(nrow(large_gaps))) {
    gap_i <- large_gaps[i, ]
    intersects <- sf::st_intersects(sp_2010, gap_i, sparse = FALSE)[, 1]
    n_inter <- sum(intersects)
    log("  Large gap %d (%.2f km2): intersects %d setores 2010",
        large_gaps$gap_id[i], large_gaps$area_km2[i], n_inter)
    if (n_inter > 0) {
      inter_codes <- sp_2010$code_tract[intersects][1:min(5, n_inter)]
      log("    example 2010 codes: %s",
          paste(inter_codes, collapse = ", "))
    }
  }
}

# =============================================================================
# FINAL SUMMARY
# =============================================================================

log("\n\n===== PHASE A SUMMARY =====\n")
log("P1 (absorption hypothesis): %.2f%% of 22k missing explained by ranges",
    pct_explained)
log("P2.1 (expanded in urbano) : %.2f%% of expanded intermediaries have an urbano counterpart",
    pct_exp_in_urb)
log("P2.3 (still aggregated)   : %d polygons, hiding %d sectors",
    nrow(still_aggregated_ranges), n_hidden_sectors)
log("P3 (SP large gaps)        : %d gaps > 1 km2", sum(gaps_sf$category == "large"))
log("P4 (SP small slivers)     : %d slivers, total %.4f km2",
    sum(gaps_sf$category == "small"),
    sum(gaps_sf$area_km2[gaps_sf$category == "small"]))
log("\nArtifacts in:")
log("  %s", out_art)
log("  %s", out_fig)
