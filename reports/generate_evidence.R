# reports/generate_evidence.R
#
# Generates all empirical evidence (CSVs, RDS, PNGs) for the
# census_tract 2000 implementation report.
#
# Usage:
#   "/c/Program Files/R/R-4.5.0/bin/Rscript.exe" reports/generate_evidence.R
#
# Inputs:
#   ./data/census_tract/2000/census_tracts_2000*.parquet   (6 files)
#   ./data/census_tract/2010/census_tracts_2010.parquet
#   ./data/census_tract/2022/census_tracts_2022.parquet
#
# Outputs:
#   reports/artifacts/*.csv, *.rds   (statistics)
#   reports/figures/*.png            (visualizations)

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(arrow)
  library(geoarrow)
})

# Ensure we run from project root (where ./data/ and ./R/ live)
if (!dir.exists("data") || !dir.exists("R")) {
  stop("Run this script from the project root (geobr_prep_data/). ",
       "Current wd: ", getwd())
}

source("R/support_harmonize_geobr.R")

out_art <- "reports/artifacts"
out_fig <- "reports/figures"
dir.create(out_art, recursive = TRUE, showWarnings = FALSE)
dir.create(out_fig, recursive = TRUE, showWarnings = FALSE)

cat("=== Loading all six 2000 parquets + 2010 + 2022 ===\n")
f_all <- list.files("./data/census_tract/2000/", full.names = TRUE,
                    pattern = "\\.parquet$")
urb <- read_geoparquet("./data/census_tract/2000/census_tracts_2000_urbano.parquet")
rur <- read_geoparquet("./data/census_tract/2000/census_tracts_2000_rural.parquet")
uni <- read_geoparquet("./data/census_tract/2000/census_tracts_2000.parquet")
d10 <- read_geoparquet("./data/census_tract/2010/census_tracts_2010.parquet")
d22 <- read_geoparquet("./data/census_tract/2022/census_tracts_2022.parquet")

# Snapshot original column names BEFORE any mutation (e.g., adding area_km2 later)
orig_uni_names <- sort(names(uni))
orig_d10_names <- sort(names(d10))
orig_d22_names <- sort(names(d22))

# ---- 1. File sizes -----------------------------------------------------------
cat("[1/10] File sizes...\n")
sizes <- data.frame(
  file  = basename(f_all),
  bytes = file.size(f_all),
  mb    = round(file.size(f_all) / 1e6, 1)
)
sizes <- sizes[order(sizes$file), ]
write.csv(sizes, file.path(out_art, "file_sizes.csv"), row.names = FALSE)
print(sizes)

# ---- 2. Parquet properties ---------------------------------------------------
cat("\n[2/10] Parquet properties...\n")
props <- list(
  urbano = list(
    nrow = nrow(urb), ncol = ncol(urb),
    crs_epsg = sf::st_crs(urb)$epsg,
    geom_types = unique(as.character(sf::st_geometry_type(urb))),
    cols = names(urb),
    sf_column = attr(urb, "sf_column"),
    bbox = as.numeric(sf::st_bbox(urb))
  ),
  rural = list(
    nrow = nrow(rur), ncol = ncol(rur),
    crs_epsg = sf::st_crs(rur)$epsg,
    geom_types = unique(as.character(sf::st_geometry_type(rur))),
    cols = names(rur),
    sf_column = attr(rur, "sf_column"),
    bbox = as.numeric(sf::st_bbox(rur))
  ),
  unified = list(
    nrow = nrow(uni), ncol = ncol(uni),
    crs_epsg = sf::st_crs(uni)$epsg,
    geom_types = unique(as.character(sf::st_geometry_type(uni))),
    cols = names(uni),
    sf_column = attr(uni, "sf_column"),
    bbox = as.numeric(sf::st_bbox(uni))
  )
)
saveRDS(props, file.path(out_art, "parquet_properties.rds"))
cat("  urbano nrow :", props$urbano$nrow, "\n")
cat("  rural  nrow :", props$rural$nrow, "\n")
cat("  unified nrow:", props$unified$nrow, "\n")
cat("  CRS EPSG (all):", props$unified$crs_epsg, "\n")

# ---- 3. Zone distribution ----------------------------------------------------
cat("\n[3/10] Zone distribution...\n")
zone_stats <- uni |>
  sf::st_drop_geometry() |>
  dplyr::count(zone, name = "n") |>
  dplyr::mutate(pct = round(100 * n / sum(n), 2))
write.csv(zone_stats, file.path(out_art, "zone_distribution.csv"), row.names = FALSE)
print(zone_stats)

# ---- 4. Per-UF counts --------------------------------------------------------
cat("\n[4/10] Per-UF counts...\n")
uf_stats <- uni |>
  sf::st_drop_geometry() |>
  dplyr::group_by(code_state, abbrev_state, name_state,
                  code_region, name_region) |>
  dplyr::summarise(
    n_sectors = dplyr::n(),
    n_munis   = dplyr::n_distinct(code_muni),
    n_urban   = sum(zone == "Urbana"),
    n_rural   = sum(zone == "Rural"),
    .groups = "drop"
  ) |>
  dplyr::arrange(code_state)
write.csv(uf_stats, file.path(out_art, "per_uf_counts.csv"), row.names = FALSE)
print(uf_stats, n = 30)

# ---- 5. NA counts ------------------------------------------------------------
cat("\n[5/10] NA counts per column...\n")
na_counts <- uni |>
  sf::st_drop_geometry() |>
  dplyr::summarise(dplyr::across(dplyr::everything(), ~sum(is.na(.x)))) |>
  tidyr::pivot_longer(dplyr::everything(),
                      names_to = "column",
                      values_to = "n_na") |>
  dplyr::mutate(pct = round(100 * n_na / nrow(uni), 3))
write.csv(na_counts, file.path(out_art, "na_counts.csv"), row.names = FALSE)
print(na_counts, n = 20)

# ---- 6. Schema comparison 2000 vs 2010 vs 2022 -------------------------------
cat("\n[6/10] Schema comparison...\n")
schema_rows <- lapply(names(uni), function(nm) {
  t2000 <- class(sf::st_drop_geometry(uni)[[nm]])[1]
  t2010 <- if (nm %in% names(d10)) class(sf::st_drop_geometry(d10)[[nm]])[1] else NA
  t2022 <- if (nm %in% names(d22)) class(sf::st_drop_geometry(d22)[[nm]])[1] else NA
  data.frame(col = nm, type_2000 = t2000, type_2010 = t2010, type_2022 = t2022)
})
schema <- do.call(rbind, schema_rows)
write.csv(schema, file.path(out_art, "schema_comparison.csv"), row.names = FALSE)
print(schema)

# ---- 7. Figures --------------------------------------------------------------
cat("\n[7/10] Figures...\n")

# 7.1 AC urban+rural
ac <- uni |> dplyr::filter(code_state == 12)
png(file.path(out_fig, "ac_unified.png"), width = 1000, height = 800, res = 130)
par(mar = c(1, 1, 3, 1))
plot(sf::st_geometry(ac),
     col    = ifelse(ac$zone == "Urbana", "#c0392b", "#aed6f1"),
     border = "#00000020",
     main   = "Acre - Setores Censitarios 2000 (unified)\nvermelho = Urbana, azul = Rural")
legend("bottomleft",
       legend = c(sprintf("Urbana (%d)", sum(ac$zone == "Urbana")),
                  sprintf("Rural (%d)",  sum(ac$zone == "Rural"))),
       fill = c("#c0392b", "#aed6f1"),
       bty = "n", cex = 0.9)
dev.off()
cat("  reports/figures/ac_unified.png\n")

# 7.2 SP area around capital
sp_capital <- uni |> dplyr::filter(code_muni == 3550308)
if (nrow(sp_capital) > 0) {
  png(file.path(out_fig, "sp_capital_unified.png"), width = 1000, height = 900, res = 130)
  par(mar = c(1, 1, 3, 1))
  plot(sf::st_geometry(sp_capital),
       col    = ifelse(sp_capital$zone == "Urbana", "#c0392b", "#aed6f1"),
       border = "#00000010",
       main   = sprintf("Sao Paulo (capital) - Setores Censitarios 2000\n%d setores",
                        nrow(sp_capital)))
  dev.off()
  cat("  reports/figures/sp_capital_unified.png\n")
}

# 7.3 Area histogram by zone
cat("  computing area_km2 (may take a while)...\n")
uni$area_km2 <- suppressWarnings(
  as.numeric(sf::st_area(uni)) / 1e6
)
png(file.path(out_fig, "area_hist.png"), width = 1100, height = 600, res = 130)
par(mfrow = c(1, 2), mar = c(4.2, 4, 3, 1))
urban_areas <- uni$area_km2[uni$zone == "Urbana" & uni$area_km2 > 0]
rural_areas <- uni$area_km2[uni$zone == "Rural"  & uni$area_km2 > 0]
hist(log10(urban_areas), breaks = 60, col = "#c0392b", border = "white",
     main = sprintf("Urbana (n=%d)", length(urban_areas)),
     xlab = "log10(area em km2)", ylab = "frequencia")
abline(v = log10(median(urban_areas)), col = "black", lwd = 2, lty = 2)
hist(log10(rural_areas), breaks = 60, col = "#aed6f1", border = "white",
     main = sprintf("Rural (n=%d)", length(rural_areas)),
     xlab = "log10(area em km2)", ylab = "frequencia")
abline(v = log10(median(rural_areas)), col = "black", lwd = 2, lty = 2)
dev.off()
cat("  reports/figures/area_hist.png\n")

area_stats <- data.frame(
  zone = c("Urbana", "Rural"),
  n = c(length(urban_areas), length(rural_areas)),
  min_km2 = c(min(urban_areas), min(rural_areas)),
  median_km2 = c(median(urban_areas), median(rural_areas)),
  mean_km2 = c(mean(urban_areas), mean(rural_areas)),
  max_km2 = c(max(urban_areas), max(rural_areas)),
  q25_km2 = c(quantile(urban_areas, 0.25), quantile(rural_areas, 0.25)),
  q75_km2 = c(quantile(urban_areas, 0.75), quantile(rural_areas, 0.75))
)
write.csv(area_stats, file.path(out_art, "area_stats_by_zone.csv"), row.names = FALSE)

# 7.4 nrow by year
png(file.path(out_fig, "nrow_years.png"), width = 800, height = 600, res = 130)
par(mar = c(4, 4.5, 3, 1))
barplot(c("2000" = nrow(uni), "2010" = nrow(d10), "2022" = nrow(d22)),
        col    = c("#e67e22", "#3498db", "#27ae60"),
        main   = "Numero de setores censitarios por ano",
        ylab   = "n setores (unified)",
        border = NA)
text(x = c(0.7, 1.9, 3.1),
     y = c(nrow(uni), nrow(d10), nrow(d22)) + 8000,
     labels = format(c(nrow(uni), nrow(d10), nrow(d22)), big.mark = "."),
     cex = 1.1, font = 2)
dev.off()
cat("  reports/figures/nrow_years.png\n")

# ---- 8. Summary stats --------------------------------------------------------
cat("\n[8/10] Summary stats...\n")
summary_stats <- list(
  total_unified = nrow(uni),
  urban_parquet_nrow = nrow(urb),
  rural_parquet_nrow = nrow(rur),
  overlaps_removed   = nrow(urb) + nrow(rur) - nrow(uni),
  n_states           = length(unique(uni$code_state)),
  n_munis            = length(unique(uni$code_muni)),
  n_districts        = length(unique(stats::na.omit(uni$code_district))),
  n_subdistricts     = length(unique(stats::na.omit(uni$code_subdistrict))),
  n_neighborhoods    = length(unique(stats::na.omit(uni$code_neighborhood))),
  zone_counts        = as.list(table(uni$zone, useNA = "ifany")),
  na_name_muni       = sum(is.na(uni$name_muni)),
  pct_name_muni_ok   = round(100 * mean(!is.na(uni$name_muni)), 3),
  crs_epsg           = sf::st_crs(uni)$epsg,
  # Use the snapshotted names (before adding area_km2 later in the script)
  schema_identical_2010 = length(setdiff(orig_uni_names, orig_d10_names)) == 0 &&
                          length(setdiff(orig_d10_names, orig_uni_names)) == 0,
  schema_identical_2022 = length(setdiff(orig_uni_names, orig_d22_names)) == 0 &&
                          length(setdiff(orig_d22_names, orig_uni_names)) == 0,
  d10_nrow = nrow(d10),
  d22_nrow = nrow(d22),
  bbox_unified = as.numeric(sf::st_bbox(uni))
)
saveRDS(summary_stats, file.path(out_art, "summary_stats.rds"))

# Write human-readable summary
summary_txt <- c(
  sprintf("total_unified:         %d",   summary_stats$total_unified),
  sprintf("urban parquet nrow:    %d",   summary_stats$urban_parquet_nrow),
  sprintf("rural parquet nrow:    %d",   summary_stats$rural_parquet_nrow),
  sprintf("overlaps removed:      %d",   summary_stats$overlaps_removed),
  sprintf("n_states:              %d",   summary_stats$n_states),
  sprintf("n_munis:               %d",   summary_stats$n_munis),
  sprintf("n_districts:           %d",   summary_stats$n_districts),
  sprintf("n_subdistricts:        %d",   summary_stats$n_subdistricts),
  sprintf("n_neighborhoods:       %d",   summary_stats$n_neighborhoods),
  sprintf("NA name_muni:          %d (%.3f%% match)",
          summary_stats$na_name_muni, summary_stats$pct_name_muni_ok),
  sprintf("CRS EPSG:              %d",   summary_stats$crs_epsg),
  sprintf("schema == 2010:        %s",   summary_stats$schema_identical_2010),
  sprintf("schema == 2022:        %s",   summary_stats$schema_identical_2022),
  sprintf("2010 nrow:             %d",   summary_stats$d10_nrow),
  sprintf("2022 nrow:             %d",   summary_stats$d22_nrow),
  sprintf("bbox unified xmin,ymin,xmax,ymax: %s",
          paste(round(summary_stats$bbox_unified, 4), collapse = ", "))
)
writeLines(summary_txt, file.path(out_art, "summary_stats.txt"))
cat("\n", paste(summary_txt, collapse = "\n"), "\n", sep = "")

# ---- 9. Decomposition of code_tract empirical validation --------------------
cat("\n[9/10] Code decomposition validation (sample of 5)...\n")
decomp_sample <- uni |>
  sf::st_drop_geometry() |>
  dplyr::slice_sample(n = 5) |>
  dplyr::transmute(
    code_tract,
    code_state_from_tract = as.numeric(substr(sprintf("%015.0f", code_tract), 1, 2)),
    code_muni_from_tract  = as.numeric(substr(sprintf("%015.0f", code_tract), 1, 7)),
    code_state,
    code_muni,
    state_match = code_state_from_tract == code_state,
    muni_match  = code_muni_from_tract  == code_muni
  )
write.csv(decomp_sample, file.path(out_art, "code_decomposition_sample.csv"),
          row.names = FALSE)
print(decomp_sample)

# ---- 10. Forensic investigation of rows still missing hierarchical metadata -
# Some setores exist in the 1:500k rural mesh (published 2002) but not in the
# tabular Censo 2000 (accessed via censobr). These keep name_district /
# name_neighborhood as NA. This block documents them empirically.
cat("\n[10/10] Forensic investigation of rows w/o censobr hierarchical metadata...\n")

# Rows still missing name_district (these are the "truly orphan" 44 rows)
orphans <- uni |>
  sf::st_drop_geometry() |>
  dplyr::filter(is.na(name_district)) |>
  dplyr::mutate(code_tract_char = sprintf("%015.0f", code_tract))

cat("  Orphan rows (missing name_district):", nrow(orphans), "\n")

if (nrow(orphans) > 0) {
  # Raw rural shapefile inspection is optional (requires data-raw cache)
  rur_dir <- "./data-raw/census_tract/2000/shps_rural"
  raw_info <- NULL
  if (dir.exists(rur_dir)) {
    rur_files <- list.files(rur_dir, pattern = "\\.shp$",
                            full.names = TRUE, ignore.case = TRUE)
    if (length(rur_files) > 0) {
      cat("    Reading raw rural shapefiles for context...\n")
      raw_info <- do.call(rbind, lapply(rur_files, function(f) {
        x <- sf::st_read(f, quiet = TRUE, stringsAsFactors = FALSE,
                         options = "ENCODING=IBM437") |>
             sf::st_drop_geometry()
        names(x) <- tolower(names(x))
        x$code_tract_char <- sub("-.*$", "", as.character(x$geocodigo))
        x$shapefile <- basename(f)
        x[, c("geocodigo", "code_tract_char", "situacao", "tipo", "shapefile")]
      }))
    }
  }

  # Build the final missing_44_rows table
  if (!is.null(raw_info)) {
    missing_full <- orphans |>
      dplyr::left_join(
        raw_info |> dplyr::distinct(code_tract_char, .keep_all = TRUE),
        by = "code_tract_char"
      ) |>
      dplyr::transmute(
        code_tract_char,
        code_tract_original = geocodigo,
        is_range_notation   = nchar(geocodigo) == 20,
        code_state, code_muni, name_muni,
        dist_code    = as.integer(substr(code_tract_char, 8, 9)),
        subdist_code = as.integer(substr(code_tract_char, 10, 11)),
        setor_num    = as.integer(substr(code_tract_char, 12, 15)),
        situacao, tipo, shapefile,
        zone, abbrev_state, name_state, name_region
      ) |>
      dplyr::arrange(code_state, code_muni, code_tract_char)
  } else {
    # Without raw access, produce a simpler table
    missing_full <- orphans |>
      dplyr::transmute(
        code_tract_char,
        code_tract_original = NA_character_,
        is_range_notation = NA,
        code_state, code_muni, name_muni,
        dist_code    = as.integer(substr(code_tract_char, 8, 9)),
        subdist_code = as.integer(substr(code_tract_char, 10, 11)),
        setor_num    = as.integer(substr(code_tract_char, 12, 15)),
        situacao = NA, tipo = NA, shapefile = NA,
        zone, abbrev_state, name_state, name_region
      ) |>
      dplyr::arrange(code_state, code_muni, code_tract_char)
  }

  write.csv(missing_full, file.path(out_art, "missing_44_rows.csv"),
            row.names = FALSE)
  cat("  saved:", file.path(out_art, "missing_44_rows.csv"),
      "(", nrow(missing_full), "rows)\n")

  cat("\n  Summary by UF:\n")
  print(missing_full |>
        dplyr::count(abbrev_state, name = "n_missing") |>
        dplyr::arrange(dplyr::desc(n_missing)),
        row.names = FALSE)

  if ("situacao" %in% names(missing_full) && any(!is.na(missing_full$situacao))) {
    cat("\n  Summary by situacao:\n")
    print(missing_full |>
          dplyr::count(situacao, name = "n") |>
          dplyr::arrange(dplyr::desc(n)),
          row.names = FALSE)
  }
}

cat("\n=== Evidence generation complete ===\n")
cat("Artifacts: reports/artifacts/\n")
cat("Figures:   reports/figures/\n")
