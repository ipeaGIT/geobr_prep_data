# reports/investigation/phase_b.R
#
# Phase B: deeper analysis + healing strategy tests.
# Requires phase_a.R to have been run first (needs the artifacts).
#
# Usage:
#   "/c/Program Files/R/R-4.5.0/bin/Rscript.exe" reports/investigation/phase_b.R

setwd("d:/Dropbox/Artigos/geobr_prep_data")

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(arrow)
  library(geoarrow)
})
source("R/support_harmonize_geobr.R")

out_art <- "reports/artifacts/investigation"
out_fig <- "reports/figures/investigation"
dir.create(out_art, recursive = TRUE, showWarnings = FALSE)
dir.create(out_fig, recursive = TRUE, showWarnings = FALSE)

log <- function(...) cat(sprintf(...), "\n")

sf::sf_use_s2(FALSE)  # stable operations for complex polygons

log("===== PHASE B: DEEPER ANALYSIS + HEALING TESTS =====\n")

# Load parquets
uni <- read_geoparquet("data/census_tract/2000/census_tracts_2000.parquet")
muni_2000 <- read_geoparquet("data/municipality/2000/municipalities_2000.parquet")

# =============================================================================
# B1 — Coverage per UF (tileamento score)
# =============================================================================

log("\n\n===== B1: Coverage per UF =====\n")

log("[B1.1] Loading states 2000 envelopes...")
states_path <- "data/state/2000/state_2000.parquet"
if (!file.exists(states_path)) {
  # try alternative names
  alt <- list.files("data", pattern = "state.*2000.*parquet$",
                    recursive = TRUE, full.names = TRUE)[1]
  if (!is.na(alt)) states_path <- alt
}
if (file.exists(states_path)) {
  states_sf <- read_geoparquet(states_path)
  log("  state_2000: %d rows", nrow(states_sf))
} else {
  log("  state_2000 not found, using union of municipalities as envelope")
  states_sf <- muni_2000 |>
    dplyr::group_by(code_state, abbrev_state, name_state) |>
    dplyr::summarise(.groups = "drop")
}

log("[B1.2] Computing coverage per UF (slow, may take a few min)...")
uf_list <- sort(unique(uni$code_state))
coverage_rows <- list()

for (uf in uf_list) {
  uf_uni <- uni |> dplyr::filter(code_state == uf)
  uf_env <- states_sf |> dplyr::filter(code_state == uf)
  if (nrow(uf_env) == 0) next

  env_area <- sum(as.numeric(sf::st_area(uf_env))) / 1e6
  setores_area <- sum(as.numeric(sf::st_area(uf_uni))) / 1e6
  coverage_pct <- 100 * setores_area / env_area
  gap_pct <- 100 - coverage_pct

  abbrev <- unique(uf_uni$abbrev_state)[1]
  log("  UF %s (%s): %d setores, env=%.0f km2, setores=%.0f km2, coverage=%.2f%%",
      uf, abbrev, nrow(uf_uni), env_area, setores_area, coverage_pct)

  coverage_rows[[as.character(uf)]] <- data.frame(
    code_state = uf,
    abbrev_state = abbrev,
    n_setores = nrow(uf_uni),
    env_area_km2 = round(env_area, 2),
    setores_area_km2 = round(setores_area, 2),
    coverage_pct = round(coverage_pct, 3),
    gap_pct = round(gap_pct, 3)
  )
}

coverage_df <- do.call(rbind, coverage_rows)
coverage_df <- coverage_df[order(coverage_df$gap_pct, decreasing = TRUE), ]
write.csv(coverage_df, file.path(out_art, "coverage_per_uf.csv"),
          row.names = FALSE)
log("\n  Saved coverage_per_uf.csv")
log("\n  Top 5 UFs by gap percentage:")
print(head(coverage_df, 5))
log("\n  Top 5 UFs by coverage (best):")
print(tail(coverage_df, 5))

# =============================================================================
# B2 — Test healing strategies in AC
# =============================================================================

log("\n\n===== B2: Test healing strategies in AC =====\n")

log("[B2.1] Loading AC data (reproject to EPSG:5880 for metric snap)...")
ac_uni <- uni |>
  dplyr::filter(code_state == 12) |>
  sf::st_transform(5880)
ac_env <- muni_2000 |>
  dplyr::filter(code_state == 12) |>
  sf::st_transform(5880) |>
  sf::st_union() |>
  sf::st_make_valid()
ac_env_area <- as.numeric(sf::st_area(ac_env)) / 1e6
log("  AC: %d setores, env area %.0f km2", nrow(ac_uni), ac_env_area)

measure_gap <- function(sf_obj, envelope, label) {
  t0 <- Sys.time()
  u <- sf::st_union(sf::st_make_valid(sf_obj))
  gap <- sf::st_difference(envelope, u)
  gap_area <- sum(as.numeric(sf::st_area(gap))) / 1e6
  t1 <- Sys.time()
  data.frame(
    strategy = label,
    n_rows = nrow(sf_obj),
    gap_km2 = round(gap_area, 4),
    gap_pct = round(100 * gap_area / ac_env_area, 4),
    time_s = round(as.numeric(difftime(t1, t0, units = "secs")), 1)
  )
}

log("[B2.2] Baseline (no treatment)...")
res <- list()
res[["baseline"]] <- measure_gap(ac_uni, ac_env, "baseline")
log("  gap: %.4f km2 (%.4f%%)",
    res[["baseline"]]$gap_km2, res[["baseline"]]$gap_pct)

log("[B2.3] Buffer-unbuffer (±1 m in EPSG:5880)...")
buf1 <- ac_uni
buf1$geometry <- sf::st_buffer(sf::st_make_valid(buf1$geometry), 1)
buf1$geometry <- sf::st_buffer(buf1$geometry, -1)
res[["buffer_plus_minus_1m"]] <- measure_gap(buf1, ac_env, "buffer_plus_minus_1m")
log("  gap: %.4f km2", res[["buffer_plus_minus_1m"]]$gap_km2)

log("[B2.4] st_snap to self (tol = 10 m)...")
snapped <- ac_uni
snapped$geometry <- sf::st_snap(
  sf::st_make_valid(snapped$geometry),
  sf::st_union(sf::st_make_valid(snapped$geometry)),
  tolerance = 10
)
res[["snap_to_self_10m"]] <- measure_gap(snapped, ac_env, "snap_to_self_10m")
log("  gap: %.4f km2", res[["snap_to_self_10m"]]$gap_km2)

log("[B2.5] st_snap to envelope (tol = 100 m)...")
snap_env <- ac_uni
snap_env$geometry <- sf::st_snap(
  sf::st_make_valid(snap_env$geometry),
  ac_env,
  tolerance = 100
)
res[["snap_to_envelope_100m"]] <- measure_gap(snap_env, ac_env, "snap_to_envelope_100m")
log("  gap: %.4f km2", res[["snap_to_envelope_100m"]]$gap_km2)

log("[B2.6] Cascade: make_valid + buffer +5m + -5m...")
cascade <- ac_uni
cascade$geometry <- sf::st_make_valid(cascade$geometry)
cascade$geometry <- sf::st_buffer(cascade$geometry, 5)
cascade$geometry <- sf::st_buffer(cascade$geometry, -5)
cascade$geometry <- sf::st_make_valid(cascade$geometry)
res[["cascade_buffer_5m"]] <- measure_gap(cascade, ac_env, "cascade_buffer_5m")
log("  gap: %.4f km2", res[["cascade_buffer_5m"]]$gap_km2)

log("[B2.7] Grow setores by 50 m (absorb small slivers)...")
grown <- ac_uni
grown$geometry <- sf::st_buffer(sf::st_make_valid(grown$geometry), 50)
res[["buffer_50m_grow"]] <- measure_gap(grown, ac_env, "buffer_50m_grow")
log("  gap: %.4f km2", res[["buffer_50m_grow"]]$gap_km2)

strategies_df <- do.call(rbind, res)
write.csv(strategies_df, file.path(out_art, "snap_strategies_comparison.csv"),
          row.names = FALSE)
log("\n  Saved snap_strategies_comparison.csv:")
print(strategies_df)

# Generate before-after figure
log("[B2.8] Generating snap_ac_before_after.png...")
best_strategy_idx <- which.min(strategies_df$gap_pct)
best_label <- strategies_df$strategy[best_strategy_idx]

png(file.path(out_fig, "snap_ac_before_after.png"),
    width = 1400, height = 700, res = 130)
par(mfrow = c(1, 2), mar = c(1, 1, 3, 1))
# Baseline
plot(sf::st_geometry(ac_env), col = "#ffffff", border = "#666666",
     main = sprintf("Baseline\ngap = %.4f km2", res[["baseline"]]$gap_km2))
plot(sf::st_geometry(ac_uni), col = "#aed6f1",
     border = "#00000020", add = TRUE)
# Best
plot(sf::st_geometry(ac_env), col = "#ffffff", border = "#666666",
     main = sprintf("Best: %s\ngap = %.4f km2",
                    best_label, strategies_df$gap_km2[best_strategy_idx]))
plot(sf::st_geometry(ac_uni), col = "#aed6f1",
     border = "#00000020", add = TRUE)
dev.off()
log("  Saved")

# =============================================================================
# B3 — Viability: recover from 2010 (for large gaps)
# =============================================================================

log("\n\n===== B3: Recoverability from 2010 =====\n")

# We already know SP has 3 large gaps from Phase A. This step extends to:
# - Count large gaps across ALL UFs
# - Estimate how many setores_2010 could "fill" them

log("[B3.1] Loading 2010 data...")
d10 <- read_geoparquet("data/census_tract/2010/census_tracts_2010.parquet")
log("  2010: %d rows", nrow(d10))

# For each UF, count large gaps and intersecting 2010 setores
log("[B3.2] Quantifying large gaps across all UFs...")
recoverable_rows <- list()
for (uf in uf_list) {
  uf_uni <- uni |> dplyr::filter(code_state == uf)
  uf_d10 <- d10 |> dplyr::filter(code_state == uf)
  uf_env <- muni_2000 |>
    dplyr::filter(code_state == uf) |>
    sf::st_union() |>
    sf::st_make_valid()
  if (length(uf_env) == 0) next

  u_uni <- sf::st_union(sf::st_make_valid(uf_uni))
  gap <- sf::st_difference(uf_env, u_uni)
  gap_poly <- sf::st_cast(gap, "POLYGON", warn = FALSE)
  if (length(gap_poly) == 0) next
  gap_sf <- sf::st_sf(geometry = gap_poly)
  gap_sf$area_km2 <- as.numeric(sf::st_area(gap_sf)) / 1e6
  large_gaps <- gap_sf |> dplyr::filter(area_km2 > 1)

  if (nrow(large_gaps) > 0) {
    # Count 2010 setores that fall in the large gaps
    hits <- sf::st_intersects(uf_d10, large_gaps, sparse = FALSE)
    n_recoverable <- sum(rowSums(hits) > 0)
  } else {
    n_recoverable <- 0
  }
  abbrev <- unique(uf_uni$abbrev_state)[1]
  log("  UF %s (%s): %d large gaps, %d recoverable setores from 2010",
      uf, abbrev, nrow(large_gaps), n_recoverable)

  recoverable_rows[[as.character(uf)]] <- data.frame(
    code_state = uf,
    abbrev_state = abbrev,
    n_large_gaps = nrow(large_gaps),
    total_large_gap_area_km2 = round(sum(large_gaps$area_km2), 2),
    n_recoverable_2010_setores = n_recoverable
  )
}
recoverable_df <- do.call(rbind, recoverable_rows)
recoverable_df <- recoverable_df[order(recoverable_df$n_recoverable_2010_setores,
                                        decreasing = TRUE), ]
write.csv(recoverable_df, file.path(out_art, "recoverable_from_2010.csv"),
          row.names = FALSE)

log("\n  Saved recoverable_from_2010.csv")
log("\n  Top 10 UFs by recoverable setores:")
print(head(recoverable_df, 10))
log("\n  Total large gaps across all UFs: %d",
    sum(recoverable_df$n_large_gaps))
log("  Total large gap area: %.1f km2",
    sum(recoverable_df$total_large_gap_area_km2))
log("  Total recoverable 2010 setores: %d",
    sum(recoverable_df$n_recoverable_2010_setores))

log("\n===== PHASE B COMPLETE =====")
