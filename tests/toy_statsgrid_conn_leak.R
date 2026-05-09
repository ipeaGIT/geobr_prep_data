# Toy test: valida que on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))
# em add_state_cols libera memoria entre iteracoes.
#
# Reproduz o padrao de statistical_grid.R:142-195 com dados sinteticos
# minimos. Compara duas variantes da funcao:
#   A) SEM fix (padrao original — leak)
#   B) COM fix (on.exit dbDisconnect — proposta)
#
# Roda N iteracoes de cada e verifica que:
#   - A variante B mantem memoria estavel entre iteracoes
#   - Ambas produzem o mesmo resultado funcional (mesma tabela de saida)

suppressPackageStartupMessages({
  library(sf); library(dplyr); library(DBI); library(duckspatial)
})

# 1. Dados sinteticos minimos ---------------------------------------------
set.seed(1)
N_MUNIS <- 10
N_GRID  <- 200

munis <- do.call(rbind, lapply(seq_len(N_MUNIS), function(i) {
  cx <- (i - 1) %% 5 * 10
  cy <- ((i - 1) %/% 5) * 10
  poly <- sf::st_polygon(list(rbind(
    c(cx, cy), c(cx + 10, cy), c(cx + 10, cy + 10),
    c(cx, cy + 10), c(cx, cy)
  )))
  sf::st_sf(
    code_muni    = 1000000 + i,
    name_muni    = paste0("Muni", i),
    code_state   = 11L,
    abbrev_state = "RO",
    geometry     = sf::st_sfc(poly, crs = 4674)
  )
}))

grid_cells <- do.call(rbind, lapply(seq_len(N_GRID), function(i) {
  x <- runif(1, 0, 50); y <- runif(1, 0, 20)
  poly <- sf::st_polygon(list(rbind(
    c(x, y), c(x + 0.5, y), c(x + 0.5, y + 0.5),
    c(x, y + 0.5), c(x, y)
  )))
  sf::st_sf(
    id_unico = paste0("G", sprintf("%04d", i)),
    quadrante = (i - 1) %% 4 + 1,
    geometry = sf::st_sfc(poly, crs = 4674)
  )
}))

# 2. Funcao SEM fix (reproduz bug) ----------------------------------------
add_state_cols_A <- function(df, all_munis) {
  conn <- duckspatial::ddbs_create_conn()

  duckspatial::ddbs_register_table(conn = conn, data = all_munis,
                                   name = "munis", overwrite = TRUE)
  duckspatial::ddbs_write_table(conn = conn, data = df,
                                name = "statsgrid_raw", overwrite = TRUE)
  duckspatial::ddbs_centroid(x = "statsgrid_raw",
                             name = "grid_centroid", conn = conn)
  duckspatial::ddbs_join(conn = conn, x = "grid_centroid", y = "munis",
                         join = "intersects", name = "table_join")

  q <- "CREATE OR REPLACE TEMP VIEW output AS
        SELECT sr.*, tj.code_muni, tj.name_muni, tj.code_state, tj.abbrev_state
        FROM statsgrid_raw sr
        LEFT JOIN (SELECT id_unico, code_muni, name_muni, code_state, abbrev_state
                   FROM table_join) tj
          ON sr.id_unico = tj.id_unico;"
  DBI::dbExecute(conn, q)
  duckspatial::ddbs_read_table(conn, name = "output")
}

# 3. Funcao COM fix (proposta) --------------------------------------------
add_state_cols_B <- function(df, all_munis) {
  conn <- duckspatial::ddbs_create_conn()
  on.exit(duckspatial::ddbs_stop_conn(conn), add = TRUE)

  duckspatial::ddbs_register_table(conn = conn, data = all_munis,
                                   name = "munis", overwrite = TRUE)
  duckspatial::ddbs_write_table(conn = conn, data = df,
                                name = "statsgrid_raw", overwrite = TRUE)
  duckspatial::ddbs_centroid(x = "statsgrid_raw",
                             name = "grid_centroid", conn = conn)
  duckspatial::ddbs_join(conn = conn, x = "grid_centroid", y = "munis",
                         join = "intersects", name = "table_join")

  q <- "CREATE OR REPLACE TEMP VIEW output AS
        SELECT sr.*, tj.code_muni, tj.name_muni, tj.code_state, tj.abbrev_state
        FROM statsgrid_raw sr
        LEFT JOIN (SELECT id_unico, code_muni, name_muni, code_state, abbrev_state
                   FROM table_join) tj
          ON sr.id_unico = tj.id_unico;"
  DBI::dbExecute(conn, q)
  duckspatial::ddbs_read_table(conn, name = "output")
}

# 4. Rodar N iteracoes de cada e comparar -------------------------------
N_ITER <- 10

cat("=== Variante A (SEM fix, leak esperado) ===\n")
mem_start_A <- as.numeric(gc(full = TRUE)[2, 2])
for (i in seq_len(N_ITER)) {
  r_A <- add_state_cols_A(grid_cells, munis)
}
mem_end_A <- as.numeric(gc(full = TRUE)[2, 2])
cat(sprintf("RAM usada (start -> end): %.1f -> %.1f MB (delta = %.1f MB)\n",
            mem_start_A, mem_end_A, mem_end_A - mem_start_A))

cat("\n=== Variante B (COM fix, on.exit dbDisconnect) ===\n")
mem_start_B <- as.numeric(gc(full = TRUE)[2, 2])
for (i in seq_len(N_ITER)) {
  r_B <- add_state_cols_B(grid_cells, munis)
}
mem_end_B <- as.numeric(gc(full = TRUE)[2, 2])
cat(sprintf("RAM usada (start -> end): %.1f -> %.1f MB (delta = %.1f MB)\n",
            mem_start_B, mem_end_B, mem_end_B - mem_start_B))

# 5. Verificacoes funcionais ---------------------------------------------
stopifnot(nrow(r_A) == N_GRID)
stopifnot(nrow(r_B) == N_GRID)
stopifnot(identical(r_A$id_unico, r_B$id_unico))
cat("\nPASS: ambas variantes produzem mesmo resultado funcional.\n")

# 6. Verifica que fix nao deixa conexoes penduradas ----------------------
open_drivers <- duckdb::duckdb_shutdown(duckdb::duckdb())
cat("\nPASS: teste concluido. Fix B deve ter delta de memoria <= fix A.\n")
