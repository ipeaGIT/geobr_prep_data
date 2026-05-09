# Plano: Bugs de Infraestrutura

**Status**: CONCLUÍDO (2026-03-30)

Correções que afetam múltiplos datasets. Aplicar antes de trabalhar em datasets
individuais para evitar que os bugs se propaguem.

## BUG-001 — normalize_sf_geometry() (issue #32)

**Arquivo:** `R/support_harmonize_geobr.R:75`
**Impacto:** Baixo (comentada), mas bloqueia issue #32.

**Fix — 2 alterações no mesmo arquivo:**

1. Adicionar a função (sugestão: após a função `harmonize_geobr`, por volta da linha 100):
```r
normalize_sf_geometry <- function(temp_sf) {
  if (attr(temp_sf, "sf_column") != "geometry") {
    temp_sf <- sf::st_rename_geometry(temp_sf, "geometry")
  }
  return(temp_sf)
}
```

2. Descomentar linha 75:
```r
temp_sf <- normalize_sf_geometry(temp_sf)
```

**Teste:**
```r
source("./R/support_harmonize_geobr.R")
# Criar sf com coluna geometry nomeada diferente:
test_sf <- sf::st_sf(a = 1, geom = sf::st_sfc(sf::st_point(c(0,0))))
result <- normalize_sf_geometry(test_sf)
attr(result, "sf_column")  # deve retornar "geometry"
```

---

## BUG-002 — to_multipolygon() apaga colunas (issue #30)

**Arquivo:** `R/support_harmonize_geobr.R:315-321`
**Impacto:** Alto — colunas não-code/name são perdidas silenciosamente.

**Código atual (problemático):**
```r
col_names <- names(temp_sf)
col_names <- col_names[!col_names %like% 'geometry|geom']
col_names <- col_names[col_names %like% "code_|name_"]  # ← filtro excessivo
temp_sf <- temp_sf |>
  group_by(across(all_of(col_names))) |>
  summarise(geometry = sf::st_union(geometry)) |>
  ungroup()
```

**Fix proposto — remover o segundo filtro:**
```r
col_names <- names(temp_sf)
col_names <- col_names[!col_names %like% 'geometry|geom']
# Remover filtro "code_|name_" para preservar todas as colunas
temp_sf <- temp_sf |>
  group_by(across(all_of(col_names))) |>
  summarise(geometry = sf::st_union(geometry)) |>
  ungroup()
```

**Teste:**
```r
# Dataset com coluna extra
test_sf <- sf::st_read("./data/biomes/2019/biomes_2019.parquet") |>
           sf::st_as_sf()
test_sf$extra_col <- "teste"
result <- harmonize_geobr(test_sf, year = 2019,
                          add_state = FALSE, add_region = FALSE,
                          add_snake_case = FALSE,
                          use_multipolygon = TRUE)
"extra_col" %in% names(result)  # deve ser TRUE após o fix
```

---

## BUG-003 — tidyselect all_of() em biomes.R (sem issue)

**Arquivo:** `R/biomes.R:142-143`

**Código atual:**
```r
biomes_raw |>
  select('name_biome' = all_of(snake_colname),  # 666
         'code_biome' = all_of(id_colname),     # 666
         year, geometry)
```

**Fix:**
```r
biomes_raw |>
  dplyr::rename(name_biome = all_of(snake_colname),
                code_biome = all_of(id_colname)) |>
  dplyr::select(name_biome, code_biome, year, geometry)
```

---

## BUG-006 — amazonia_legal em all_files (CRÍTICO)

**Arquivo:** `_targets.R:640`

**Fix imediato:**
```r
# Comentar linha 640:
# amazonialegal_clean, #02
```

**Fix definitivo:** Implementar o dataset completo.
Ver `.claude/plans/amazonia_legal.md`.

---

## BUG-007 — schools em all_files (CRÍTICO)

**Arquivo:** `_targets.R:649`

**Fix imediato:**
```r
# Comentar linha 649:
# schools_clean, #09
```

---

## Ordem de aplicação sugerida

1. **BUG-006 + BUG-007** (críticos) — comentar linhas em `_targets.R` para
   que `tar_make()` funcione sem erros imediatos.

2. **BUG-003** (simples) — 2 linhas em `biomes.R`, sem risco.

3. **BUG-001** — adicionar função e descomentar linha; testar isoladamente.

4. **BUG-002** — mais arriscado (afeta comportamento de `harmonize_geobr`);
   testar com vários datasets antes de confirmar que não quebra nada.

## Verificação final após todos os fixes

```r
targets::tar_make()
targets::tar_meta(fields = warnings, complete_only = TRUE)
# Não deve haver erros nos datasets ativos
```
