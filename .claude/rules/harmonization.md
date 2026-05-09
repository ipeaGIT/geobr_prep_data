# Como usar harmonize_geobr() corretamente

Arquivo: `R/support_harmonize_geobr.R` (linha 1)

## Assinatura completa

```r
harmonize_geobr(
  temp_sf,
  year           = parent.frame()$year, # herdado do escopo pai se omitido
  add_state      = TRUE,
  state_column   = c('name_state', 'code_state'),
  add_region     = TRUE,
  region_column  = c('code_state', 'code_muni'),
  add_snake_case = TRUE,
  snake_colname,          # OBRIGATÓRIO se add_snake_case = TRUE
  projection_fix = TRUE,
  encoding_utf8  = TRUE,
  topology_fix   = TRUE,
  remove_z_dimension = TRUE,
  use_multipolygon   = TRUE
)
```

## Ordem interna de execução

1. `add_state_info()` — adiciona code_state, name_state, abbrev_state
2. `add_region_info()` — adiciona code_region, name_region
3. `snake_case_names()` — title case + preposições minúsculas
4. `harmonize_projection()` — reprojetar para EPSG:4674
5. `use_encoding_utf8()` — converter strings para UTF-8
6. `fix_topology()` — 3 tentativas de correção topológica
7. `sf::st_zm()` — remover dimensão Z
8. `to_multipolygon()` — converter para MULTIPOLYGON + dissolve (**BUG #30**)
9. `temp_sf$year <- year` — adicionar coluna year

**ATENÇÃO linha 75 (comentada):** `normalize_sf_geometry()` não existe → #32

## Exemplos de uso

### Dataset sem hierarquia de estados (biomas, semiárido)
```r
temp_sf <- harmonize_geobr(
  temp_sf        = raw,
  year           = year,
  add_state      = FALSE,
  add_region     = FALSE,
  add_snake_case = TRUE,
  snake_colname  = "name_biome",
  projection_fix = TRUE,
  encoding_utf8  = TRUE,
  topology_fix   = TRUE,
  remove_z_dimension = TRUE,
  use_multipolygon   = TRUE
)
```

### Dataset com code_muni (município, meso, micro)
```r
# Preparar code_state ANTES de chamar (obrigatório se state_column = "code_state")
temp_sf$code_state <- substr(as.character(temp_sf$code_muni), 1, 2) |>
                      as.numeric()

temp_sf <- harmonize_geobr(
  temp_sf        = temp_sf,
  year           = year,
  add_state      = TRUE,
  state_column   = "code_state",
  add_region     = TRUE,
  region_column  = "code_state",
  add_snake_case = FALSE,  # se nome já estiver formatado
  projection_fix = TRUE,
  encoding_utf8  = TRUE,
  topology_fix   = TRUE,
  remove_z_dimension = TRUE,
  use_multipolygon   = TRUE
)
```

### Dados de ponto (health_facilities, schools)
```r
# use_multipolygon = FALSE — pontos não têm dissolve
temp_sf <- harmonize_geobr(
  temp_sf        = raw,
  year           = year,
  add_state      = TRUE,
  state_column   = "code_state",
  add_region     = TRUE,
  region_column  = "code_state",
  add_snake_case = FALSE,
  projection_fix = TRUE,
  encoding_utf8  = TRUE,
  topology_fix   = FALSE,   # pontos não têm topologia
  remove_z_dimension = TRUE,
  use_multipolygon   = FALSE
)
```

## BUG #30 — to_multipolygon() apaga colunas

`to_multipolygon()` em `support_harmonize_geobr.R:315-321` filtra `col_names`
para colunas com prefixo `code_` ou `name_` antes do `group_by/summarise`.
Colunas como `area_km2`, `abbrev_state` (se adicionada antes), etc. são perdidas.

**Workaround A** — desligar e converter manualmente:
```r
temp_sf <- harmonize_geobr(..., use_multipolygon = FALSE)
temp_sf <- sf::st_cast(temp_sf, "MULTIPOLYGON")
```

**Workaround B** — salvar extras antes, juntar depois:
```r
extra <- sf::st_drop_geometry(temp_sf) |> dplyr::select(code_muni, col_extra)
temp_sf <- harmonize_geobr(..., use_multipolygon = TRUE)
temp_sf <- dplyr::left_join(temp_sf, extra, by = "code_muni")
```

## fix_topology() — como funciona

Três tentativas em cascata:
1. `sf::st_make_valid()` com s2 ativado
2. `lwgeom::lwgeom_make_valid()` para os ainda inválidos
3. Rebuild via `st_boundary() → st_node() → st_polygonize()` em EPSG:5880

Se todas falharem: `stop("Topology errors could not be fixed.")`.

## simplify_temp_sf() — uso correto

```r
# SEMPRE chamar APÓS harmonize_geobr(), nunca antes
temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)
# tolerance = 100m (reprojeção interna para EPSG:3857)
# Para dados em escala pequena (biomas, país): tolerance = 500
```

## state_column deve existir ANTES de chamar harmonize_geobr()

`add_state_info()` faz `stop()` se a coluna não existir. Preparar antes:

```r
# Para município (code_muni de 7 dígitos):
temp_sf$code_state <- substr(as.character(temp_sf$code_muni), 1, 2) |>
                      as.numeric()

# Para meso/microrregião (code de 4-7 dígitos):
temp_sf$code_state <- substr(as.character(temp_sf$code_meso), 1, 2) |>
                      as.numeric()
```
