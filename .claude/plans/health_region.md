# Plano: health_region (issue #23)

**Status**: CONCLUÍDO (2026-03-31)

## Bugs críticos no script atual (prep_health_region.R)

| Bug    | Linha | Problema                                           |
|--------|-------|----------------------------------------------------|
| BUG-008| 624   | Usa `maptools` removido do CRAN (anos 1991-2005)   |
| BUG-009| 120, 277 | Função `clean_healthregions()` definida 2x     |
| BUG-010| 124   | `dir_clean` aponta para `data/intermediate_regions/` |
| Estrutura | 232 | Target comentado em `_targets.R`                 |

## Fonte de dados

| Anos         | Fonte                                             | Formato       |
|--------------|---------------------------------------------------|---------------|
| 1991-2005    | FTP DATASUS (formato .MAP binário proprietário)   | .MAP (ilegível sem maptools) |
| 2013         | FTP DATASUS: `ftp://ftp.datasus.gov.br/territorio/mapas/` | .SHP em ZIP |

## Colunas esperadas no output final

```
code_health_region  numeric  código da região de saúde (7 dígitos IBGE)
name_health_region  character nome
code_state          numeric  código do estado (2 dígitos)
abbrev_state        character sigla do estado
name_state          character nome do estado
code_region         numeric  código da grande região
name_region         character nome da grande região
year                numeric
geometry            sfc_MULTIPOLYGON
```

## Plano de implementação

### Passo 1 — Corrigir BUG-010 (dir_clean)

Em `R/prep_health_region.R:124`:
```r
# De:
dir_clean <- paste0("./data/intermediate_regions/", year)
# Para:
dir_clean <- paste0("./data/health_region/", year)
```

### Passo 2 — Corrigir BUG-009 (função duplicada)

Remover a definição duplicada da linha 120. Manter apenas a versão da linha 277
(usa `compression_level = 7`).

### Passo 3 — Estratégia para anos históricos (1991-2005)

**Opção A (recomendada):** Implementar apenas 2013, que usa SHP normal:
```r
tar_target(name = years_healthregion, command = c(2013))
```
Dados históricos de 1991-2005 requerem solução separada (ver Opção B).

**Opção B (futura):** Para 1991-2005, verificar se há:
1. Dados pré-processados em .gpkg no repositório do geobr
2. Alternativa via IBGE (regiões de saúde históricas em shapefile)
3. Reconstrução manual a partir de tabelas de municípios + join com `read_municipality()`

### Passo 4 — Implementar download_healthregions() para 2013

```r
download_healthregions <- function(year) {

  # Apenas 2013 disponível com SHP normal do DATASUS FTP
  ftp_link <- switch(as.character(year),
    "2013" = "ftp://ftp.datasus.gov.br/territorio/mapas/todos_mapas_2013.zip",
    stop(paste("Ano", year, "não suportado. Anos históricos 1991-2005 requerem maptools."))
  )

  zip_dir <- paste0(tempdir(), "/health_region/", year)
  in_zip  <- paste0(zip_dir, "/zipped/")
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(in_zip,  recursive = TRUE, showWarnings = FALSE)
  dir.create(out_zip, recursive = TRUE, showWarnings = FALSE)

  file_raw <- fs::file_temp(tmp_dir = in_zip, ext = "zip")
  httr::GET(url = ftp_link, httr::progress(),
            httr::write_disk(path = file_raw, overwrite = TRUE))

  unzip_geobr(zip_dir = zip_dir, in_zip = in_zip,
              out_zip = out_zip, is_shp = TRUE)

  # Ler apenas o SHP de regiões de saúde (filtrar entre vários no ZIP)
  shp_files <- list.files(out_zip, pattern = "(?i)rgsaud|regsaude|saude",
                          full.names = TRUE, recursive = TRUE)
  if (length(shp_files) == 0) {
    shp_files <- list.files(out_zip, pattern = "\\.shp$",
                            full.names = TRUE, recursive = TRUE)
  }

  raw <- sf::st_read(shp_files[1], quiet = TRUE, stringsAsFactors = FALSE,
                     options = "ENCODING=WINDOWS-1252")

  glimpse(raw)
  return(raw)
}
```

### Passo 5 — Implementar clean_healthregions() corrigida

```r
clean_healthregions <- function(healthregions_raw, year) {

  dir_clean <- paste0("./data/health_region/", year)  # BUG-010 corrigido
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  # Inspecionar colunas antes de padronizar:
  # names(healthregions_raw)

  temp_sf <- healthregions_raw |>
    dplyr::rename(
      code_health_region = <coluna_codigo>,  # verificar nome real
      name_health_region = <coluna_nome>     # verificar nome real
    ) |>
    dplyr::select(code_health_region, name_health_region, geometry)

  temp_sf$code_state <- substr(as.character(temp_sf$code_health_region), 1, 2) |>
                        as.numeric()

  temp_sf <- harmonize_geobr(
    temp_sf        = temp_sf,
    year           = year,
    add_state      = TRUE,
    state_column   = "code_state",
    add_region     = TRUE,
    region_column  = "code_state",
    add_snake_case = TRUE,
    snake_colname  = "name_health_region",
    projection_fix = TRUE,
    encoding_utf8  = TRUE,
    topology_fix   = TRUE,
    remove_z_dimension = TRUE,
    use_multipolygon   = TRUE
  )

  temp_sf <- temp_sf |>
    dplyr::select(code_health_region, name_health_region,
                  code_state, abbrev_state, name_state,
                  code_region, name_region, year, geometry)

  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)

  arrow::write_parquet(temp_sf,
    sink = paste0(dir_clean, "/health_region_", year, ".parquet"),
    compression = 'zstd', compression_level = 7)

  arrow::write_parquet(temp_sf_simplified,
    sink = paste0(dir_clean, "/health_region_", year, "_simplified.parquet"),
    compression = 'zstd', compression_level = 7)

  files <- list.files(dir_clean, pattern = ".parquet",
                      full.names = TRUE, recursive = TRUE)
  return(files)
}
```

### Passo 6 — Estrutura de targets em _targets.R

```r
#23. Regiões de Saúde -----------------------------------------------

  tar_target(name = years_healthregion,
             command = c(2013)),
             # Anos 1991-2005 dependem de maptools (removido do CRAN)

  tar_target(name = healthregion_raw,
             command = download_healthregions(years_healthregion),
             pattern = map(years_healthregion)),

  tar_target(name = healthregion_clean,
             command = clean_healthregions(healthregion_raw, years_healthregion),
             pattern = map(healthregion_raw, years_healthregion),
             format = 'file'),
```

E adicionar `healthregion_clean` no bloco `all_files`.

## Riscos
- FTP DATASUS pode estar fora do ar ou ter estrutura de ZIP diferente
- Nome exato do arquivo .shp dentro do ZIP de 2013 precisa ser verificado
- Para anos históricos 1991-2005: solução depende de dados alternativos
