# Plano: amazonia_legal (issue #2)

**Status**: CONCLUÍDO (2026-03-29)

## Problema atual

1. Fonte original para 2012 (`http://mapas.mma.gov.br/ms_tmp/amazlegal.*`) offline
2. `_targets.R:640` referencia `amazonialegal_clean` mas target está comentado → BUG-006
3. Erro "object not found" ao rodar `tar_make()` na máquina atual

## Fontes disponíveis (IBGE FTP)

```
https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/amazonia_legal/
```

| Ano  | Arquivo                                                  | Formato         |
|------|----------------------------------------------------------|-----------------|
| 2012 | Sem alternativa imediata                                 | OFFLINE         |
| 2019 | `2019/lista_de_municipios_da_amazonia_legal_2019_SHP.zip`| Municípios (dissolve necessário) |
| 2020 | `2020/lista_de_municipios_da_Amazonia_Legal_2020_SHP.zip`| Municípios (dissolve necessário) |
| 2021 | `2021/Limites_Amazonia_Legal_2021_shp.zip`               | Polígono único  |
| 2022 | `2022/Limites_Amazonia_Legal_2022_shp.zip`               | Polígono único  |
| 2024 | `2024/Limites_Amazonia_Legal_2024_shp.zip`               | Polígono único  |

## Plano de implementação (6 passos)

### Passo 1 — Correção imediata do BUG-006

Em `_targets.R:640`, comentar a linha:
```r
# amazonialegal_clean, #02
```
Isso permite rodar `tar_make()` sem erro enquanto o fix completo é preparado.

### Passo 2 — Atualizar years_amazon em _targets.R

```r
# Descomentar e atualizar o bloco (linhas 110-125):
tar_target(name = years_amazon,
           command = c(2019, 2020, 2021, 2022, 2024)),
           # 2012 removido: fonte MMA offline sem alternativa
```

### Passo 3 — Atualizar download_amazonialegal() em R/amazonia_legal.R

```r
download_amazonialegal <- function(year) {

  base <- "https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/amazonia_legal/"

  ftp_link <- switch(as.character(year),
    "2019" = paste0(base, "2019/lista_de_municipios_da_amazonia_legal_2019_SHP.zip"),
    "2020" = paste0(base, "2020/lista_de_municipios_da_Amazonia_Legal_2020_SHP.zip"),
    "2021" = paste0(base, "2021/Limites_Amazonia_Legal_2021_shp.zip"),
    "2022" = paste0(base, "2022/Limites_Amazonia_Legal_2022_shp.zip"),
    "2024" = paste0(base, "2024/Limites_Amazonia_Legal_2024_shp.zip"),
    stop(paste("Ano", year, "não suportado para amazonia_legal"))
  )

  # ... manter lógica de download existente
}
```

### Passo 4 — Atualizar clean_amazonialegal() em R/amazonia_legal.R

2019/2020 são listas de municípios → precisam de dissolve.
2021+ são polígonos únicos da Amazônia Legal.

```r
clean_amazonialegal <- function(amazonialegal_raw, year) {

  dir_clean <- paste0("./data/amazonia_legal/", year)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  # Para 2019/2020: dissolver municípios em polígono único
  if (year %in% c(2019, 2020)) {
    temp_sf <- amazonialegal_raw |>
      sf::st_union() |>
      sf::st_as_sf()
    sf::st_geometry(temp_sf) <- "geometry"
  } else {
    temp_sf <- amazonialegal_raw
  }

  temp_sf <- harmonize_geobr(
    temp_sf        = temp_sf,
    year           = year,
    add_state      = FALSE,
    add_region     = FALSE,
    add_snake_case = FALSE,
    projection_fix = TRUE,
    encoding_utf8  = TRUE,
    topology_fix   = TRUE,
    remove_z_dimension = TRUE,
    use_multipolygon   = TRUE
  )

  temp_sf <- temp_sf |> dplyr::select(year, geometry)

  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 500)

  arrow::write_parquet(temp_sf,
    sink = paste0(dir_clean, "/amazonialegal_", year, ".parquet"),
    compression = 'zstd', compression_level = 7)

  arrow::write_parquet(temp_sf_simplified,
    sink = paste0(dir_clean, "/amazonialegal_", year, "_simplified.parquet"),
    compression = 'zstd', compression_level = 7)

  files <- list.files(dir_clean, pattern = ".parquet",
                      full.names = TRUE, recursive = TRUE)
  return(files)
}
```

### Passo 5 — Verificar nomes de colunas por ano

Antes de finalizar, inspecionar as colunas retornadas por `download_amazonialegal()`
para cada ano:
```r
# Rodar interativamente para verificar:
raw_2019 <- download_amazonialegal(2019)
names(raw_2019)
raw_2021 <- download_amazonialegal(2021)
names(raw_2021)
```

### Passo 6 — Descomentar em all_files e testar

```r
# Descomentar em _targets.R:640:
amazonialegal_clean, #02

# Rodar:
targets::tar_make(names = c("amazonialegal_raw", "amazonialegal_clean"))
```

## Riscos
- IBGE pode mudar nomes de arquivos no FTP sem aviso
- Para 2012: verificar se há arquivo estático disponível no IBGE ou MMA archive
- Colunas de 2019/2020 (lista de municípios) podem precisar de tratamento adicional
