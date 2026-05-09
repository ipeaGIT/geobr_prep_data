# Checklist: Implementar um Novo Dataset

Usar este template ao criar ou migrar um dataset para o pipeline `targets`.

## Etapa 0 — Pesquisa

- [ ] Identificar a fonte oficial (IBGE, DATASUS, MMA, FUNAI, etc.)
- [ ] Verificar se a URL está ativa: `httr::HEAD(url)$status_code == 200`
- [ ] Documentar os anos disponíveis
- [ ] Verificar encoding do shapefile por ano (IBM437 / WINDOWS-1252 / UTF-8)
- [ ] Identificar os nomes das colunas por ano (costumam mudar!)
- [ ] Checar se há script legado em `ainda_sem_targets/` para reaproveitar

## Etapa 1 — Criar R/[dataset].R

```r
#> DATASET: [nome]
#> Source: [URL base]
#> Anos disponíveis: ...
#> Encoding: ...

download_[dataset] <- function(year) {

  ## URLs por ano
  ftp_link <- switch(as.character(year),
    "2022" = "https://...",
    "2010" = "https://...",
    stop(paste("Ano", year, "não suportado"))
  )

  ## Criar diretórios
  zip_dir <- paste0(tempdir(), "/[dataset]/", year)
  in_zip  <- paste0(zip_dir, "/zipped/")
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(in_zip,  recursive = TRUE, showWarnings = FALSE)
  dir.create(out_zip, recursive = TRUE, showWarnings = FALSE)

  ## Download
  encode <- dplyr::case_when(
    year == 2000 ~ "IBM437",
    year <= 2010 ~ "WINDOWS-1252",
    TRUE         ~ "UTF-8"
  )
  file_raw <- fs::file_temp(tmp_dir = in_zip, ext = "zip")
  httr::GET(url = ftp_link, httr::progress(),
            httr::write_disk(path = file_raw, overwrite = TRUE))

  ## Unzip e ler
  unzip_geobr(zip_dir = zip_dir, in_zip = in_zip,
              out_zip = out_zip, is_shp = TRUE)

  raw <- sf::st_read(out_zip, quiet = TRUE, stringsAsFactors = FALSE,
                     options = paste0("ENCODING=", encode))

  glimpse(raw)
  return(raw)
}

clean_[dataset] <- function(raw, year) {

  dir_clean <- paste0("./data/[dataset]/", year)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  ## Renomear colunas para padrão geobr
  temp_sf <- raw |>
    dplyr::rename(code_X = col_origem, name_X = outro_col) |>
    dplyr::select(code_X, name_X, geometry)

  ## Harmonizar
  temp_sf <- harmonize_geobr(
    temp_sf        = temp_sf,
    year           = year,
    add_state      = TRUE,
    state_column   = "code_state",
    add_region     = TRUE,
    region_column  = "code_state",
    add_snake_case = TRUE,
    snake_colname  = "name_X",
    projection_fix = TRUE,
    encoding_utf8  = TRUE,
    topology_fix   = TRUE,
    remove_z_dimension = TRUE,
    use_multipolygon   = TRUE
  )

  ## Reordenar colunas (geometry SEMPRE por último)
  temp_sf <- temp_sf |>
    dplyr::select(code_X, name_X,
                  code_state, abbrev_state, name_state,
                  code_region, name_region,
                  year, geometry)

  ## Verificar integridade
  stopifnot(sf::st_crs(temp_sf)$epsg == 4674)
  stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
  stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")

  ## Versão simplificada
  temp_sf_simplified <- simplify_temp_sf(temp_sf, tolerance = 100)

  ## Salvar
  arrow::write_parquet(temp_sf,
    sink = paste0(dir_clean, "/[dataset]_", year, ".parquet"),
    compression = 'zstd', compression_level = 7)

  arrow::write_parquet(temp_sf_simplified,
    sink = paste0(dir_clean, "/[dataset]_", year, "_simplified.parquet"),
    compression = 'zstd', compression_level = 7)

  files <- list.files(dir_clean, pattern = ".parquet",
                      full.names = TRUE, recursive = TRUE)
  return(files)
}
```

## Etapa 2 — Adicionar targets em _targets.R

```r
#XX. [Nome do dataset] ---------------------------------------------------

  tar_target(name = years_[dataset],
             command = c([anos])),

  tar_target(name = [dataset]_raw,
             command = download_[dataset](years_[dataset]),
             pattern = map(years_[dataset])),

  tar_target(name = [dataset]_clean,
             command = clean_[dataset]([dataset]_raw, years_[dataset]),
             pattern = map([dataset]_raw, years_[dataset]),
             format = 'file'),
```

## Etapa 3 — Adicionar em all_files (_targets.R bloco END)

```r
tar_target(name = all_files,
           command = c(
             ...,
             [dataset]_clean  #XX
           ))
```

**ATENÇÃO:** Se o target for comentado depois, comentar TAMBÉM em `all_files`.
Targets fantasmas em `all_files` quebram `tar_make()`.

## Etapa 4 — Validação pós tar_make()

```r
f <- list.files("./data/[dataset]/", full.names = TRUE, recursive = TRUE,
                pattern = ".parquet")
ds <- arrow::open_dataset(f[1]) |> dplyr::collect() |> sf::st_as_sf()
names(ds)                        # colunas e ordem correta?
sf::st_crs(ds)$epsg              # deve ser 4674
unique(sf::st_geometry_type(ds)) # deve ser MULTIPOLYGON
nrow(ds)                         # número de feições plausível?
# glimpse_geobr(ds)              # diagnóstico completo
```

## Etapa 5 — Datasets sem parâmetro year

Para datasets sem variação temporal (ex: `disaster_risk_areas`):

```r
tar_target(name = [dataset]_raw,
           command = download_[dataset]()),

tar_target(name = [dataset]_clean,
           command = clean_[dataset]([dataset]_raw),
           format = 'file'),
```

E nas funções, remover o parâmetro `year` e usar ano fixo no nome do arquivo.
