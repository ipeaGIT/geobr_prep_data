# Plano: municipal_seat (issue #16)

**Status**: CONCLUÍDO (2026-03-30)

## Contexto

Script existe em `ainda_sem_targets/prep_municipal_seat.R` mas não está
integrado ao pipeline `targets`. Dados são pontos (sede administrativa de cada
município), não polígonos.

**Fonte:** IBGE — malhas municipais (mesmo FTP dos municípios)
**Anos disponíveis:** 2010, 2022
**Geometria:** POINT (não MULTIPOLYGON)

## Colunas esperadas no output

```
code_muni     numeric  código IBGE do município (7 dígitos)
name_muni     character nome do município
code_state    numeric  2 dígitos
abbrev_state  character 2 letras
name_state    character
code_region   numeric
name_region   character
year          numeric
geometry      sfc_POINT  ← pontos, não polígonos
```

## Plano de implementação

### Passo 1 — Ler o script legado

Ler `ainda_sem_targets/prep_municipal_seat.R` para entender:
- URL exata dos dados no IBGE
- Nomes das colunas no SHP original
- Tratamento especial aplicado

### Passo 2 — Criar R/municipal_seat.R

```r
download_municipalseat <- function(year) {

  # URLs baseadas no padrão de malhas municipais do IBGE
  ftp_link <- switch(as.character(year),
    "2010" = "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2010/Brasil/BR/br_municipios_sede_2010.zip",
    "2022" = "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/Brasil/BR/BR_Municipios_Sedes_2022.zip",
    stop(paste("Ano", year, "não suportado para municipal_seat"))
  )
  # ATENÇÃO: verificar URLs exatas no FTP do IBGE antes de usar

  zip_dir <- paste0(tempdir(), "/municipal_seat/", year)
  in_zip  <- paste0(zip_dir, "/zipped/")
  out_zip <- paste0(zip_dir, "/unzipped/")
  dir.create(in_zip,  recursive = TRUE, showWarnings = FALSE)
  dir.create(out_zip, recursive = TRUE, showWarnings = FALSE)

  encode <- if (year == 2010) "WINDOWS-1252" else "UTF-8"

  file_raw <- fs::file_temp(tmp_dir = in_zip, ext = "zip")
  httr::GET(url = ftp_link, httr::progress(),
            httr::write_disk(path = file_raw, overwrite = TRUE))

  unzip_geobr(zip_dir = zip_dir, in_zip = in_zip,
              out_zip = out_zip, is_shp = TRUE)

  raw <- sf::st_read(out_zip, quiet = TRUE, stringsAsFactors = FALSE,
                     options = paste0("ENCODING=", encode))

  glimpse(raw)
  return(raw)
}

clean_municipalseat <- function(municipal_seat_raw, year) {

  dir_clean <- paste0("./data/municipal_seat/", year)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

  # Padronizar colunas (verificar nomes reais após download)
  temp_sf <- municipal_seat_raw |>
    dplyr::rename(
      code_muni = <coluna_codigo_muni>,
      name_muni = <coluna_nome_muni>
    ) |>
    dplyr::select(code_muni, name_muni, geometry)

  temp_sf$code_muni  <- as.numeric(temp_sf$code_muni)
  temp_sf$code_state <- substr(as.character(temp_sf$code_muni), 1, 2) |>
                        as.numeric()

  # Pontos não têm topologia nem multipolygon
  temp_sf <- harmonize_geobr(
    temp_sf        = temp_sf,
    year           = year,
    add_state      = TRUE,
    state_column   = "code_state",
    add_region     = TRUE,
    region_column  = "code_state",
    add_snake_case = TRUE,
    snake_colname  = "name_muni",
    projection_fix = TRUE,
    encoding_utf8  = TRUE,
    topology_fix   = FALSE,      # pontos não têm topologia
    remove_z_dimension = TRUE,
    use_multipolygon   = FALSE   # pontos não são polígonos
  )

  temp_sf <- temp_sf |>
    dplyr::select(code_muni, name_muni,
                  code_state, abbrev_state, name_state,
                  code_region, name_region,
                  year, geometry)

  # Sem versão simplificada (pontos não são simplificáveis)
  arrow::write_parquet(temp_sf,
    sink = paste0(dir_clean, "/municipal_seat_", year, ".parquet"),
    compression = 'zstd', compression_level = 7)

  files <- list.files(dir_clean, pattern = ".parquet",
                      full.names = TRUE, recursive = TRUE)
  return(files)
}
```

### Passo 3 — Adicionar targets em _targets.R

```r
#16. Sede Municipal -----------------------------------------------

  tar_target(name = years_municipalseat,
             command = c(2010, 2022)),

  tar_target(name = municipalseat_raw,
             command = download_municipalseat(years_municipalseat),
             pattern = map(years_municipalseat)),

  tar_target(name = municipalseat_clean,
             command = clean_municipalseat(municipalseat_raw, years_municipalseat),
             pattern = map(municipalseat_raw, years_municipalseat),
             format = 'file'),
```

E adicionar `municipalseat_clean` no bloco `all_files`.

## Pontos de atenção

1. **URLs IBGE:** Verificar os endereços exatos no FTP antes de implementar.
   O padrão de URLs do IBGE muda entre anos.

2. **Nomes de colunas:** Inspecionar `names(raw)` após download para mapear
   corretamente para `code_muni` e `name_muni`.

3. **Geometria POINT:** Não gerar versão `_simplified.parquet` (pontos não
   têm geometria para simplificar).

4. **Verificar script legado:** `ainda_sem_targets/prep_municipal_seat.R`
   pode ter os URLs corretos e tratamentos específicos já resolvidos.
