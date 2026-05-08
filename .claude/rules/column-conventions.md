# Convenções de Colunas — geobr_prep_data

REGRA: Toda coluna deve seguir este padrão antes de salvar o Parquet.
Verificar com `names(temp_sf)` e `glimpse_geobr(temp_sf)` antes de gravar.

## Prefixos obrigatórios

| Prefixo    | Tipo R    | Exemplos                                       |
|------------|-----------|------------------------------------------------|
| `code_*`   | numeric   | code_muni, code_state, code_region, code_biome |
| `name_*`   | character | name_muni, name_state, name_region             |
| `abbrev_*` | character | abbrev_state (2 letras, MAIÚSCULAS: SP, AC)    |

## Ordem obrigatória das colunas

```
1. Colunas de identificação do dataset (code_X, name_X)
2. code_state
3. abbrev_state
4. name_state
5. code_region
6. name_region
7. year
8. geometry  ← SEMPRE A ÚLTIMA COLUNA
```

Implementação:
```r
temp_sf <- temp_sf |>
  dplyr::select(code_muni, name_muni,
                code_state, abbrev_state, name_state,
                code_region, name_region,
                year, geometry)
```

## Tipos de dados obrigatórios

| Coluna       | Tipo R          | Observação                              |
|--------------|-----------------|-----------------------------------------|
| `code_*`     | numeric         | NÃO character — usar `as.numeric()`     |
| `name_*`     | character       | NÃO factor                              |
| `abbrev_state` | character     | NÃO factor; sempre 2 letras maiúsculas  |
| `year`       | numeric         | NÃO character                           |
| `geometry`   | sfc_MULTIPOLYGON| `sf::st_cast(x, "MULTIPOLYGON")`       |

## Mapeamento IBGE → geobr

| Coluna original IBGE                                      | Coluna geobr   |
|-----------------------------------------------------------|----------------|
| cd_mun, cd_geocmu, geocodigo, cd_geocodm, cd_municip      | code_muni      |
| nm_mun, nm_municip, nome, nome_munic, nm_mun_censo        | name_muni      |
| cd_uf, cod_uf, coduf, id_uf, cd_uf (numérico)             | code_state     |
| sigla, sigla_uf, sg_uf, abbrev_state                      | abbrev_state   |
| nm_uf, nm_estado, nome_uf                                 | name_state     |
| cd_regia, cd_regiao, nm_regia                             | code_region    |
| nm_regiao, nm_regia, nome_regiao                          | name_region    |

Usar `standardcol_geobr()` ou `rename_cols_geobr()` para mapeamento automático.
Dicionário completo em `support_harmonize_geobr.R` (função `rename_cols_geobr`).

## Derivar code_state de code_muni

```r
temp_sf$code_state <- substr(as.character(temp_sf$code_muni), 1, 2) |>
                      as.numeric()
```

Para mesoregião/microrregião: mesma lógica (primeiros 2 dígitos).

## Formatação de nomes (snake_case_names)

Aplicar via `harmonize_geobr(..., snake_colname = "name_X")` ou diretamente:
```r
temp_sf <- snake_case_names(temp_sf, colname = c("name_state", "name_muni"))
```

Regras:
- Primeira letra de cada palavra em maiúscula (Title Case)
- Preposições em minúsculo: do, dos, da, das, de, del, d'

Correto: "São Paulo", "Mato Grosso do Sul", "Espírito Santo"
Errado:  "SÃO PAULO", "Mato Grosso Do Sul", "Espirito Santo"

## Checklist final antes de gravar Parquet

```r
stopifnot(is.numeric(temp_sf$code_state))
stopifnot(is.character(temp_sf$name_state))
stopifnot(is.character(temp_sf$abbrev_state))
stopifnot(all(nchar(temp_sf$abbrev_state) == 2))
stopifnot(names(temp_sf)[ncol(temp_sf)] == "geometry")
stopifnot(sf::st_crs(temp_sf)$epsg == 4674)
stopifnot(all(sf::st_geometry_type(temp_sf) == "MULTIPOLYGON"))
stopifnot(!any(is.na(temp_sf$code_state)))
```
