# BACKLOG — geobr_prep_data

Atualizado: 2026-04-24

## Legenda
- **DONE** — completo e rodando no pipeline
- **BUG** — no pipeline mas com issue aberta

---

## Pipeline ativo (36 datasets)

| # | Dataset               | Status | Observações                                    |
|---|-----------------------|--------|------------------------------------------------|
| 01 | semiarid              | DONE | 4 anos (2005-2022)                             |
| 02 | amazonia_legal        | DONE | IBGE FTP 2019-2024, dissolve 2019/2020         |
| 03 | biomes                | DONE | 2004, 2019                                     |
| 04 | statistical_grid      | DONE | 2010, 2022 — download paralelo pendente        |
| 05 | health_facilities     | DONE | atualização mensal YYYY_MM, POINT data         |
| 06 | indigenous_land       | DONE | 2024, FUNAI                                    |
| 07 | intermediate_regions  | DONE | 2019-2024                                      |
| 08 | immediate_regions     | DONE | 2019-2024                                      |
| 09 | schools               | DONE | 2007-2024 (18 anos), microdados INEP + geocodebr, POINT |
| 09b | schools_bi           | DONE | Oracle BI CSV (download manual em data-raw/), POINT    |
| 10 | states                | DONE | 2000-2024 (15 anos)                            |
| 11 | regions               | DONE | 2000-2024 (15 anos)                            |
| 12 | country               | DONE | 2000-2024 (15 anos)                            |
| 13 | mesoregions           | DONE | 2000-2018 (9 anos)                             |
| 14 | microregions          | DONE | 2000-2018 (9 anos)                             |
| 15 | municipality          | DONE | 2000-2024 (17 anos)                            |
| 16 | municipal_seat        | DONE | 2010                                           |
| 17 | census_tract          | DONE | 2000 (unified 236k feat, 27/27 UFs CompatMalhas, Voronoi gap fill), 2010, 2022 |
| 18 | weighting_area        | DONE | 2010 (54 parquets)                             |
| 19 | metro_area            | DONE | 1970, 2001-2003, 2005, 2008-2010, 2013-2024 (20 anos, 40 parquets), Excel→Parquet, join com municipality/hist_muni |
| 19b| metro_area_depara     | DONE | Bridge SIDRA/BET → cod_recmetropol 2022 (233 linhas, 1 parquet); cobertura 86% |
| 20 | urban_area            | DONE | 2005, 2015                                     |
| 21 | conservation_units    | DONE | 2024, 2025                                     |
| 22 | disaster_risk_areas   | DONE | sem year                                       |
| 23 | health_region         | DONE | 1991-2013 (6 anos), DATASUS FTP .MAP nativo    |
| 24 | neighborhoods         | DONE | 2010, 2022                                     |
| 25 | urban_concentrations  | DONE | sem year                                       |
| 26 | pop_arrangements      | DONE | sem year                                       |
| 27 | favelas               | DONE | 2022, IBGE Censo                               |
| 28 | localidades           | DONE | 2010                                           |
| 29 | polling_places        | DONE | 2022, 2024, TSE                                |
| 29b | electoral_zones      | DONE | 2022, 2024, TSE                                |
| 31 | river_basins          | DONE | 2021 (3 níveis)                                |
| 32 | historical_empire     | DONE | 1872-1991 (11 anos), IBGE FTP                  |
| 33 | capitals              | DONE | 2010, POINT, derivado de municipal_seat          |
| 34 | quilombo_area         | DONE | 2024 snapshot, INCRA, 427 áreas quilombolas      |
| 35 | comparable_areas (AMC)| DONE | 91 pares de anos (1872-2020), crosswalk temporal  |

**Total: 675 Parquet files, ~8.6 GB. Pipeline 100%: 0 errors, 0 outdated.**

---

## Datasets futuros (possibilidades)

| Dataset | Fonte | Complexidade | Script legado |
|---|---|---|---|
| Rodovias federais | DNIT | Moderada (LINESTRING) | `ainda_sem_targets/prep_federal_highway.R` |
| Setores agropecuários | IBGE Censo Agro | Alta | `ainda_sem_targets/prep_agro_census_tract.R` (template vazio) |

---

## Bugs de infraestrutura (21 BUGS, TODOS RESOLVIDOS)

| Bug                           | Fix aplicado           | Data       |
|-------------------------------|------------------------|------------|
| normalize_sf_geometry() #32   | Função criada          | 2026-03-30 |
| to_multipolygon() col loss #30| Filtro removido        | 2026-03-30 |
| tidyselect all_of()           | rename + select        | 2026-03-30 |
| states_clean memory           | rm/gc + glimpse        | 2026-03-30 |
| amazonia_legal all_files #2   | Target descomentado    | 2026-03-29 |
| schools all_files #9          | Target descomentado    | 2026-03-31 |
| upload overwrite              | overwrite = TRUE       | 2026-03-30 |
| dir_clean health_region #23   | Path corrigido         | 2026-03-30 |
| função duplicada health_region| Duplicata deletada     | 2026-03-30 |
| maptools removido CRAN #23    | read_datasus_map()     | 2026-03-31 |
| health_facilities CRS=NA      | crs=4326 + proj fix    | 2026-03-31 |
| st_set_crs sem atribuição     | obj <- st_set_crs()    | 2026-03-31 |
| geobr::read_municipality dep  | read_geoparquet()      | 2026-03-31 |
| to_multipolygon() grouped_df  | ungroup() adicionado   | 2026-03-31 |
| weighting_area unzip encoding | system2("unzip")       | 2026-03-31 |
| schools INEP fonte            | microdados+geocodebr   | 2026-04-01 |
| schools_clean abbrev_state dup| check condicional      | 2026-04-01 |
| pop_arrangements timeout      | httr::timeout(300)     | 2026-04-01 |
| schools 2023 CEP letras       | gsub("[^0-9]","",CEP)  | 2026-04-02 |
| read_geoparquet CRS=NA        | sfarrow fallback       | 2026-04-02 |
| states code_state character   | as.numeric() pós-join  | 2026-04-02 |
| municipality_2001 AC/RN zerados | readmerge_geobr clean_names interno (Rafael) | 2026-04-24 |
| duplicatas 1991/2001/2005     | dissolve_polygons_no_split em hist_muni | 2026-04-24 |
| urbanarea 2015 (NomConcUrb vs NomeConcUr) | coalesce no clean_urbanarea | 2026-04-24 |
| urbanarea 2005 schema inconsistente | type = "Área urbanizada" fixo | 2026-04-24 |
| statsgrid OOM ~29% quadrantes | ddbs_stop_conn em vez de dbDisconnect | 2026-04-24 |

Ver detalhes em `.claude/PROBLEMS.md`.

---

## Melhorias concluídas

- [x] Paralelizar statistical_grid downloads com `curl::multi_download()`
- [x] Criar `validate_geobr()` — validação automatizada de todos os parquets
- [x] Criar testes testthat para funções core (5 arquivos em tests/testthat/)
- [x] GeoParquet com metadados espaciais via `write_geobr_parquet()` + geoarrow
- [x] Pipeline self-contained sem `geobr::read_municipality()`
- [x] `read_geoparquet()` com sfarrow fallback para CRS
- [x] Schools: microdados INEP + geocodebr (18 anos, 100% geocodificado)

## Pendente

- [ ] **Re-rodar tar_make()** para regerar os ~49 targets outdated pelos hashes dos commits do Rafael (`readmerge_geobr clean_names`, `download paralelo`, `fix historical states`, `regioes de saude`).
- [ ] **Regerar metro_area_2001** para propagar o fix AC/RN do municipality_2001 (RM Natal ainda ausente no parquet antigo).
- [ ] Testar e descomentar target de upload (`upload_arquivos()`)