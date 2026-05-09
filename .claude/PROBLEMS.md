# PROBLEMS — Bugs Resolvidos (Histórico)

Atualizado: 2026-04-23 (23 bugs resolvidos; 2 pendentes para colaborador)

---

## BUG-001 | normalize_sf_geometry() não existe ✓ RESOLVIDO
**Issue:** #32 (OPEN)
**Arquivo:** `R/support_harmonize_geobr.R:82-87`
**Fix aplicado (2026-03-30):** Função `normalize_sf_geometry()` criada e chamada
dentro de `harmonize_geobr()`, antes de `to_multipolygon()` para garantir que
o regex `'geometry|geom'` funcione corretamente.

---

## BUG-002 | to_multipolygon() apaga colunas ✓ RESOLVIDO
**Issue:** #30 (OPEN)
**Arquivo:** `R/support_harmonize_geobr.R:322-324`
**Fix aplicado (2026-03-30):** Removido filtro `col_names %like% "code_|name_"`.
Agora `group_by` usa todas as colunas não-geometry, preservando `abbrev_state`,
`year`, `area_km2` e quaisquer outras colunas presentes.

---

## BUG-003 | tidyselect all_of() em biomes.R ✓ RESOLVIDO
**Issue:** sem issue formal
**Arquivo:** `R/biomes.R:141-145`
**Fix aplicado (2026-03-30):** Substituído `select()` com renomeação inline
por `dplyr::rename()` + `dplyr::select()`, eliminando warning de deprecation.

---

## BUG-004 | states_clean com memory issues ✓ RESOLVIDO
**Issue:** sem issue formal
**Arquivo:** `R/prep_state.R`
**Fix aplicado (2026-03-30):** Removidos 14 `glimpse()` redundantes e adicionados
`rm()/gc()` para liberar objetos intermediários (`states_raw`, `states_clean`,
`temp_sf`, `temp_sf_simplified`) após último uso.

## BUG-004 (antigo) | _targets.R:264
**Código original:**
```r
tar_target(name = states_clean, # This target is with memory issues
```
**Impacto:** Pipeline pode ser lento ou falhar ao processar 16 anos × dissolve.
**Fix temporário:** `targets::tar_make(callr_function = NULL)`
**Investigar:** Processar estados por lote de anos ou usar `crew` com mais workers.

---

## BUG-005 | upload.R sem overwrite + URL errada ✓ RESOLVIDO
**Issue:** sem issue formal
**Arquivo:** `R/upload.R:26-35`
**Fix aplicado (2026-03-30):** Adicionado `overwrite = TRUE` em `pb_upload()` e
corrigido URL de `padronizacao_cnefe` para `geobr`.

---

## BUG-006 | amazonia_legal em all_files sem target ativo ✓ RESOLVIDO
**Issue:** #2 (OPEN)
**Arquivo:** `_targets.R:640`
**Fix aplicado (2026-03-29):** Linha 640 comentada.
**Fix definitivo pendente:** Implementar dataset completo → ver `.claude/plans/amazonia_legal.md`.

---

## BUG-007 | schools em all_files sem target ativo ✓ RESOLVIDO
**Issue:** #9 (OPEN)
**Arquivo:** `_targets.R:647`
**Fix aplicado (2026-03-29):** Linha 647 comentada.

---

## BUG-008 | prep_health_region.R usa maptools removido do CRAN ✓ RESOLVIDO
**Issue:** #23 (OPEN)
**Arquivo:** `R/prep_health_region.R`
**Fix aplicado (2026-03-31):** Construído `read_datasus_map()` em
`support_harmonize_geobr.R` — leitor .MAP puro em R, sem dependência de maptools.
Download agora é direto do DATASUS FTP (`ftp://ftp.datasus.gov.br/territorio/mapas/`).
Todos os 6 anos (1991-2013) funcionam.

---

## BUG-009 | clean_healthregions() definida duas vezes ✓ RESOLVIDO
**Issue:** #23 (OPEN)
**Arquivo:** `R/prep_health_region.R`
**Fix aplicado (2026-03-30):** Deletada primeira definição (linhas 119-275,
`compression_level = 22`). Mantida apenas a segunda com `compression_level = 7`.

---

## BUG-010 | dir_clean errado em clean_healthregions() ✓ RESOLVIDO
**Issue:** #23 (OPEN)
**Arquivo:** `R/prep_health_region.R:124`
**Fix aplicado (2026-03-30):** Corrigido `./data/intermediate_regions/` para
`./data/health_region/`.

---

## BUG-011 | health_facilities CRS = NA ✓ RESOLVIDO
**Arquivo:** `R/health_facilities.R:102-103, 216`
**Causa:** `st_as_sf()` sem parâmetro `crs=` (dados CSV) + `projection_fix = F`.
**Fix aplicado (2026-03-31):** `crs = 4326` no `st_as_sf()` + `projection_fix = T`
+ `stopifnot()` validação CRS. Decisão: usar WGS84 (4326) e transformar para 4674.

---

## BUG-012 | st_set_crs() sem atribuição em regions e country ✓ RESOLVIDO
**Arquivo:** `R/prep_region.R:372`, `R/country.R:320`
**Causa:** `st_set_crs(obj, 4674)` retorna novo objeto mas resultado não era atribuído.
**Fix aplicado (2026-03-31):** `obj <- st_set_crs(obj, 4674)`.

---

## BUG-013 | Dependência circular: 3 scripts usam geobr::read_municipality() ✓ RESOLVIDO
**Arquivos:** `R/semiarid.R:151`, `R/metro_area.R:104`, `R/weighting_area.R:138`
**Causa:** Pipeline consome sua própria saída publicada como input.
**Fix aplicado (2026-03-31):** Substituído `geobr::read_municipality()` por leitura
dos parquets do próprio pipeline via `read_geoparquet()`. DAG atualizado com
dependências explícitas: municipality_clean → {semiarid, metro_area, weighting_area},
historicalempire_clean → metro_area (para 1970).

---

## BUG-015 | to_multipolygon() retorna grouped_df ✓ RESOLVIDO
**Arquivo:** `R/support_harmonize_geobr.R:328-330`
**Causa:** `group_by() |> summarise()` sem `ungroup()` no final. Resultado é
`grouped_df`, o que quebra `st_transform()` em `simplify_temp_sf()` a jusante.
**Fix aplicado (2026-03-31):** Adicionado `|> dplyr::ungroup()` após `summarise()`.

---

## BUG-016 | weighting_area unzip falha com filenames não-UTF-8 ✓ RESOLVIDO
**Arquivo:** `R/weighting_area.R:83`
**Causa:** `utils::unzip()` falha com "string multibyte inválida em '<93>nia'"
porque os ZIPs do IBGE contêm diretórios com acentos (ex: `11_RO_Rondônia/`)
em encoding WINDOWS-1252, incompatível com o parser UTF-8 do R.
**Fix aplicado (2026-03-31):** Substituído `utils::unzip()` por
`system2("unzip", args = c("-joq", ...))` que lida com encoding de filenames.

---

## BUG-017 | states_clean: abbrev_state duplicada após BUG-002 fix ✓ RESOLVIDO
**Arquivo:** `R/prep_state.R:407-446`
**Causa:** Após BUG-002 fix, `to_multipolygon()` preserva `abbrev_state`. O código
pós-harmonização tentava re-adicionar via `inner_join`, criando `abbrev_state.x/.y`.
Depois `relocate(abbrev_state)` falhava: "Column abbrev_state doesn't exist."
**Fix aplicado (2026-04-01):** Simplificado para check condicional:
`if (!"abbrev_state" %in% names(temp_sf))` antes do join.

---

## BUG-018 | pop_arrangements/urban_concentrations download timeout ✓ RESOLVIDO
**Arquivo:** `R/prep_pop-arrengments_urban-concentr.r:68, 240`
**Causa:** `download.file()` sem timeout (default 60s). ZIP de 87MB do IBGE FTP
frequentemente excede o timeout em conexões lentas.
**Fix aplicado (2026-04-01):** Substituído por `httr::GET() + httr::timeout(300)`
com verificação de arquivo.

---

## BUG-014 | schools.R — fonte INEP direto ✓ RESOLVIDO
**Arquivo:** `R/schools.R`
**Causa original:** Pipeline usava GPKG do geobr (dependência circular).
**Fix (2026-04-01):** Reescrito com estratégia: microdados INEP + geocodebr (CNEFE/IBGE).
  - Microdados: download.inep.gov.br → 215k escolas com metadados
  - Geocodificação: geocodebr → endereços → lat/lon → POINT geometry (CRS 4674)
  - Oracle BI: scraper SOAP preservado, bloqueado pelo INEP
  - GPKG geobr: removido permanentemente
  - Parquet: sfarrow::st_write_parquet() (geoarrow não instalado)
  - Testado: pipeline completo OK, 20 colunas, EPSG 4674

---

## BUG-019 | Schools 2023 CEP com letras crash geocodebr ✓ RESOLVIDO
**Arquivo:** `R/schools.R:289`
**Causa:** CSV do INEP 2023 tem 49 CEPs com caracteres não-numéricos (notação
científica `5.6e+07`). `enderecobr::padronizar_enderecos()` aborta com
"CEP não deve conter letras" para todo o lote de 217k escolas.
**Fix aplicado (2026-04-02):** `gsub("[^0-9]", "", df$CO_CEP)` antes de passar
ao geocodebr. 217k escolas geocodificadas a 100% após fix.

---

## BUG-020 | read_geoparquet perde CRS de parquets sfarrow ✓ RESOLVIDO
**Arquivo:** `R/support_harmonize_geobr.R:576-585`
**Causa:** `arrow::open_dataset() |> sf::st_as_sf()` não preserva CRS de
parquets escritos com `sfarrow::st_write_parquet()`. Retorna EPSG=NA.
**Fix aplicado (2026-04-02):** `read_geoparquet()` agora tenta sfarrow
primeiro (preserva CRS). Fallback para arrow se CRS=NA ou erro.

---

## BUG-021 | states_2000 code_state character no parquet ✓ RESOLVIDO
**Arquivo:** `R/prep_state.R:411-418`
**Causa:** `states_geobr()` retorna code_state como character. `prep_state.R`
convertia para character antes do join e nunca convertia de volta para numeric.
Validação falhava: "code_state is not numeric".
**Fix aplicado (2026-04-02):** Adicionado `as.numeric()` após o join, antes de
salvar o parquet. Mesmo fix em `prep_urban_area.R:167`.

---

## BUG-022 | metro_area 2021-2024 `type` mapeado errado ✓ RESOLVIDO
**Arquivo:** `R/metro_area.R:577`
**Causa:** `normalize_metro_2021_2024()` mapeava `type = "LABEL_RECMETROPOL"`
(nome do recorte, ex: "Belo Horizonte (MG)") quando deveria ser
`"LABEL_CATMETROPOL"` (categoria: "RM de Belo Horizonte (MG)",
"RIDE do Distrito Federal e Entorno", "Colar Metropolitano de...").
Descoberto no 2º ciclo de auditoria (iter 1 Agent A, iter 2 Agent G).
**Fix aplicado (2026-04-23):** Linha 577 alterada para `LABEL_CATMETROPOL`.
Parquets 2021, 2022, 2023, 2024 (main + simplified = 8 arquivos) regenerados.
Teste `tests/toy_normalize_metro_2021_2024.R` atualizado para validar valor
correto.

---

## BUG-023 | metro_area: `year` int32 em 1970 vs double em outros anos ✓ DOCUMENTADO
**Arquivo:** `R/metro_area.R` (`metro_area_1970_data()` usa `year = 1970L`)
**Causa:** 1970 hardcoda `year` como integer literal (`1970L`), outros anos
recebem via `raw$year[1]` que vira double.
**Status:** Não corrigido (inconsistência cosmética; não afeta análises
diretas, apenas concatenação entre datasets). Documentado no relatório iter 4.

---

## PENDENTES (para colaborador)

### PEND-01 | municipality_2001 sem RN ❌ PENDENTE
**Arquivo:** `data/municipality/2001/municipalities_2001.parquet`
**Causa:** Parquet construído sem nenhum município do estado RN (0 linhas
com `abbrev_state == 'RN'`). Validado empiricamente: `municipality_2000`
tem 166 RNs, `municipality_2005` tem 167 RNs, `municipality_2001` tem 0.
**Impacto:** `metro_area_2001` perde RM Natal e qualquer RM do RN. Potencial-
mente afeta outros datasets que joinam com municipality_2001.
**Hipóteses a investigar:**
1. Filtro em `R/municipality.R` que descarta RN em 2001
2. URL FTP IBGE `municipio_2001/rn/` inacessível na execução que gerou o parquet
3. Erro no unzip/encoding que perdeu os shapefiles de RN
**Prioridade:** ALTA.

### PEND-02 | municipality_2001/2005 duplicatas ❌ PENDENTE
**Arquivo:** `data/municipality/2001/` e `data/municipality/2005/`
**Causa:** 239 linhas duplicadas em 2001 (35 códigos únicos com múltiplos
registros) e 243 em 2005 (38 códigos). Cada duplicata tem **geometria
ESPACIAL DIFERENTE** (não são duplicatas exatas — são ilhas/fragmentos
separados do mesmo município armazenados como linhas separadas).
**Impacto:** `metro_area_2001, 2002, 2003, 2005` herdam 74-102 linhas
extras cada (via left_join). Não corrigível com `distinct()` sem colapsar
geometrias legítimas. Validado iter 3 Agent H, iter 6 Agent Q.
**Investigação sugerida:** provável bug em `R/municipality.R` que não
dissolve fragmentos (MULTIPOLYGON não sendo agrupado por `code_muni`).
**Prioridade:** MÉDIA.
