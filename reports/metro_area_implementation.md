# metro_area — Relatório de implementação

**Data**: 2026-04-22
**Dataset**: metro_area (Regiões Metropolitanas, RIDEs, Aglomerações Urbanas)
**Status**: IMPLEMENTADO — 40 parquets em `data/metro_area/YYYY/`
**Plano de referência**: `C:/Users/antro/.claude/plans/quero-agora-trabalhar-com-linear-pebble.md`

---

## 1. Cobertura

**20 anos** processados: 1970, 2001, 2002, 2003, 2005, 2008, 2009, 2010, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024.

Anos ausentes no FTP IBGE (confirmado via WebFetch 2026-04-22): 2004, 2006, 2007, 2011, 2012.

Para comparação: geobr antigo cobre 1970, 2001-2003, 2005, 2010, 2013-2018 (12 anos). **Cobertura estendida em 8 anos** (2008, 2009, 2019, 2020, 2021, 2022, 2023, 2024).

## 2. Resultados por ano

Números validados empiricamente via execução real (`/d/tmp/run_metro_all.R`):

| Ano  | nrow | n_rms | n_munis | Notas                                                        |
| ------| -----:| ------:| --------:| --------------------------------------------------------------|
| 1970 | 113  | 9     | 113     | Hardcoded (Lei Complementar 014/020)                         |
| 2001 | 460  | 27    | 386     | RN ausente (ver §5.1)                                        |
| 2002 | 506  | 38    | 429     | Snapshot 30.08.02; proxy muni 2005                           |
| 2003 | 515  | 36    | 410     | Proxy muni 2005                                              |
| 2005 | 548  | 38    | 470     | 1 código 6-dígitos recuperado via check-digit lookup (§5.3)  |
| 2008 | 514  | 36    | 514     | Sem legislation (schema 4 cols)                              |
| 2009 | 550  | 38    | 550     | Sem legislation (schema 4 cols)                              |
| 2010 | 696  | 42    | 696     | Snapshot único 07_31                                         |
| 2013 | 1124 | 69    | 1122    | Snapshot junho                                               |
| 2014 | 1221 | 73    | 1220    | Snapshot junho                                               |
| 2015 | 1320 | 77    | 1320    | Snapshot junho (schema Português; UPPERCASE surge só em dez) |
| 2016 | 1331 | 77    | 1330    | Snapshot junho                                               |
| 2017 | 1385 | 80    | 1384    | Snapshot junho                                               |
| 2018 | 1403 | 81    | 1402    | Snapshot junho_v2 (schema 9 cols; dez tem 12)                |
| 2019 | 1425 | 82    | 1424    | Snapshot junho; schema 12 cols                               |
| 2020 | 1420 | 82    | 1418    | Snapshot junho_v2                                            |
| 2021 | 1470 | 84    | 1432    | Schema novo 16 cols (RECMETROPOL/CATMETROPOL/SUBCATMETROPOL) |
| 2022 | 1419 | 78    | 1388    | Schema 16 cols                                               |
| 2023 | 1439 | 80    | 1397    | Naming novo `Composicao_RM_2023.xlsx`                        |
| 2024 | 1440 | 80    | 1398    | Naming novo                                                  |

Cross-check: 77 RMs em 2015 bate com a contagem oficial do IBGE.

## 3. Estrutura do output

Cada ano produz 2 parquets em `data/metro_area/YYYY/`:
- `metro_area_YYYY.parquet` (geometria completa)
- `metro_area_YYYY_simplified.parquet` (tolerance=100m)

Schema:
```
name_metro       <chr>   # "RM Belém", "RIDE-DF", etc.
code_muni        <dbl>   # 7 dígitos
name_muni        <chr>
type             <chr>   # (2010+) "RM", "RIDE", "Aglomeração Urbana"
subdivision      <chr>   # (2008+) "Núcleo Metropolitano", "Colar", etc.
legislation      <chr>   # (exceto 2008, 2009) "Lei Complementar NNN"
legislation_date <chr>   # formato "DD.MM.YYYY"
code_state       <dbl>
abbrev_state     <chr>   # 2 letras
name_state       <chr>
code_region      <dbl>
name_region      <chr>
year             <int>
geometry         <MULTIPOLYGON [°]>   # SIRGAS 2000 (EPSG:4674)
```

## 4. Validações passadas

- ✅ CRS = 4674 para todos os 40 parquets
- ✅ Geometria MULTIPOLYGON para todos
- ✅ Coluna `geometry` sempre a última
- ✅ Tipos: `code_muni`, `code_state` numeric; `abbrev_state` character 2-letras
- ✅ Nenhum NA em `code_muni`, `code_state`, `abbrev_state`, `name_metro`
- ✅ 7 toy/unit tests em `tests/` passaram

## 5. Limitações e avisos

### 5.1 ⚠️ metro_area 2001 — RN (Rio Grande do Norte) sem geometria

**Localização exata do problema** (validado empiricamente em 2026-04-23):

- ✅ **Lista crua** (XLS `RM%2004.09.01.xls` de metro_area 2001): TEM os 6
  códigos de municípios do RN que compõem a RM Natal
  (2402600, 2403608, 2407104, 2408102, 2403251, 2412005). A lista do IBGE
  está correta.
- ❌ **Shapefile** (`data/municipality/2001/municipalities_2001.parquet`):
  **0 municípios com `abbrev_state='RN'`** e **0 códigos iniciados por `24`
  (UF code de RN)**. O shapefile deste projeto foi construído sem RN.

Consequência: no `left_join(lista_metro_2001, municipality_2001, by='code_muni')`,
os 6 códigos do RN não encontram geometria — as linhas ficam com
`st_is_empty(geometry) == TRUE` e são descartadas. A RM Natal desaparece
do output `metro_area_2001.parquet`.

Comparação com anos adjacentes:
- `municipality_2000`: 166 municípios RN ✓
- `municipality_2001`: 0 municípios RN ❌
- `municipality_2005` em diante: 167 municípios RN ✓

O problema é portanto no **dataset `municipality` para o ano 2001**, não no
metro_area. Afeta qualquer outro dataset do pipeline que joine com
`municipality_2001`.

**Correção**: fora do escopo deste plano. Ver tarefa pós-1 em §18 do plano
de metro_area (auditar/rebuildar `municipality_2001`; investigar se
[R/municipality.R](../R/municipality.R) tem lógica que descarta RN ou se o
download IBGE para `municipio_2001/rn/` está inacessível).

### 5.2 Proxy year_muni para anos sem parquet próprio

O IBGE não publica shapes de municípios para 2002, 2003, 2008, 2009
(confirmado iter 8 Agent X: HTTP 404 em `municipio_2002/` etc.).
Mapeamento usado:

| metro_area year | muni proxy year | Justificativa |
|-----------------|-----------------|---------------|
| 1970 | 1970 (hist_muni) | Guanabara → Rio renomeado manualmente |
| 2001 | 2001 | (⚠ ver 5.1) |
| 2002, 2003 | **2005** | 0% unmatched (validado iter 8); proxy 2001 perderia RN |
| 2005 | 2005 | Identidade |
| 2008 | 2007 | 99.4% match empírico |
| 2009 | 2010 | 99.8% match empírico |
| 2010-2024 | mesmo ano | 100% match |

Trade-off em 2002-2003: usa fronteiras municipais de 2005. Para municípios
cujas fronteiras mudaram 2002→2005, há pequena imprecisão geométrica.
Aceita em troca de cobertura completa (RN inteiro preservado).

### 5.3 Código IBGE de 6 dígitos em 2005 — recuperação automática

O código IBGE de município é **unicamente definido por 6 dígitos**; o 7º é
apenas dígito verificador. A convenção do projeto (e da maioria das fontes)
é sempre reportar 7 dígitos, mas planilhas IBGE antigas ocasionalmente
publicam apenas 6 dígitos.

Em 2005, um código apareceu com 6 dígitos: `310945`. O `clean_metro_area()`
detecta esses casos e **recupera automaticamente** o 7º dígito via lookup no
parquet de municípios carregado:

```
Recuperado digito verificador (year 2005): 310945 -> 3109451
```

Validação empírica (2026-04-23):
- Código 3109451 corresponde a **Cabeceira Grande (MG)**, no noroeste do estado, na divisa com o DF (centroide em `-47.12, -16.07`; distância 0.85° do centro do DF — dentro da RIDE-DF).
- Match único: em `municipality_2005`, há apenas 1 código que começa com `310945` → recovery determinístico.
- XLS crú de 2005 (linha 432) lista "CABECEIRA GRANDE" para o código 310945 — o nome bate.
- Pré-recovery: o código 310945 fazia 0 matches no parquet de municípios, logo sem recovery a linha ficaria sem geometria.

Com a recuperação, 2005 tem 470 municípios (não 469, se descartássemos).

Se algum código de 6 dígitos não tivesse match único no parquet, seria descartado
com warning. Não houve casos assim em nenhum dos 20 anos processados.

### 5.4 Municípios em múltiplas RMs (aviso, não bug)

Alguns municípios pertencem legitimamente a >1 RM/RIDE/Aglom Urbana
(fronteira entre 2 regiões). Observado:
- 2013: 2 municípios
- 2014: 1 município
- 2024: 39 municípios (crescimento ao longo dos anos)

O pipeline preserva essas duplicações (1 linha por RM × município) e apenas
emite `message()` alertando.

## 6. Arquivos modificados

- [R/metro_area.R](../R/metro_area.R) — rewrite completo (catálogo URL, 8 helpers, hardening, year_muni)
- [_targets.R:470-487](../_targets.R#L470-L487) — bloco #19 descomentado e atualizado
- [_targets.R:809](../_targets.R#L809) — `metropolitanarea_clean` → `metro_area_clean` em `all_files`
- `tests/toy_normalize_metro_2001_2003_prefix.R` (novo)
- `tests/toy_normalize_metro_2008_2009.R` (novo)
- `tests/toy_normalize_metro_2021_2024.R` (novo)
- `tests/toy_clean_metro_area_dispatcher.R` (novo)
- `tests/toy_clean_metro_area_validators.R` (novo)
- `tests/unit_metro_area_2018_06_v2.R` (novo)
- `tests/unit_metro_area_e2e_3years.R` (novo)
- [.claude/BACKLOG.md:34](../.claude/BACKLOG.md#L34) — linha 19 atualizada

## 7. Comando de reprodução

### 7.1 Via targets (canônico)

```r
# Pré-requisito: data/municipality/ e data/historical_empire/ já processados
# (isto é, `targets::tar_make(names = c("municipality_clean", "hist_muni_clean"))` rodou antes)
targets::tar_make(names = "metro_area_clean")
```

### 7.2 Bypass tar_make (usado nesta execução)

Na execução desta implementação (2026-04-23), `targets::tar_make(names = "metro_area_clean")`
**falhou em uma dependência upstream NÃO relacionada ao metro_area**:
o targets considerou múltiplos targets (incluindo `municipality_raw`) como "outdated"
no bookkeeping e tentou reexecutá-los. A cadeia envolve pacote R `RCurl` (usado em
`R/river_basins.R` e `R/amazonia_legal.R`) que não está instalado neste ambiente — erro retornado:

```
✖ municipality_raw_6428dc5e119031cc errored
  could not find packages RCurl in library paths
```

Observação importante: **os parquets em `data/municipality/*/` JÁ EXISTEM
em disco e estão válidos** (construídos em sessões anteriores). O pipeline
não precisa reconstruí-los; é apenas um artefato do bookkeeping do targets.

**Bypass adotado**: executar `clean_metro_area()` diretamente em R, lendo
os parquets de `data/municipality/` e `data/historical_empire/` já
existentes. Script: [`/d/tmp/run_metro_all.R`](../../../../../tmp/run_metro_all.R).

```bash
"/c/Program Files/R/R-4.5.0/bin/Rscript.exe" /d/tmp/run_metro_all.R
```

O resultado é **equivalente** ao que `tar_make` produziria porque:
1. `clean_metro_area()` é a mesma função que o target chamaria
2. Os mesmos parquets de municipality/hist_muni em disco são lidos
3. O mesmo output (40 parquets em `data/metro_area/YYYY/`) é gravado

Depois que o problema do `RCurl` for resolvido (tarefa fora do escopo deste
plano — instalar o pacote ou ajustar download_municipality para usar `httr`
em vez de `RCurl`), `tar_make(names = "metro_area_clean")` funcionará
diretamente sem necessidade de bypass.

## 8. Tarefas pós-implementação (ver §18 do plano)

- **Alta prioridade**: auditar/corrigir `municipality_2001` (RN ausente) — ver §5.1
- **Alta prioridade**: auditar/corrigir duplicatas em `municipality_2001` e
  `municipality_2005` (239 e 243 linhas duplicadas, respectivamente, com mesmo
  `code_muni` mas geometrias DIFERENTES — provável bug de processamento em
  `R/municipality.R`). Efeito: parquets metro_area 2001, 2002, 2003 e 2005 têm
  74-102 linhas extras herdadas. Não corrigido aqui porque `distinct()` colapsaria
  geometrias espacialmente distintas (validado empiricamente).
- **Média**: integrar `DePara_RecortesMetropolitanosEAfins.xlsx` como target
  separado (`metro_area_depara_clean`). Spec completo pronto (iter 5 Agent P);
  não implementado por ser opcional e requerer plano próprio.
- **Baixa**: monitorar FTP IBGE para eventual publicação de shapes 2002, 2003,
  2008, 2009 (hoje 404).

## 9. Auditoria final — 2º ciclo de 7 iterações (2026-04-23)

Após a implementação inicial, executei 7 iterações adicionais de validação
(multi-agente) para verificar exaustivamente a qualidade do dataset produzido.

### Bugs descobertos e corrigidos nesta auditoria

1. **Bug CRÍTICO `type` em 2021-2024**: o helper `normalize_metro_2021_2024`
   mapeava `type = LABEL_RECMETROPOL` (nome do recorte, ex: "Belo Horizonte (MG)")
   quando deveria mapear para `LABEL_CATMETROPOL` (categoria do recorte, ex:
   "RM de Belo Horizonte (MG)", "RIDE do Distrito Federal e Entorno",
   "Colar Metropolitano de Belo Horizonte (MG)", "Aglomeração Urbana...").
   - **Fix aplicado**: [R/metro_area.R:577](../R/metro_area.R#L577)
   - **Parquets regenerados**: 2021, 2022, 2023, 2024 (main + simplified = 8 arquivos)
   - **Teste atualizado**: [tests/toy_normalize_metro_2021_2024.R](../tests/toy_normalize_metro_2021_2024.R) agora valida `result$type[1] == "Região Metropolitana"` (era "Manaus (AM)")

2. **Investigação de "duplicatas em 2001-2003"** (Agent E iter 2 reportou 85-116
   linhas repetidas nos parquets):
   - **Conclusão**: o helper `normalize_metro_2001_2003` **não produz duplicatas**
     (output tem 392, 429, 413 linhas únicas para os 3 anos). As duplicatas
     aparecem **após o left_join** com parquets `municipality_2001`/`municipality_2005`,
     que **têm 239 e 243 linhas duplicadas, respectivamente** — mesmo `code_muni`
     com **geometrias espaciais distintas** (ilhas/fragmentos/territórios desconectados).
   - **Fix rejeitado**: aplicar `distinct()` após o join colapsaria geometrias
     legítimas. Documentado como tarefa pós-plano acima.

### Validações adicionais (não-bugs)

- **URLs FTP**: todas as 19 URLs hardcoded retornam HTTP 200; sem mudanças desde
  a implementação inicial.
- **Metodologia IBGE**: nosso schema respeita as definições oficiais
  (RM, RIDE, Aglomeração Urbana, Colar Metropolitano) — ver [§3.4 do relatório](#estrutura-do-output).
- **Helpers 1970-2020**: validados individualmente (helpers corretos,
  schemas conformes, encodings UTF-8 preservados).
- **Município em múltiplas RMs**: 2024 tem 39 casos (vs 2-3 em anos anteriores) —
  reflete o novo schema "Recortes" 2021+, é comportamento legítimo não bug.
- **Transição 2020→2021**: mudança de "Região Metropolitana de X" para
  "Recorte Metropolitano de X" é renomeação semântica IBGE, não erro.

### Decisões arquiteturais confirmadas

- **DePara opção A** (target separado, não enriquecimento automático dos parquets
  existentes) — spec pronto em [iter 5 Agent P]; implementação em plano futuro.
- **Bridge 2019-2020 ↔ 2021+** via conjunto de municípios tem 91.5% de match
  (75/82 RMs do 2020 casam com RMs do 2021 por frozenset de `code_muni`). 7 RMs
  órfãs (Aglomeração Urbana de Franca, Piracicaba, Litoral Norte, Sul, etc.) —
  extintas ou reestruturadas. Documentação em plano futuro se DePara for
  integrado.

### Limpeza de comentários

Removidas todas as referências a "Agent X", "iter N" em comentários de código
(regra `git-commits.md` proíbe qualquer menção a IA/Claude no repositório).

---

## 10. DePara — tabela de bridge RMs 2019-2020 ↔ 2021+

### 10.1 Motivação

O IBGE reestruturou os códigos de regiões metropolitanas entre 2020 e 2021:

| Era | Schema | Identificador principal |
|-----|--------|-------------------------|
| 2019-2020 | 12 cols com `COD` (4 dígitos, estilo SIDRA) | ex: 1101 = Porto Velho |
| 2021-2024 | 16 cols com `COD_RECMETROPOL` (3 dígitos, nova codificação) | ex: 001 = Porto Velho |

Análises temporais cross-era exigem mapeamento entre os dois sistemas. O IBGE
publica oficialmente o arquivo
[`DePara_RecortesMetropolitanosEAfins.xlsx`](https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/municipios_por_regioes_metropolitanas/Situacao_2020a2029/DePara_RecortesMetropolitanosEAfins.xlsx)
que fornece essa correspondência.

### 10.2 Target dedicado

Optamos pela Opção A (target separado, sem modificar parquets existentes) —
validada em plano §19. Implementação:

- Funções em [R/metro_area.R](../R/metro_area.R):
  `download_metro_area_depara()` + `clean_metro_area_depara()`
- Targets em [_targets.R:493-501](../_targets.R#L493-L501) (bloco #19b)
- Output: `data/metro_area_depara/metro_area_depara_clean.parquet`

### 10.3 Schema do parquet

```
233 linhas × 11 colunas:

  source                <chr>   # "SIDRA_categ" (77) | "BET_categ" (81) |
                                #  "SIDRA_subcateg" (37) | "BET_subcateg" (38)
  cod_antigo            <int>   # código antigo (SIDRA/BET)
  nome_antigo           <chr>   # nome na taxonomia antiga
  cod_rec_metropol      <int>   # novo código IBGE 2022 (1-78 em uso)
  cod_cat_metropol      <int>   # categoria principal (01) ou subdivisão (02)
  nome_cat_metropol     <chr>   # ex: "Região Metropolitana", "RIDE"
  label_rec_metropol    <chr>   # ex: "RM de Porto Velho (RO)"
  cod_subcat_metropol   <int>   # 01-05 (NA para sheets categ)
  nome_subcat_metropol  <chr>   # ex: "Sub-região Norte" (NA para categ)
  label_subcat_metropol <chr>   # (NA para categ)
  year                  <int>   # 2022 (data de publicação do DePara)
```

### 10.4 Exemplo de uso

```r
library(arrow); library(dplyr)
depara <- arrow::read_parquet("data/metro_area_depara/metro_area_depara_clean.parquet")

# Lookup direto: Porto Velho na taxonomia antiga -> novo código
depara |>
  filter(source == "SIDRA_categ", cod_antigo == 1101) |>
  select(cod_antigo, nome_antigo, cod_rec_metropol, label_rec_metropol)
# #> cod_antigo nome_antigo  cod_rec_metropol label_rec_metropol
# #>       1101 Porto Velho                 1 RM de Porto Velho (RO)

# Enriquecer análise 2020 com código novo (para join futuro com 2021+)
# (Pseudo-código — requer que o usuário tenha o `COD` de 2020 preservado)
lookup <- depara |>
  filter(source == "SIDRA_categ", !is.na(cod_rec_metropol)) |>
  select(cod_antigo, cod_rec_metropol)
# ds_2020_enriched <- ds_2020 |> left_join(lookup, by = c("cod_sidra" = "cod_antigo"))
```

### 10.5 Limitações conhecidas

- **Cobertura: ~86%** via SIDRA direto (74 de 86 RMs de 2020). Combinado com
  bridge por conjunto de municípios (implementável pelo usuário), chega a ~98%.
- **Escopo temporal**: o DePara só ajuda cross-era 2019-2020 ↔ 2021+. Para
  anos pré-2019 (schema 9 cols ou anterior, sem `COD`), o DePara não se aplica.
- **233 linhas vs 252 esperadas**: ~19 linhas das sheets originais tinham
  valores "-" (sem mapeamento) e foram filtradas pelo helper.

### 10.6 Destino das 2 RMs órfãs — NÃO EXTINTAS, mas omitidas do arquivo FTP

Duas Aglomerações Urbanas presentes em `metro_area_2020` mas sem mapeamento
via DePara **NEM** via bridge por conjunto de municípios foram investigadas
em 3 iterações empíricas locais (2026-04-23) + 3 agentes de pesquisa online
(2026-04-23):

| COD 2020 | Nome | UF | Lei Comp. | Nº munis | Status |
|---------|------|-----|-----------|----------|--------|
| 3509 | Aglomeração Urbana de Franca | SP | LC 1.323/2018 | 19 | **ATIVA (omitida do FTP pós-2020)** |
| 4304 | Aglomeração Urbana do Litoral Norte | RS | LC 12.100/2004 | 20 | **ATIVA (omitida do FTP pós-2020)** |

#### Evidência local (empírica nos dados do pipeline)

1. **Zero membros em `metro_area` 2021-2024**: nenhum dos 19+20 municípios
   aparece em qualquer RM/RIDE/AU pós-2020 no arquivo
   `Composicao_RMs_RIDEs_AglomUrbanas_*`.
2. **Ausentes no DePara IBGE** (`cod_antigo == 3509` e `== 4304`): 0 linhas.
3. **"Litoral Norte" em 2021+ é entidade diferente**: referências a
   "Litoral Norte" em 2021-2024 (39 linhas/ano) correspondem à **RM do Vale
   do Paraíba e Litoral Norte de SP** (COD_RECMETROPOL 52), não à AU do RS.

#### Evidência online (pesquisa institucional/legal, 2026-04-23)

**Ambas AUs continuam juridicamente e institucionalmente ativas:**

- **AU Franca (SP)**:
  - [LC 1.323/2018](https://www.al.sp.gov.br/repositorio/legislacao/lei.complementar/2018/lei.complementar-1323-22.05.2018.html)
    não foi revogada; [portal PDUI-AUF](https://auf.pdui.sp.gov.br/) ativo com
    publicações 2022-2024
  - IBGE 2024 declara explicitamente que existem "3 aglomerações urbanas no
    Brasil: 1 em SP (Franca) e 2 em RS" —
    [IBGE Agência de Notícias 2024](https://agenciadenoticias.ibge.gov.br/agencia-noticias/2012-agencia-de-noticias/noticias/43754-ibge-divulga-recortes-geograficos-do-pais-atualizados-para-2024)
  - [IBGE Quadro Geográfico 2024 (PDF)](https://www.ibge.gov.br/apps/quadrogeografico/pdf/qg_2024_230_aglomurb.pdf)
    lista AU Franca explicitamente

- **AU Litoral Norte (RS)**:
  - [LC 12.100/2004](https://www.al.rs.gov.br/filerepository/repLegis/arquivos/12.100.pdf)
    não foi revogada
  - [METROPLAN](https://www.metroplan.rs.gov.br/) (Fundação Estadual de
    Planejamento Metropolitano/RS) continua a gestão das AUs gaúchas
  - População cresceu: 340.436 (2020) → 434.330 (2024) conforme
    [Atlas Socioeconômico RS](https://atlassocioeconomico.rs.gov.br/aglomeracoes-urbanas)
  - [COREDE Litoral](https://litoralnarede.com.br/consulta-popular-2024-veja-propostas-do-litoral-norte-e-saiba-como-votar/)
    e [Consórcio Municipal Litoral Norte](https://consorciolitoralnorte.brtransparencia.com.br/)
    operam em 2024

- **Meta-contexto constitucional**: CF art. 25 §3º confere **competência
  exclusiva aos Estados** para criar/modificar/extinguir RMs/AUs. O IBGE
  não tem autonomia legal para desreconhecer unilateralmente uma AU criada
  por lei estadual válida — ele apenas reporta. Ver
  [Lei 13.089/2015 (Estatuto da Metrópole)](https://www.planalto.gov.br/ccivil_03/_ato2015-2018/2015/lei/l13089.htm).

#### Interpretação (revista)

A evidência local ("ausentes em metro_area 2021+") NÃO significa que as AUs
foram extintas. Significa apenas que o arquivo específico
`Composicao_RMs_RIDEs_AglomUrbanas_*.xlsx` (nossa fonte) foi reformulado em
2021: passou a **omitir** as 3 AUs remanescentes (Franca + 2 RS), mesmo
sendo reconhecidas pelo próprio IBGE em outros documentos (Quadro Geográfico
PDF, comunicados institucionais).

Hipótese: mudança editorial do IBGE ao renomear o conceito para "Recortes
Metropolitanos" (2021+) — o arquivo FTP passou a priorizar RMs stricto sensu
+ RIDEs, deixando as AUs menores em outros produtos.

#### Ação no pipeline — nenhuma mudança

Comportamento atual reflete fielmente a fonte FTP:
- `metro_area_2020` preserva as 2 AUs (dado histórico do FTP)
- `metro_area_2021-2024` não contém as 2 AUs (fielmente ao FTP pós-2020)
- `metro_area_depara_clean` omite (fielmente ao DePara oficial)

Se futuramente for necessário incluir as 3 AUs remanescentes em anos 2021+,
a fonte seria o "Quadro Geográfico" PDF do IBGE (não nosso XLSX atual) —
abrir plano separado.

#### Monitoring

[`tests/toy_orphan_rms_extintas.R`](../tests/toy_orphan_rms_extintas.R) —
valida que o fato persiste. Se o IBGE retornar essas AUs ao arquivo FTP
`Composicao_RMs_*` em alguma versão futura, o teste falha e sinaliza que
nosso pipeline pode passar a capturá-las automaticamente sem nenhum fix.
