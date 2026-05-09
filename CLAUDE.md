# geobr_prep_data — Instruções para Claude Code

> **REGRA FUNDAMENTAL 1:** Toda decisão técnica (CRS, encoding, schema, fonte
> de dados, etc.) deve ser validada por **(A) citação literal de fonte oficial**
> + **(B) evidência empírica reproduzível**. Nunca justificar por conhecimento
> pré-treinado. Detalhes em [.claude/rules/evidence-based-decisions.md](.claude/rules/evidence-based-decisions.md).

> **REGRA FUNDAMENTAL 2:** Antes de implementar QUALQUER mudança: **INVESTIGAR
> → PLANEJAR → TESTAR** em loop até ter absoluta certeza. NUNCA implementar
> sem teste prévio que passe. Detalhes em [.claude/rules/investigate-plan-test-loop.md](.claude/rules/investigate-plan-test-loop.md).

## O que é este projeto
Pipeline R com `targets` que baixa, processa e padroniza dados geoespaciais
brasileiros (shapefiles → Parquet) para o pacote `geobr` (IPEA).
Output: Parquet comprimidos com zstd → GitHub Releases de `ipeaGIT/geobr`
via `piggyback`.

## Stack
R · targets · tarchetypes · sf · arrow/geoarrow · lwgeom · crew · renv · piggyback · geocodebr · sfarrow

## Arquivos-chave
- `_targets.R`                    — entrypoint do pipeline
- `R/support_harmonize_geobr.R`   — harmonize_geobr() e funções auxiliares
- `R/support_fun.R`               — download_file(), readmerge_geobr(), etc.
- `R/upload.R`                    — upload_arquivos() via piggyback
- `R/[dataset].R`                 — download_X() + clean_X() por dataset
- `ainda_sem_targets/`            — ~30 scripts legados SEM integração targets

## Padrão obrigatório de cada dataset
```r
tar_target(name = years_X,  command = c(ano1, ano2, ...))
tar_target(name = X_raw,    command = download_X(years_X),
           pattern = map(years_X))
tar_target(name = X_clean,  command = clean_X(X_raw, years_X),
           pattern = map(X_raw, years_X), format = 'file')
```
Output: `./data/[dataset]/[year]/[dataset]_[year].parquet`
     e: `./data/[dataset]/[year]/[dataset]_[year]_simplified.parquet`

## CRS padrão
SIRGAS 2000 — EPSG:4674. Sempre MULTIPOLYGON. `geometry` sempre a última coluna.
Simplificação: `simplify_temp_sf(temp_sf, tolerance = 100)`.

## Leitura de Parquet com geometria (OBRIGATÓRIO)
Arquivos Parquet que contêm coluna de geometria (sf) **devem** ser lidos com
`read_geoparquet()` definida em `R/support_harmonize_geobr.R`.
`arrow::read_parquet()` retorna a geometria como `geoarrow_vctr`, que NÃO é
reconhecido por `sf::st_as_sf()`. A helper faz a conversão correta:
```r
read_geoparquet(files)
# Implementação atual (2026-04-02):
# 1. Tenta sfarrow::st_read_parquet() (preserva CRS de parquets sfarrow)
# 2. Fallback: arrow::open_dataset(files) |> sf::st_as_sf()
```
**NUNCA** usar `arrow::read_parquet() |> sf::st_as_sf()` direto — vai falhar.

## Convenção de colunas (OBRIGATÓRIA)
- `code_*` = numérico · `name_*` = character (Title Case) · `abbrev_state` = 2 letras
- `code_state` = sempre os 2 primeiros dígitos do código do dataset
- Ordem: code_X → name_X → code_state → abbrev_state → name_state →
         code_region → name_region → year → geometry

Ver detalhes em `.claude/rules/column-conventions.md`.

## Bugs ativos
- **~49 targets outdated** por hashes de funções compartilhadas (commits do Rafael:
  `readmerge_geobr clean_names`, `download paralelo`, `fix historical states`,
  `regioes de saude`). Precisam rebuildar.

Bugs anteriores (21 + bugs sessão 11) resolvidos. Ver `.claude/PROBLEMS.md` e `.claude/BACKLOG.md`.

## Como rodar o pipeline
```r
library(targets)
targets::tar_make()                                            # rodar tudo
targets::tar_visnetwork()                                     # visualizar DAG
targets::tar_meta(fields = warnings, complete_only = TRUE)   # ver erros
```

## Como implementar um novo dataset
1. Criar `R/[dataset].R` com `download_X(year)` e `clean_X(raw, year)`
2. Adicionar 3 targets em `_targets.R` (ver padrão acima)
3. Adicionar em `all_files` no bloco END de `_targets.R`
4. Checar se target comentado também está comentado em `all_files`

Checklist completo em `.claude/rules/new-dataset.md`.

## Fontes de dados proibidas (OBRIGATÓRIO)
- **basedosdados.org** (Base dos Dados) — NÃO é fonte oficial. Proibido usar
  em qualquer lugar do projeto.
- **Transparência Brasil** — NÃO é fonte válida para este projeto.
- Usar APENAS fontes oficiais: IBGE, INEP, DATASUS, MMA, FUNAI, IPEA, etc.
- **schools (escolas)**: fonte é microdados do Censo Escolar
  (download.inep.gov.br) + geocodificação via geocodebr (CNEFE/IBGE).
  Oracle BI INEP: acesso público permanentemente revogado (2026-04-01),
  scraper SOAP preservado em R/schools.R mas inativo.
  schools_bi (#09b): CSV do Oracle BI baixado manualmente (data-raw/).
  GPKG do geobr: proibido como fonte permanente.
  Ver `.claude/plans/2026-04-01_schools-inep-scraper.md` para histórico.

## Encodings por período (shapefiles IBGE)
| Período   | Encoding     |
|-----------|--------------|
| 2000      | IBM437       |
| 2001-2010 | WINDOWS-1252 |
| 2013+     | UTF-8        |

## Regras de git (OBRIGATÓRIAS)
- **Nenhum commit pode conter qualquer referência ao uso de IA/Claude** —
  nem em mensagem, nem em trailers (`Co-Authored-By`, `Generated-by`, etc.)
- Commits devem parecer trabalho humano normal: mensagem direta, sem assinaturas de IA
- `CLAUDE.md` e `.claude/` SÃO versionados (fazem parte do repo)
- `.claude/settings.local.json` fica no `.gitignore` (configurações locais)
- `memory/` (fora do repo, em `~/.claude/projects/`) não é versionado

Ver `.claude/rules/git-commits.md` para exemplos.

## Ambiente R no Windows (REGRAS ABSOLUTAS)
- **Rscript**: `/c/Program Files/R/R-4.5.0/bin/Rscript.exe` (sempre a versão mais recente)
- **NUNCA usar versão antiga do R** (ex: R-4.4.2). Sempre a mais recente.
- **NUNCA rodar scripts R inline com `-e`** (causa segfault no Windows)
- **NUNCA usar heredoc no bash (`cat > file << 'EOF'`)** para escrever scripts R.
  Heredocs corrompem escapes de regex (`\\.` → `\.`) e causam erros de parse.
- **SEMPRE usar a ferramenta Write** para criar arquivos `.R` temporários:
  1. `Write` para criar `/tmp/test.R` com o conteúdo completo
  2. `Bash` para executar: `"/c/Program Files/R/R-4.5.0/bin/Rscript.exe" /tmp/test.R`
  3. `Bash` para limpar: `rm /tmp/test.R`

## Onde buscar mais contexto
- `.claude/rules/evidence-based-decisions.md` — **regra fundamental** sobre citar + validar
- `.claude/rules/investigate-plan-test-loop.md` — **REGRA FUNDAMENTAL 2**: loop obrigatório
- `.claude/rules/minimal-changes.md`  — **regra**: mudanças mínimas e cirúrgicas
- `.claude/rules/testing-protocol.md` — **regra**: testar antes de implementar
- `.claude/BACKLOG.md`         — status de todos os 36 datasets
- `.claude/PROBLEMS.md`        — 21 bugs resolvidos com histórico
- `.claude/rules/`             — new-dataset, column-conventions, harmonization, plan-first
- `.claude/plans/`             — planos concluídos (histórico)
- `reports/`                   — relatórios técnicos detalhados por dataset
- `memory/common-issues.md`   — armadilhas recorrentes e soluções
