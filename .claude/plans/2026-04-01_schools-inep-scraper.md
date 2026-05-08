# Plano: Scraper INEP para Escolas

**Status**: CONCLUÍDO (Fase 2 ativa: microdados INEP + geocodebr). Fase 1 Oracle BI permanentemente bloqueada pelo INEP.
**Data**: 2026-04-01
**Dataset**: schools

## Contexto

O R/schools.R atual depende de GPKG pré-processados do geobr (IPEA) — inaceitável.
Objetivo: fonte de dados INEP direto, sem intermediários.

## Fase 1: Oracle BI (PAUSADA — bloqueio servidor)

### O que funciona
- SOAP auth em `inepdata.inep.gov.br/analytics-ws/saw.dll` ✓
- SOAP logon em `anonymousdata.inep.gov.br/analytics-ws/saw.dll` ✓
- `executeSQLQuery` funcional (SQL lógico OBIEE) ✓
- Subject area `"Escola"` (93 cols) existe ✓
- Subject area `"Docente"` (5 cols) existe ✓
- Subject area `"Aluno"` (75 cols) existe ✓
- `SELECT 1 FROM "Escola"` retorna dados ✓
- `SELECT "Categoria Administrativa" FROM "Escola"` retorna dados ✓
- chromote/headless Chrome disponível ✓

### Colunas descobertas no SA "Escola"
Presentation table: `"Escola"` (todas as colunas pertencem a essa tabela)

| Coluna | Tipo | Valores |
|--------|------|---------|
| Categoria Administrativa | varchar(150) | Público, Privada |
| Localização | varchar(150) | Urbana, Rural |
| Dependência Administrativa | varchar(150) | Federal, Estadual, Municipal, Privada |
| Localização Diferenciada | varchar(150) | (vazio) |
| Educação Especial | varchar(150) | (vazio) |
| Total | integer | Soma (medida agregada) |

### Bloqueios

1. **SA "Escola" é AGREGADO** — retorna estatísticas, não escolas individuais
2. **`SELECT *` quebrado** — bug OBIEE: "expected 1 column, received N"
3. **Colunas ID** (código, nome, UF, lat/lon) — não encontradas em ~200 tentativas
4. **Dashboard `Catálogo de Escolas`** — "acesso negado" para user `inepdata`
5. **Dashboard `Mapa das Escolas`** — "acesso negado"
6. **URL pública** `NQUser=inepdata&NQPassword=Inep2014` — retorna **404**
7. **`anonymousdata.inep.gov.br`** — "Permissões Insuficientes" para tudo
8. **BI Publisher** — redirecionado para portal INEP
9. **Catálogo listing** — vazio (sem permissão)
10. **Go URL Format=csv** — retorna HTML (JavaScript requerido)

### Conclusão Fase 1
O INEP revogou acesso público ao Oracle BI. O user `inepdata/Inep2014` pode
fazer logon SOAP mas não tem permissão para dashboards ou subject areas com
dados individuais de escolas. O subject area acessível ("Escola") contém apenas
dados agregados.

### Como retomar
Se o INEP restaurar acesso:
1. Verificar com chromote se dashboards voltaram a ser acessíveis
2. Se sim: usar chromote para exportar CSV do dashboard
3. Se SOAP: descobrir subject area com dados individuais e nomes de colunas
4. O código SOAP base funciona — só precisa dos nomes corretos

Credenciais: `inepdata / Inep2014`
Endpoints:
- SOAP: `https://inepdata.inep.gov.br/analytics-ws/saw.dll?SoapImpl=nQSessionService`
- Dashboard: `https://inepdata.inep.gov.br/analytics/saw.dll?Dashboard&PortalPath=...`

---

## Fase 2: Microdados do Censo Escolar (ATIVA)

### Fonte
`https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_{year}.zip`
- Disponível: 1995-2025
- Formato: ZIP com CSV delimitado por pipe (|)
- Encoding: Latin-1 (ISO-8859-1)
- Tamanho: ~2-5 GB por ano
- Atualização: anual

### A investigar
1. Os microdados recentes (2024/2025) incluem NU_LATITUDE/NU_LONGITUDE?
2. Quais colunas estão disponíveis na tabela ESCOLAS?
3. Formato do dicionário de dados dentro do ZIP
4. Tamanho da subtabela ESCOLAS (separada das regiões)

### Abordagem proposta
1. Baixar ZIP do ano desejado
2. Extrair apenas o arquivo de ESCOLAS (não o de matrículas/docentes)
3. Ler CSV com readr (pipe-delimited, Latin-1)
4. Verificar presença de lat/lon
5. Se presente: mapear colunas e processar
6. Se ausente: considerar abortar e documentar

### Arquivos a modificar
- [ ] `R/schools.R` — download_schools + clean_schools
- [ ] `_targets.R` — years_schools
- [ ] `.claude/PROBLEMS.md` — BUG-014
- [ ] `.claude/BACKLOG.md` — schools status

## Verificação
- [ ] Dados baixam corretamente
- [ ] Colunas mapeadas para padrão geobr
- [ ] CRS 4674 (se lat/lon presente: 4326 → st_transform)
- [ ] Geometria POINT
- [ ] `tar_make(names = "schools_clean")` sem erro
