# Rule: Decisões Técnicas Baseadas em Evidências

**Status:** OBRIGATÓRIA
**Estabelecida:** 2026-04-11
**Escopo:** Todo o projeto geobr_prep_data, todas as decisões técnicas

## Princípio

Nenhuma decisão técnica no projeto pode ser justificada apenas pelo conhecimento
pré-treinado do modelo ou por "eu sei que é assim". Toda decisão que afete o
pipeline, os dados produzidos ou a interpretação de um dataset deve ser sustentada
por **DUAS fontes de validação simultâneas**:

**(A) Citação literal de fonte oficial** — URL + trecho copiado verbatim.
**(B) Evidência empírica reproduzível** — script R que lê o dado real e mostra
propriedades observáveis (bbox, nrow, CRS, tipos de coluna, valores únicos, etc.).

Se apenas uma das duas está disponível, a decisão é **tentativa** e deve ser
marcada como tal no código (comentário `# TENTATIVE:`) e no plano correspondente.

## O que conta como "decisão técnica"

- Atribuição de CRS (EPSG) a um shapefile sem `.prj` válido
- Escolha de encoding para leitura de shapefile
- Decomposição de códigos hierárquicos (ex: UF+MUNI+DIST)
- Mapeamento de colunas originais para colunas geobr (`code_muni`, `name_muni`, …)
- Critérios de filtragem (ex: remover um setor específico)
- Decisões sobre união, deduplicação, dissolução
- Escolha de fonte de dados (FTP A vs FTP B)
- Escolha de escala/tolerância de simplificação

## Fontes oficiais aceitas

- **FTP IBGE:** `https://geoftp.ibge.gov.br/...`
- **Documentação IBGE publicada:** PDFs/DOCs do FTP IBGE ou do site
  `ibge.gov.br/geociencias/`
- **INEP:** `https://download.inep.gov.br/` e microdados oficiais
- **DATASUS:** `https://datasus.saude.gov.br/` e `ftp.datasus.gov.br/`
- **IPEA / ipeaGIT:** GitHub Releases de `ipeaGIT/*` (geobr, censobr, etc.)
- **FUNAI, MMA, ICMBio, DNIT:** sites oficiais governamentais (`.gov.br`)
- Outras fontes listadas em [CLAUDE.md](../../CLAUDE.md)

## Fontes NÃO aceitas

- **basedosdados.org** (proibido por CLAUDE.md)
- **Transparência Brasil** (proibido por CLAUDE.md)
- Tutoriais de terceiros, blogs, Stack Overflow (podem ser referenciados como
  pista, mas não como justificativa final)
- Conhecimento pré-treinado do LLM ("é padrão usar X", "geralmente Y")
- Wikipedia (pode ser ponto de partida mas não prova)

## Formato obrigatório de citação literal

```markdown
> "Sistema de Projeção Policônica - projetado. Latitude origem: 0° = Equador;
>  Longitude origem: 54° W Gr."

— `referencias_metodologicas.doc`, extraído de
https://geoftp.ibge.gov.br/.../censo_2000/setor_rural/documentacao/referencias_metodologicas.zip
(acessado em 2026-04-11)
```

Elementos obrigatórios:
1. Blockquote com o texto LITERAL (sem paráfrase)
2. Nome do arquivo ou seção de origem
3. URL completa de onde foi obtido
4. Data de acesso
5. Se for um arquivo local (baixado), informar também o hash ou tamanho

## Formato obrigatório de evidência empírica

```r
library(sf)
x <- sf::st_read("caminho/ao/shapefile.shp", quiet = TRUE,
                 options = "ENCODING=IBM437")
sf::st_bbox(x)
#> xmin: -73.99   ymin: -11.14
#> xmax: -66.62   ymax:  -7.11
# Rio Branco-AC está em ~(-67.81, -9.97), dentro do bbox: CRS é lat/lon.
```

Elementos obrigatórios:
1. Código R completo e auto-contido (com `library()` necessário)
2. Output literal (não paráfrase)
3. Interpretação curta conectando a evidência à decisão
4. Se possível, path reproduzível (absoluto ou dentro do projeto)

## Aplicação retroativa: casos corretos (do census_tract 2000)

### Exemplo 1 — CRS do rural IBGE 2000 (WGS84/SAD69 lat/lon, não Policônica)

**(A) Citação (parcial, o IBGE diz Policônica):**
> "Sistema de Projeção Policônica - projetado. Latitude origem: 0° = Equador"
>
> — `referencias_metodologicas.doc`

**(B) Evidência empírica (desmente o texto para este arquivo específico):**
```r
x <- sf::st_read("ac_setores_censitarios/12SE500G.shp")
sf::st_crs(x)                          #> NA (sem .prj)
sf::st_bbox(x)                         #> xmin=-73.99, xmax=-66.62
                                        #> ymin=-11.14, ymax=-7.11
# Coordenadas estão em graus decimais, não em metros.
# Rio Branco-AC centro: -67.81, -9.97 → dentro do bbox.
# Conclusão: arquivo está em lat/lon (EPSG:4618 SAD69 geographic),
# não em Policônica como o documento sugere.
```

**Decisão final:** atribuir EPSG:4618 e reprojetar para 4674. O conflito entre
A e B foi resolvido pela evidência empírica — a documentação descreve o
produto "oficial" abstrato, mas o arquivo específico baixado está em lat/lon.

### Exemplo 2 — Estrutura do `code_tract` (15 dígitos)

**(A) Citação:**
> "os dois primeiros dígitos se referem ao código do Estado; os cinco
> subsequentes se relacionam ao Município; os dois seguintes indicam o
> Distrito; os dois na sequência apontam o Subdistrito; os quatro últimos ao
> Setor Censitário."
>
> — Site IBGE, 26565-malhas-de-setores-censitarios

**(B) Evidência empírica:**
```r
library(censobr); library(dplyr)
d <- read_tracts(year = 2000, dataset = "Basico") |> collect()
ex <- d |> filter(code_muni == 1200203) |> head(1)
# ex$code_tract == "120020305000001"
# substr(ex$code_tract, 1, 2)  == "12"      == ex$code_state
# substr(ex$code_tract, 1, 7)  == "1200203" == ex$code_muni
# substr(ex$code_tract, 8, 9)  == "05"      (distrito)
# Validado 100% em 215.811 setores.
```

**Decisão final:** decompor `code_tract` via `substr()` nas posições descritas.

### Exemplo 3 — EPSG SAD69 UTM para shapefiles urbanos

**(A) Citação:**
> "ZONE 18S" / "PROJECTION UTM" / "UNITS METERS"
>
> — Arquivo `1200203.PRJ` baixado do FTP IBGE

**(B) Evidência empírica:**
```r
sf::st_crs(29188)$proj4string
#> "+proj=utm +zone=18 +south +ellps=aust_SA
#>  +towgs84=-57,1,-41,0,0,0,0 +units=m +no_defs"
# ellps=aust_SA = SAD69. Fórmula: 29170 + zone.
```

**Decisão final:** parser custom `detect_utm_zone_from_prj()` + atribuição
`29170 + zone` para zonas 11..25.

## Aplicação retroativa: casos INCORRETOS (contraexemplos)

### Contraexemplo 1 — "Assumi SIRGAS 2000 porque é o padrão moderno"

Errado: `sf::st_crs(x) <- 4674` sem verificar o datum original. O dado de 2000
é SAD69 (EPSG:4618) e atribuir 4674 diretamente produz um offset de ~50m em
latitude. **Correção:** atribuir 4618 e depois `st_transform(4674)`.

### Contraexemplo 2 — "O shapefile tem UTF-8 porque é moderno"

Errado: os shapefiles IBGE 2000 estão em IBM437 (confirmado por
`file 1200203.DBF` → "FoxBase+/dBase III DBF"). Abrir com UTF-8 produz
caracteres acentuados quebrados em `name_muni`. **Correção:** usar
`options = "ENCODING=IBM437"` explicitamente.

### Contraexemplo 3 — "O Censo 2000 tem X setores" (sem citar fonte)

Errado: afirmar "o Brasil tem ~215 mil setores em 2000" sem validação é
irresponsável. **Correção:** ler `censobr::read_tracts(year = 2000)` e
verificar `nrow()`:
```r
d <- censobr::read_tracts(year = 2000, dataset = "Basico") |> collect()
nrow(d)  #> 215811
```

## Integração com o workflow de plano

Em todo plano salvo em [.claude/plans/](../plans/), cada decisão técnica deve
ter um bloco estruturado:

```markdown
### Decisão: <título>

**Fonte oficial (A):** <URL + citação literal>
**Evidência empírica (B):**
\`\`\`r
<código R reproduzível>
\`\`\`
\`\`\`
<output observado>
\`\`\`
**Conclusão:** <o que será feito no código>
```

Planos que não seguem esse formato devem ser rejeitados na review.

## Integração com o workflow de agentes

Quando um Explore agent retornar afirmações sobre dados, **sempre exigir**
que ele:
1. Baixe o arquivo em questão (ou mostre o URL exato onde foi baixado)
2. Rode um script R que imprime as propriedades (names, class, nrow, bbox, CRS)
3. Colará o output literal na resposta

Agentes que retornam apenas "é padrão que X" sem prova devem ser instruídos a
re-rodar com evidência, ou suas conclusões devem ser re-validadas pelo orquestrador.

## Exceções permitidas

- **Sintaxe R/pacotes:** não é necessário citar documentação para uso básico de
  `dplyr::select()`, `sf::st_read()`, etc. São ferramentas, não dados.
- **Convenções do projeto:** referências internas a [CLAUDE.md](../../CLAUDE.md)
  ou `.claude/rules/` são aceitas sem validação empírica adicional.
- **Refactor interno:** mudar o nome de uma variável local não precisa de
  citação nem evidência, a menos que a semântica mude.

## Checklist de review de PRs

Antes de aprovar uma mudança que toca dados:
- [ ] Toda URL de fonte externa aparece no código ou em comentário adjacente
- [ ] Toda decisão não óbvia tem citação + evidência empírica no plano ou commit msg
- [ ] `stopifnot()` no código valida propriedades críticas (CRS, nrow, tipos)
- [ ] Nenhum CRS é atribuído "no escuro" — sempre justificado por .prj ou bbox
- [ ] Nenhum encoding é escolhido sem referência ao período IBGE ([R/support_fun.R:117-127](../../R/support_fun.R))

## Referências cruzadas

- [CLAUDE.md](../../CLAUDE.md) — convenções gerais do projeto
- [.claude/rules/plan-first.md](plan-first.md) — fluxo de planejamento
- [.claude/rules/harmonization.md](harmonization.md) — uso correto de `harmonize_geobr()`
- [.claude/rules/column-conventions.md](column-conventions.md) — padrão de colunas
- [reports/census_tract_2000_implementation.md](../../reports/census_tract_2000_implementation.md) —
  exemplo completo de aplicação desta rule em um dataset real
