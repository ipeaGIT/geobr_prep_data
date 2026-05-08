# Regra: Protocolo de Testes

**Status:** OBRIGATÓRIA
**Estabelecida:** 2026-04-15
**Escopo:** Todo o projeto geobr_prep_data

## Pré-requisito

Este protocolo faz parte do loop **Investigar → Planejar → Testar**
descrito em [investigate-plan-test-loop.md](investigate-plan-test-loop.md).
O teste é a fase 3 do loop. Só implementar quando o teste passar.

## Princípio

Toda mudança no código de produção deve ser precedida por um teste em
contexto restrito. Nunca testar diretamente no pipeline completo como
primeira abordagem.

## Hierarquia de testes

### Nível 1: Toy Example (obrigatório)

Script R autônomo que:
- Cria dados sintéticos OU lê um subset mínimo de dados reais
- Executa APENAS a função alterada
- Verifica o resultado com `stopifnot()` ou comparação explícita
- Roda em < 30 segundos

Localização: `tests/toy_[funcao].R`

```r
# Exemplo: tests/toy_fill_gaps_voronoi.R
library(sf)

# Criar dados de teste
poly <- st_polygon(list(matrix(c(0,0, 10,0, 10,10, 0,10, 0,0), ncol=2, byrow=TRUE)))
range_poly <- st_sf(geometry = st_sfc(poly, crs = 4674))

part1 <- st_polygon(list(matrix(c(1,1, 4,1, 4,4, 1,4, 1,1), ncol=2, byrow=TRUE)))
part2 <- st_polygon(list(matrix(c(6,6, 9,6, 9,9, 6,9, 6,6), ncol=2, byrow=TRUE)))
partitions <- st_sf(
  code_tract = c("001", "002"),
  match_method = c("code_match", "graph_divisao"),
  match_confidence = c(1.0, 0.8),
  geometry = st_sfc(part1, part2, crs = 4674)
)

# Testar
result <- fill_gaps_voronoi(partitions, range_poly)

# Verificar
stopifnot(nrow(result) == 2)
stopifnot(all(st_is_valid(result)))
stopifnot(st_crs(result)$epsg == 4674)
# Voronoi deve cobrir todo o range (dentro de tolerância)
total_area <- st_area(st_union(result))
range_area <- st_area(range_poly)
coverage <- as.numeric(total_area / range_area)
stopifnot(coverage > 0.99)
cat("PASS: fill_gaps_voronoi\n")
```

### Nível 2: Unit Test com dados reais (recomendado)

Script R que:
- Lê um subset pequeno de dados reais (1 UF, 1 município)
- Executa a função com dados reais
- Verifica propriedades do resultado
- Roda em < 2 minutos

Localização: `tests/unit_[funcao].R`

### Nível 3: Integração (só após níveis 1 e 2)

```r
tar_make(names = [target_afetado])
```

Só rodar quando os níveis 1 e 2 passaram.

## Regras Rscript no Windows

- **NUNCA usar `-e`** — causa segfault
- Sempre escrever em arquivo .R separado
- Usar `/c/Program Files/R/R-4.5.0/bin/Rscript.exe`
- Se R der segfault: verificar se há processos R pendurados (`tasklist`)
  e matar antes de re-tentar

## Quando um teste falha

1. Ler a mensagem de erro COMPLETA
2. Identificar a linha exata do erro (traceback)
3. Criar um toy example AINDA MENOR que reproduz o erro
4. Corrigir no toy example primeiro
5. Só então aplicar a correção no código real

## Diretório de testes

```
tests/
  toy_fill_gaps_voronoi.R
  toy_partition_range.R
  toy_read_geoparquet.R
  unit_rural_clean.R
  unit_unified_clean.R
```

Esses arquivos ficam no `.gitignore` (são testes locais, não parte do
pipeline de produção).
