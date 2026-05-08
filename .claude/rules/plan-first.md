# Fluxo Plano-Primeiro

**Para qualquer tarefa não-trivial, entrar em modo planejamento antes de escrever código.**

## Pré-requisito: Loop Investigar → Planejar → Testar

**ANTES** de entrar em modo plano, aplicar o loop obrigatório de
[investigate-plan-test-loop.md](investigate-plan-test-loop.md):
investigar com evidência, planejar mudança mínima, testar em toy example.
Só prosseguir quando o teste passar.

## O Protocolo

1. **Loop I→P→T** — investigar, planejar, testar até certeza absoluta
2. **Entrar em modo plano** — usar `EnterPlanMode`
3. **Consultar MEMORY.md** — ler entradas relevantes à tarefa
4. **Redigir o plano** — quais mudanças, em quais arquivos, em que ordem
5. **Salvar em disco** — gravar em `.claude/plans/YYYY-MM-DD_descricao-curta.md`
6. **Apresentar ao usuário** — aguardar aprovação via `ExitPlanMode`
7. **Implementar** — seguir o plano aprovado
8. **Atualizar backlog** — marcar itens concluídos em `.claude/BACKLOG.md`

## Planos em Disco

Planos sobrevivem à compressão de contexto. Salvar todo plano em:

```
.claude/plans/YYYY-MM-DD_descricao-curta.md
```

Formato do arquivo de plano:

```markdown
# Plano: [título]

**Status**: RASCUNHO | APROVADO | CONCLUÍDO
**Data**: YYYY-MM-DD
**Dataset**: [nome do dataset ou "infraestrutura"]

## Objetivo
[O que será feito e por quê]

## Abordagem
[Como será feito]

## Arquivos a modificar
- [ ] R/[dataset].R — [o que muda]
- [ ] _targets.R — [o que muda]

## Verificação
- [ ] `targets::tar_make(names = "[dataset]_clean")` roda sem erro
- [ ] Parquet gerado em `data/[dataset]/[year]/`
- [ ] Colunas corretas: `names(ds)` + `sf::st_crs(ds)$epsg == 4674`
- [ ] `.claude/BACKLOG.md` atualizado
- [ ] `.claude/PROBLEMS.md` atualizado se bug foi resolvido
```

## O que é não-trivial (requer plano)

- Implementar novo dataset (download + clean + targets)
- Corrigir bug em `support_harmonize_geobr.R` (afeta múltiplos datasets)
- Modificar `_targets.R` (descomentar/comentar blocos)
- Migrar script de `ainda_sem_targets/` para o pipeline
- Qualquer mudança que afete mais de 2 arquivos

## O que é trivial (sem plano necessário)

- Corrigir typo ou nome de variável em arquivo único
- Ajustar URL de download em `download_X()`
- Adicionar comentário ou documentação
- Atualizar `.claude/BACKLOG.md` ou `.claude/PROBLEMS.md`

## Gestão de Contexto

- Preferir auto-compressão a `/clear`
- Salvar contexto importante em `memory/` antes de perdê-lo
- `/clear` apenas quando o contexto estiver genuinamente poluído
- Ao final de tarefas longas, atualizar `MEMORY.md` com descobertas relevantes
