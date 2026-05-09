# Protocolo de Recuperação de Sessão

**Após compressão de contexto ou início de nova sessão, executar este protocolo.**

## Passos

1. **Ler CLAUDE.md** na raiz do projeto (`d:/Dropbox/Artigos/geobr_prep_data/CLAUDE.md`)

2. **Ler MEMORY.md** em:
   `C:/Users/antro/.claude/projects/d--Dropbox-Artigos-geobr-prep-data/memory/MEMORY.md`

3. **Verificar planos recentes:**
   ```
   ls .claude/plans/
   ```
   Ler o plano relevante para a tarefa em andamento. Verificar status
   (RASCUNHO / APROVADO / CONCLUÍDO).

4. **Verificar estado do trabalho:**
   ```
   git status
   git log --oneline -5
   ls .claude/
   ```

5. **Relembrar regra fundamental 2:** Antes de qualquer mudança, aplicar
   o loop **Investigar → Planejar → Testar** até certeza absoluta.
   Ver `.claude/rules/investigate-plan-test-loop.md`.

6. **Declarar entendimento:** Dizer ao usuário o que entende ser o estado
   atual do projeto e da tarefa em andamento. Pedir confirmação antes de
   continuar.

## Quando ativar

- Após qualquer compressão automática de contexto
- No início de qualquer nova sessão do Claude Code
- Quando o usuário diz "continue de onde parou" ou similar
- Quando o contexto parece incompleto ou confuso

## Prioridade das fontes

1. Plano mais recente em `.claude/plans/` (mais específico)
2. `memory/common-issues.md` (problemas e soluções conhecidos)
3. `memory/project-status.md` (status de todos os datasets)
4. `memory/MEMORY.md` (notas persistentes)
5. `CLAUDE.md` (contexto e convenções do projeto)
6. `.claude/BACKLOG.md` e `.claude/PROBLEMS.md` (estado atual)
