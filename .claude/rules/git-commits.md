# Regras de Git — geobr_prep_data

## Regras absolutas

1. **Nenhum commit pode referenciar IA ou Claude** — nem na mensagem,
   nem em trailers, nem em comentários de código commitados.

   Proibido:
   ```
   Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
   Generated-by: Claude
   # sugerido por IA
   ```

2. **Arquivos do Claude SÃO versionados:**
   - `CLAUDE.md` e `.claude/` fazem parte do repositório
   - `.claude/settings.local.json` fica no `.gitignore` (configurações locais)
   - `memory/` (em `~/.claude/projects/`) NÃO é versionado

3. **Mensagens de commit devem parecer trabalho humano normal:**
   - Imperativo no presente: "fix:", "add:", "update:", "refactor:"
   - Descritivo e técnico, sem mencionar ferramentas de IA

## Formato correto

```
fix: corrige bug em to_multipolygon() que apagava abbrev_state

Filtro de colunas em support_harmonize_geobr.R:317 agora preserva
colunas com prefixo abbrev_ além de code_ e name_.
```

## Checklist antes de commitar

- [ ] `git diff --staged` — nenhuma linha menciona Claude/IA em código R
- [ ] Mensagem não contém Co-Authored-By, Generated-by ou similar