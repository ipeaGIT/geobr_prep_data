# Regra: Investigar → Planejar → Testar (Loop Obrigatório)

**Status:** OBRIGATÓRIA — NUNCA VIOLAR
**Estabelecida:** 2026-04-15
**Escopo:** Todo o projeto geobr_prep_data

## Princípio

Antes de implementar QUALQUER mudança no código de produção, é obrigatório
passar por um loop de investigação, planejamento e teste até ter **absoluta
certeza** de que o problema foi identificado e a solução funciona.

## O Loop

```
  ┌─────────────┐
  │  INVESTIGAR  │ ← Entender o problema com evidência empírica
  └──────┬──────┘
         ▼
  ┌─────────────┐
  │  PLANEJAR    │ ← Descrever a mudança mínima necessária
  └──────┬──────┘
         ▼
  ┌─────────────┐
  │  TESTAR      │ ← Validar em toy example / contexto isolado
  └──────┬──────┘
         │
         ▼
    Funcionou?
    │         │
   Não       Sim ──► IMPLEMENTAR no código real
    │
    └──► Volta para INVESTIGAR
```

**Só sair do loop quando o teste passar.**
**NUNCA implementar sem ter passado pelo loop.**

## Fase 1: INVESTIGAR

- Ler o código envolvido (funções, callers, callees)
- Identificar a causa raiz com evidência empírica
- Documentar: "O erro X acontece porque Y na linha Z do arquivo W"
- Se não conseguir reproduzir o erro: investigar mais, não adivinhar

## Fase 2: PLANEJAR

- Descrever a mudança em texto ANTES de editar qualquer arquivo
- Listar: quais linhas mudam, por que mudam, o que NÃO muda
- Verificar: quem chama a função afetada? Quem consome o output?
- Se a mudança afeta >2 arquivos: usar EnterPlanMode + plano formal

## Fase 3: TESTAR

- Criar um script R em arquivo .R (via Write tool, NUNCA heredoc)
- O script deve:
  1. Reproduzir o cenário do bug em escala mínima
  2. Aplicar a correção proposta
  3. Verificar com stopifnot() que o resultado está correto
- Rodar o script com Rscript
- Se falhar: voltar para INVESTIGAR
- Se passar: prosseguir para implementação

## Exemplos

### Bom: Loop correto

```
INVESTIGAR: "code_tract_range vira NA porque use_encoding_utf8() aplica
as.numeric() a todas as colunas code_*. Linha 290 de support_harmonize_geobr.R."

PLANEJAR: "Renomear code_tract_range → original_id para evitar o prefixo code_."

TESTAR:
  # toy_test_original_id.R
  df <- data.frame(code_muni = "1200203", original_id = "120020305000001-0074")
  df$code_muni <- as.numeric(df$code_muni)  # simula use_encoding_utf8
  stopifnot(!is.na(df$original_id))  # original_id sobrevive
  cat("PASS\n")

→ Passou? Sim → IMPLEMENTAR rename no código real.
```

### Ruim: Pular o loop

```
"Acho que o problema é X. Vou mudar Y."
→ Edita o arquivo
→ Roda tar_make
→ Falha com erro diferente
→ Edita de novo
→ Falha de novo
→ Ciclo vicioso
```

## Integração com outras regras

- **plan-first.md**: o plano formal é parte da fase PLANEJAR
- **minimal-changes.md**: a implementação deve ser mínima e cirúrgica
- **testing-protocol.md**: o teste segue o protocolo de toy examples
- **evidence-based-decisions.md**: a investigação usa evidência empírica

## Esta regra NÃO tem exceções

Mesmo para mudanças que parecem triviais:
- Renomear uma coluna? TESTAR que o rename não quebra nada.
- Adicionar uma coluna? TESTAR que ela sobrevive ao pipeline.
- Alterar um regex? TESTAR com exemplos reais.
- Reverter uma mudança? TESTAR que o código revertido funciona.
