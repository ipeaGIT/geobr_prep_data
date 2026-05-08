# Regra: Mudanças Mínimas e Cirúrgicas

**Status:** OBRIGATÓRIA
**Estabelecida:** 2026-04-15
**Escopo:** Todo o projeto geobr_prep_data

## Pré-requisito

Antes de qualquer mudança, passar pelo loop **Investigar → Planejar → Testar**
descrito em [investigate-plan-test-loop.md](investigate-plan-test-loop.md).

## Princípio

Toda alteração no código deve ser **mínima, cirúrgica e planejada**.
Nunca modificar mais do que o estritamente necessário para o objetivo
imediato. Não "aproveitar" para fazer correções ou melhorias em áreas
não solicitadas.

## Antes de editar qualquer arquivo

1. **Planejar**: descrever em texto as mudanças ANTES de fazer qualquer edit
2. **Delimitar escopo**: listar QUAIS linhas serão alteradas e POR QUÊ
3. **Verificar dependências**: toda função alterada → quem a chama?
   toda coluna adicionada → quem a consome?
4. **Medir impacto**: quantos targets são invalidados pela mudança?

## Regras de edição

- **Uma mudança por vez**: não misturar fix de bug com feature nova
- **Sem refactor oportunista**: se não foi pedido, não refatorar
- **Sem "melhorias" cosméticas**: não renomear variáveis, adicionar
  comentários ou reorganizar código que não faz parte da tarefa
- **Sem editar funções compartilhadas** (harmonize_geobr, read_geoparquet,
  etc.) sem plano aprovado — afetam TODOS os 36 datasets
- **Testar ANTES de editar o arquivo real**: criar toy example ou unit test
  que valide a mudança isoladamente

## Ordem obrigatória de implementação

```
1. Planejar (texto descritivo da mudança)
2. Criar unit test / toy example que valide a mudança
3. Rodar o test (deve passar)
4. Fazer a edição mínima no arquivo real
5. Rodar tar_make apenas no target afetado
6. Validar output
7. Commitar
```

## Funções compartilhadas protegidas

As seguintes funções NÃO devem ser alteradas sem plano aprovado:

| Função | Arquivo | Razão |
|--------|---------|-------|
| harmonize_geobr() | support_harmonize_geobr.R | Afeta todos os datasets |
| read_geoparquet() | support_harmonize_geobr.R | Afeta toda leitura de parquet |
| write_geobr_parquet() | support_harmonize_geobr.R | Afeta toda escrita de parquet |
| to_multipolygon() | support_harmonize_geobr.R | Afeta conversão de geometria |
| simplify_temp_sf() | support_harmonize_geobr.R | Afeta simplificação |
| validate_geobr() | support_harmonize_geobr.R | Afeta validação |

## O que fazer quando um teste falha

1. **NÃO** editar a função compartilhada para "resolver"
2. Investigar: o teste está errado ou a função tem um bug real?
3. Se for bug real: planejar o fix separadamente, com evidência empírica
4. Se for limitação conhecida: contornar no código do dataset, não na
   função compartilhada

## Anti-padrões proibidos

- Editar 3+ arquivos em uma única mudança sem plano aprovado
- Alterar `read_geoparquet()` para resolver um problema de um dataset
- Adicionar colunas a `clean_one_2000()` e `fill_gaps_voronoi()` e
  `clean_censustract_2000_unified()` tudo de uma vez
- Rodar `tar_make()` sem saber exatamente quais targets serão afetados
- Fazer "fix" iterativo: editar → testar → falha → editar → testar → ...
  (sinal de que faltou planejamento)

## Checklist antes de qualquer PR / commit

- [ ] Mudança é mínima e focada em UM objetivo
- [ ] Nenhuma função compartilhada foi alterada sem necessidade
- [ ] Unit test existe e passa
- [ ] `tar_make(names = ...)` roda apenas no target afetado
- [ ] Output validado (nrow, names, CRS, geom_source, etc.)
