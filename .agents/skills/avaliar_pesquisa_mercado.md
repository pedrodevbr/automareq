---
description: Análise de Mercado de Materiais (Busca Web)
---

# Avaliar Similaridade e Encontrar Preços e Nomes

**Objetivo:** Automatizar buscas Web para atestar se a referência de um material é uma peça válida de fornecedor, confirmar o nome comercial da marca, a consistência descritiva (Cobertura) nas documentações e encontrar preços estimados do mercado (R$, US$).

## Instruções

1. **Obter as informações necessárias:**
   - Montar uma string de pesquisa Web composta do `Texto_Breve_Material` + `Numero_Peca_Fabricante` ou PNs identificados.
   - Definir regras para busca focada, omitindo sufixos internos da empresa se existir e priorizando links de fornecedores ou E-commerces B2B.

2. **Decisão do Agente Web e LLM Extrator:**
   - Uma pesquisa multi-engine deve buscar resultados comerciais, abstraindo documentação não comercial.
   - Extraia as propriedades encontradas em formato serializado (ex: dict Python):
     - `reference_found`: (T/F - O Modelo X da marca Y foi validado positivamente em catálogos mundiais?)
     - `price_estimated`: Retornar montante e moeda corrente.
     - `text_coverage`: ("TOTAL", "PARCIAL", "INCOMPATIVEL" - A descrição local bate com o sumário técnico do site encontrado?) Se a cobertura for parcial ou pior, descreva os *gaps*.
     - `url_fornecedor`: URL final e explícita do fornecedor para auditoria.
     - `part_number_confirmed`: Confirmar que o código catalogado internamente bate com as referências globais do E-commerce (ex: "FALSE", "TRUE").
   
3. **Avaliar e Reportar (Ações ao Dashboard):**
   - Caso `part_number_confirmed` for falso: `"Revise o PN, part number não confirmado na pesquisa."`
   - Caso `text_coverage` for desfasado (PARCIAL ou pior): `"Adicione informações adicionais nas descrições. Cobertura textual: {ref_cov}. {gaps}"`
   - Caso geral: Sempre informe na notificação final `"Verifique a referência do mercado. {is_valid}"` e alerte de todas as nuances encontradas.
