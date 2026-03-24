---
description: Avaliação de Lead Time do Material
---

# Avaliar Lead Time

**Objetivo:** Verificar se o lead time de um material (Prazo_Entrega_Previsto) está condizente com as regras de negócio para itens de prateleira (off-the-shelf) ou itens críticos (sob encomenda).

## Instruções

1. **Obter as informações:**
   - `Prazo_Entrega_Previsto` (em dias)
   - Se é um material crítico ou sob encomenda (ex: Grupo de Mercadoria, Observações).

2. **Aplicar a lógica de validação:**
   - Se o lead time for **0**, levante um alerta contendo `Motivo: Lead time é igual a zero, o que requer validação junto a suprimentos.`
   - Se o lead time for **maior que 180 dias**, verifique se o material é reconhecidamente sob encomenda/crítico. Caso contrário, levante: `Motivo: Lead time excede 180 dias sem justificativa clara no escopo do fornecimento.`

3. **Reportar (Ação no Dashboard):**
   - Caso o item possua lead time inválido, gere o rótulo de validação da seguinte forma:
     `[LT] Altere o Prazo_Entrega_Previsto. Motivo: <seu alerta aqui>`
   - Para lead times corretos, marque a métrica de LT como OK.
