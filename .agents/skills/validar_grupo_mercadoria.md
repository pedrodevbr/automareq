---
description: Análise e Classificação do Grupo de Mercadorias (GRPM)
---

# Grupo de Mercadoria

**Objetivo:** Interpretar a descrição do material (Texto Breve / Denominação) e verificar se o agrupamento atual em `Grupo_Mercadoria` (formato XXYYZZ) é o mais adequado perante as alternativas disponíveis.

## Instruções

1. **Obter as informações essenciais:**
   - `Codigo_Material` (Identificador)
   - `Texto_Breve_Material` (Descrição curta do material)
   - `Grupo_Mercadoria` (O código de 6 caracteres ao qual pertence atualmente, ex: 101010)
   - Tabela oficial de grupos (Mapeamento Código -> Descrição da Classe de Material)

2. **Avaliar a compatibilidade do código:**
   - O formato atual deve estar estritamente no formato CÓDIGO_NÚMERICO sem letras (ex: '1A' é inválido).  
     Caso encontre divergência no formato -> `[GRPM] Altere o Grupo_Mercadoria. Formato de grupo inválido.`
   
3. **Decisão do LLM - Semantic Match:**
   - Ao receber as informações detalhadas, cruze o texto breve com as instâncias textuais de suas categorias/classes.
   - O modelo deve eleger:
      a) **[MANTER]**: O grupo provido já está totalmente alinhado à natureza do item.  
      b) **[TROCAR]**: Há um grupo flagrantemente mais apropriado. Retorne a Justificativa, o Novo Código recomendado e a Nova Descrição e emita: `[GRPM] Altere o Grupo_Mercadoria para: {novo_codigo} ({nova_descricao}). Motivo: {justificativa}`  
      c) **[INCERTO]**: Ambiguidade extrema ou a descrição carece de especificidade para determinar o item em uma classe. Emita: `[GRPM] Revise o Grupo_Mercadoria. Motivo: {justificativa}`
