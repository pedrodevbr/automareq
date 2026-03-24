---
description: Avaliação de Imagem do Item Industrial
---

# Avaliar Mídia Associada ao Material

**Objetivo:** Obter a foto da peça no repositório oficial `P:\\Mfotos\\Padronizadas\\` correspondente ao material da análise e, por meio de um modelo de visão computacional, auditar as especificações de imagem (nitidez, validade, qualidade da exibição).

## Instruções

1. **Localização e Formato:**
   - Buscar a imagem com o código oficial de material, adicionado do status `(A)` de arquivo principal (ex. `{Codigo_Material}(A).jpg`).
   - Se a imagem estiver ausente no diretório base, submeta o seguinte alerta:
     `[IMG] Adicione uma imagem padronizada válida. O arquivo não foi encontrado.`

2. **Decisão do LLM Visão - Qualidade de Imagem:**
   - Enviar a mídia decodificada em Base64 para análise com a flag `Model (ex: gemini-3.1-flash-lite)`.
   - **Métricas e Parâmetros de Avaliação:**
     - **BOA:** Imagem nítida, boa iluminação, fundo limpo, produto claramente identificável sem ranhuras.
     - **ACEITAVEL:** Qualidade tolerante, leve desfoque mas que permite compreensão técnica e manuseio do analista de catálogo (pequenas rasuras ou sombras).
     - **SUBSTITUIR:** Condição extrema: Imagem desfocada a perder identificação, muito amarelada ou claramente com sujeiras, distorcida e imprópria para banco de dados industrial.
   - O LLM deve responder exclusivamente por `{img_qualidade: BOA | ACEITAVEL | SUBSTITUIR, img_motivo: texto}`

3. **Reportar (Ação no Dashboard):**
   - Caso `img_qualidade == "SUBSTITUIR"`, retorne o alerta principal detalhado:
     `[IMG] Substitua a imagem. Motivo: {img_motivo}`.
