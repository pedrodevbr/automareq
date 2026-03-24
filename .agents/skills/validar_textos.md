---
description: Análise de Textos de Materiais (Texto Breve, Longo e Traduções)
---

# Auditoria de Textos (PT/ES)

**Objetivo:** Garantir que a narrativa estrutural (Abreviações Corretas, Informações Técnicas Claras, Concordância Gramatical) seja devidamente respeitada nos campos Textuais. 

## Instruções

1. **Obter as informações essenciais:**
   - `Texto_Breve_Material` (Até 40 Caracteres)
   - `Texto_Dados_Basicos_PT`, `Texto_Dados_Basicos_ES` (Descrição Completa de Catálogo)
   - `Texto_ES`, `Texto_PT` (Qualquer das frentes correspondente ao idioma Espanhol e Inglês (quando requerido))
   - Regras do PDM (Product Data Management)

2. **Avaliar Qualidade & Aderência ao PDM:**
   - Verificar as regras normativas gramaticais e acrónimos obrigatórios para nomes de equipamentos (por ex. `VÁLVULA` vs `VALV`). 
   - Procurar por inconsistências nos tamanhos (ex: Text_PT vazio e Texto Breve preenchido).
   - O modelo deve compilar os gaps e fornecer sugestões gramaticais: `Análise de Texto e Padrões`.

3. **Reportar ao Usuário:**
   - Na lista de falhas, levante a notificação: `[TXT] Corrija os textos (Texto_PT / Texto_ES). Análise: <justificativas + conselho gramatical e formato>.`
