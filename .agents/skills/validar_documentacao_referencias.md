---
description: Análise de Preenchimento da Referência no PDM (Part Number e Observações)
---

# Preenchimento Obrigatório da Referência 

**Objetivo:** Verificar se a referência comercial (Part Number/Modelo/Ref Fábrica) está contida de forma adequada na documentação e metadados do Material nas ferramentas ERP/PDM.

## Regras e Lógica
1. **Verificação de Dados Básicos (PT):**
   - Caso `Texto_Dados_Basicos_PT` esteja devidamente preenchido, podemos inferir que o escopo descritivo já possui a referência e marca/modelo. Essa condição está [OK].

2. **Verificação dos campos de Observação e Fabrico (Se regra 1 falhar):**
   - Se `Texto_Dados_Basicos_PT` NÃO conter a indicação do fabricante preenchida ou estiver vago, os campos `Numero_Peca_Fabricante` (Estático) e `Texto_Observacao_PT` (Dinâmico) DEVEM, cumulativamente, conter a mesma indicação ou número de peça.
   
3. **Erros Reportáveis:**
   - Ausência em PN: Se *Numero_Peca_Fabricante* estiver vazio, reporte a falta para preenchimento posterior.  
     `"[REF] PN ausente no campo Numero_Peca_Fabricante"`
   -  Ausência em OBS (Dinâmico): Se o PN estiver preenchido em `Numero_Peca_Fabricante` mas NÃO replicado dentro do escopo geral em `Texto_Observacao_PT`, reporte que a documentação livre esqueceu esse adendo.  
     `"[REF] Referencia ausente em Texto_Observacao_PT"` ou `[OBS] Preencha o campo Texto_Observacao_PT com a referência {sugestao}`.
