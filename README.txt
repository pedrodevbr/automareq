SAP Procurement Agent — Quick Start
======================================

WHAT IT DOES
  Reads a SAP materials Excel, uses Claude AI + web search to find/validate
  manufacturer part numbers for each item, then outputs an Excel with two sheets:
    - "Análise"          Full analysis with color-coded status per item
    - "SAP_ME51N_Import" Ready to import into SAP MM (ME51N Purchase Requisition)
    - "Resumo"           Status summary counts


1. INSTALL DEPENDENCIES
-----------------------
Make sure Python 3.9+ is installed, then run:

    pip install -r requirements.txt


2. CONFIGURE API KEY (OpenRouter)
----------------------------------
Sign up free at https://openrouter.ai — you get credits to start.
Copy your API key (starts with sk-or-...).

Option A — Environment variable (recommended):
    Windows:   set OPENROUTER_API_KEY=sk-or-...
    Mac/Linux: export OPENROUTER_API_KEY=sk-or-...

Option B — Edit config.json:
    "openrouter_api_key": "sk-or-..."

OpenRouter lets you choose any model (see config.json → "model_options").
Default: anthropic/claude-sonnet-4-5  (great quality/cost balance)


3. PREPARE YOUR INPUT EXCEL
----------------------------
The input Excel should have these columns (all optional except Texto_PT):

    Codigo_Material           SAP material code
    Texto_Breve_Material      Short description (max 40 chars)
    Texto_PT                  Full technical description in Portuguese ← KEY FIELD
    Numero_Peca_Fabricante    Manufacturer part number (current in SAP)
    Texto_Observacao_PT       Additional observations or partial references
    Texto_Dados_Basicos_PT    If filled → proprietary item, skip web search
    Preco_Unitario            Current SAP price in USD
    Responsavel               Responsible code (PT for BR, ES for PY)
    Unidade                   Unit of measure (defaults to UN)

See sample_input.xlsx for an example with 6 items.


4. RUN THE AGENT
----------------
Basic usage (output auto-named):
    python procurement_agent.py sample_input.xlsx

Specify output file:
    python procurement_agent.py materials.xlsx --output pr_ready.xlsx

Resume after a failure (skip first 50 rows):
    python procurement_agent.py materials.xlsx --start-row 50

Slow down if hitting API rate limits:
    python procurement_agent.py materials.xlsx --delay 4.0

See all options:
    python procurement_agent.py --help


5. UNDERSTAND THE OUTPUT
------------------------
Sheet "Análise" — color coding:
    Green   (OK)          Reference confirmed, specs match
    Yellow  (VERIFY/TYPO) Found but needs manual check or has a typo
    Red     (MISSING/CONFLICT/OVERPRICED) Needs action
    Orange  (INCOMPLETE)  Partial reference found
    Lavender (PROPRIETARY) Sole-source item, no web search done
    Grey    (NO_MATCH)    Nothing found

Sheet "SAP_ME51N_Import" — key fields:
    Nro_Peca_Fabricante → Updated with web-found reference (if HIGH/MEDIUM confidence)
    Preco_Unitario_USD  → Market price from web search
    Status_Analise      → Use to filter which rows need attention before import
    URL_Verificacao     → Source URL to verify the reference


6. SAP ME51N IMPORT
--------------------
To import the "SAP_ME51N_Import" sheet into SAP:
  - Use LSMW (Legacy System Migration Workbench) with BAPI_PR_CREATE
  - Or use the custom Z_CREATE_PR program if your organization has one
  - Review all CONFLICT/VERIFY rows before importing
  - Update Quantidade and CentroC_ElemPEP fields as needed per requisition


7. RESPONSIBLE LANGUAGE MAPPING
---------------------------------
Responsavel codes → language for "Acao_Especifica":
    PEDROHVB, VICKY, MTSE → Português (Brazil)
    DGOMEZ, HERMESI, GYEGROS, LUCASD, ACOSTAJ → Español (Paraguay)

Add more codes to PY_RESPONSAVEIS set in procurement_agent.py as needed.


8. TIPS FOR LARGE BATCHES
--------------------------
- Checkpoints auto-saved every 25 items (files: *_checkpoint_25.xlsx, etc.)
- Use --start-row to resume from any checkpoint if something fails
- Increase --delay if you see RateLimitError messages (default 2.5s)
- Items with Texto_Dados_Basicos_PT filled are skipped (no API call, faster)
- Items marked MISSING with price > USD 200 get the most thorough search

======================================
Questions? Edit config.json for SAP defaults (plant, purchasing group, etc.)
