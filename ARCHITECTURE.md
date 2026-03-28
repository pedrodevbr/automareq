"""
ARCHITECTURE.md — AutomaReq Pipeline Architecture & Project Flow
=================================================================

This document describes the full project structure, data flow, and module
responsibilities for the AutomaReq procurement automation pipeline.


ENTRY POINTS
============

  panel.py          → Interactive Rich CLI (recommended)
  main.py           → Linear pipeline execution (legacy)
  app.py            → Streamlit web GUI


PIPELINE STAGES (11 stages, sequential)
========================================

  ┌─────────────────────────────────────────────────────────────────┐
  │                        EXTRACTION                               │
  ├─────────────────────────────────────────────────────────────────┤
  │                                                                 │
  │  [1] SAP Export (optional)                                      │
  │      services/sap_service.py → SapManager                       │
  │      Automates SAP GUI to extract reports                       │
  │                                                                 │
  │  [2] Load (ETL)                                                 │
  │      core/load.py → process_excel_data()                        │
  │      OP.XLSX + 0127.XLSX + 0130.XLSX → merged DataFrame        │
  │      Parquet cache for fast re-loads                            │
  │                                                                 │
  ├─────────────────────────────────────────────────────────────────┤
  │                        PREPARATION                              │
  ├─────────────────────────────────────────────────────────────────┤
  │                                                                 │
  │  [3] Filter / Sample                                            │
  │      core/pipeline.py → Pipeline._run_filter()                  │
  │      test mode (N samples) vs production (all materials)        │
  │      Optional: filter by Responsavel                            │
  │                                                                 │
  ├─────────────────────────────────────────────────────────────────┤
  │                        VALIDATION                               │
  ├─────────────────────────────────────────────────────────────────┤
  │                                                                 │
  │  [4] Validation (7 substages)                                   │
  │      core/validators/orchestrator.py → run_validations()        │
  │                                                                 │
  │      4a. leadtime   → core/validators/rules.py                  │
  │          Lead time must be >0, multiple of 30, ≤720 days        │
  │                                                                 │
  │      4b. grpm       → core/validators/stages/grpm.py            │
  │          Format check (rules.py) + ML suggestion + LLM decision │
  │                                                                 │
  │      4c. texts      → core/validators/stages/texts.py           │
  │          PT/ES embedding similarity + AI audit for mismatches   │
  │                                                                 │
  │      4d. obs        → core/validators/stages/obs.py             │
  │          PN presence check + LLM reference extraction           │
  │                                                                 │
  │      4e. reference  → core/validators/stages/reference.py       │
  │          Market search via ReferenceValidator + PN confirmation  │
  │                                                                 │
  │      4f. images     → core/validators/stages/images.py          │
  │          Vision LLM evaluates photo quality (currently disabled) │
  │                                                                 │
  │      4g. ref_obs    → core/validators/rules.py                  │
  │          Cross-checks PN in observation fields                  │
  │                                                                 │
  │      → Consolidation: rules.py → consolidate_validation_summary │
  │        Produces: resumo_validacao, classificacao_validacao       │
  │                                                                 │
  ├─────────────────────────────────────────────────────────────────┤
  │                        CALCULATION                              │
  ├─────────────────────────────────────────────────────────────────┤
  │                                                                 │
  │  [5] Stock Parameter Calculations                               │
  │      core/calculate.py → run_calculations()                     │
  │                                                                 │
  │      5a. TMD (Time Mean Demand) — periods / non-zero periods    │
  │      5b. CV (Coefficient of Variation) — std / mean             │
  │      5c. Classification:                                        │
  │          CV < 0.7 + TMD < 1.32 → Suave                         │
  │          CV > 0.7 + TMD < 1.32 → Intermitente                  │
  │          CV < 0.7 + TMD > 1.32 → Errático                      │
  │          CV > 0.7 + TMD > 1.32 → Esporádico                    │
  │      5d. Outlier detection (IQR-based)                          │
  │      5e. PR (Reorder Point) — 2nd highest cleaned consumption   │
  │      5f. Annual demand from LTD slices                          │
  │      5g. Policy decision tree → ZP/ZL/ZM/ZE/ZO/ZD              │
  │      5h. MAX = PR + LEC (Economic Order Quantity)               │
  │      5i. Tributacao, Nivel de Servico, flags                    │
  │                                                                 │
  ├─────────────────────────────────────────────────────────────────┤
  │                        VISUALIZATION                            │
  ├─────────────────────────────────────────────────────────────────┤
  │                                                                 │
  │  [6] Summary / Visuals                                          │
  │      utils/visuals.py → build_summary(), render_summary()       │
  │      Statistics, distributions, Rich terminal tables            │
  │                                                                 │
  ├─────────────────────────────────────────────────────────────────┤
  │                        ANALYSIS                                 │
  ├─────────────────────────────────────────────────────────────────┤
  │                                                                 │
  │  [7] Analysis Phase 1 (optional — JIRA + SAP)                   │
  │      core/analyzers/orchestrator.py → run_analysis()            │
  │                                                                 │
  │      7a. jira_analysis → stages/jira_analysis.py                │
  │          Read all JIRA comments + LLM summary                   │
  │                                                                 │
  │      7b. smit          → stages/smit.py                         │
  │          SMIT materials: read JIRA tickets, enrich DataFrame    │
  │                                                                 │
  │      7c. frac          → stages/frac.py                         │
  │          FRAC materials: create JIRA tickets, change SAP MRP    │
  │                                                                 │
  │  [8] Analysis Phase 2 (optional — AI + Search)                  │
  │      core/analyzers/orchestrator.py → run_analysis()            │
  │                                                                 │
  │      8a. zstk → stages/zstk.py                                  │
  │          Business rules + AI batch analysis + market search     │
  │          Uses: services/ai_service.py (AIModule)                │
  │                services/search_service.py (ReferenceValidator)   │
  │                                                                 │
  │      8b. ad   → stages/ad.py                                    │
  │          Auto-decision REPOR + supplier quote text              │
  │                                                                 │
  │      8c. ana  → stages/ana.py                                   │
  │          Flag materials for manual specialist analysis          │
  │                                                                 │
  ├─────────────────────────────────────────────────────────────────┤
  │                          OUTPUT                                 │
  ├─────────────────────────────────────────────────────────────────┤
  │                                                                 │
  │  [9] Dashboard (optional)                                       │
  │      core/emitters/stages/dashboard.py                          │
  │      HTML + JS interactive dashboard per analyst                │
  │                                                                 │
  │  [10] Group Separation (optional)                               │
  │       core/emitters/stages/group_separation.py                  │
  │       Folder structure by Setor/Grupo + Tributacao              │
  │                                                                 │
  │  [11] Emission (optional)                                       │
  │       core/emitters/orchestrator.py → run_emission()            │
  │       11a. templates → stages/templates.py                      │
  │            Fill CPV / Inexigibilidade procurement templates     │
  │       11b. send      → stages/send_drafts.py                   │
  │            Zip folders + create Outlook email drafts            │
  │                                                                 │
  └─────────────────────────────────────────────────────────────────┘


DATA FLOW
=========

  OP.XLSX ──┐
  0127.XLSX ├──→ [Load] → Merged DataFrame (Parquet cache)
  0130.XLSX ┘         │
                      ▼
                [Filter] → test/prod sampling
                      │
                      ▼
              [Validate] → +validation columns
              (7 stages)   (leadtime_invalido, grpm_decisao_llm,
                      │     similarity_score, Text_Analysis,
                      │     obs_pn_presente, ref_*, img_*,
                      │     resumo_validacao, classificacao_validacao)
                      ▼
              [Calculate] → +stock parameters
                      │     (TMD, CV, Classificacao, PR_Calculado,
                      │      MAX_Calculado, Politica_Sugerida,
                      │      Demanda_Anual, Valor_Tributado, ...)
                      ▼
               [Summary] → Statistics & charts
                      │
                      ▼
            [Analysis P1] → +JIRA columns
            (optional)      (jira_historico_resumo, SMIT_texto,
                      │      FRAC_texto, sugestao_jira_frac)
                      ▼
            [Analysis P2] → +AI columns
            (optional)      (Analise_AI, Quantidade_OP_AI, PR_AI,
                      │      MAX_AI, Politica_AI, Comentario,
                      │      acoes_sugeridas, produto_identificado, ...)
                      ▼
              [Dashboard] → HTML/JS per analyst
              [Separation]→ Folder structure AD/ZSTK/Isento/Tributado
              [Emission]  → Templates + email drafts + ZIP


MODULE STRUCTURE
================

  automareq/
  │
  ├── main.py                   Entry point (linear, legacy)
  ├── panel.py                  Entry point (interactive CLI)
  ├── app.py                    Entry point (Streamlit web)
  │
  ├── config/                   Configuration & constants
  │   ├── __init__.py           Package marker
  │   ├── config.py             Backward-compatible re-exports
  │   ├── paths.py              Folder/path definitions
  │   ├── ai.py                 AI model names
  │   ├── business.py           Business thresholds (CV, TMD, costs)
  │   ├── personnel.py          User mapping, email config
  │   ├── prompts.py            LLM system/user prompts
  │   └── sources.py            SAP column definitions (OP, 0127, 0130)
  │
  ├── core/                     Core pipeline logic
  │   ├── pipeline.py           Pipeline engine (PipelineConfig, Pipeline)
  │   ├── base_orchestrator.py  Shared orchestrator infrastructure
  │   ├── load.py               ETL: Excel → Parquet → DataFrame
  │   ├── calculate.py          Stock parameter calculations
  │   ├── validate.py           Re-export → validators/
  │   ├── analysis.py           Re-export → analyzers/
  │   ├── emission.py           Re-export → emitters/
  │   │
  │   ├── validators/           Data validation pipeline
  │   │   ├── __init__.py
  │   │   ├── _base.py          LLMRunner, run_llm_parallel, constants
  │   │   ├── orchestrator.py   Stage registry + run_validations()
  │   │   ├── rules.py          Pure rule-based checks (no AI)
  │   │   ├── ai_stages.py      Re-exports from stages/ (compat)
  │   │   └── stages/           Individual AI validation stages
  │   │       ├── grpm.py       GRPM: ML + LLM group decision
  │   │       ├── texts.py      Text: embeddings + AI audit
  │   │       ├── obs.py        OBS: PN check + LLM extraction
  │   │       ├── reference.py  Reference: market search validation
  │   │       └── images.py     Images: vision LLM quality check
  │   │
  │   ├── analyzers/            Stock analysis pipeline
  │   │   ├── __init__.py
  │   │   ├── _base.py          Shared helpers (update_row, checkpoint)
  │   │   ├── orchestrator.py   Stage registry + run_analysis()
  │   │   └── stages/
  │   │       ├── smit.py       SMIT: JIRA read + enrichment
  │   │       ├── frac.py       FRAC: JIRA create + SAP MRP change
  │   │       ├── zstk.py       ZSTK: rules + AI + market search
  │   │       ├── ad.py         AD: auto-decision + quote text
  │   │       ├── ana.py        ANA: manual analysis flag
  │   │       └── jira_analysis.py  JIRA comment LLM analysis
  │   │
  │   └── emitters/             Output generation pipeline
  │       ├── __init__.py
  │       ├── _base.py          Re-exports step_header
  │       ├── orchestrator.py   Stage registry + run_emission()
  │       └── stages/
  │           ├── dashboard.py      HTML/JS dashboard export
  │           ├── group_separation.py  Folder structure by group
  │           ├── templates.py      Procurement template filling
  │           └── send_drafts.py    Email draft creation + ZIP
  │
  ├── services/                 External service integrations
  │   ├── ai_service.py         AIModule: batch LLM analysis
  │   ├── jira_service.py       JiraModule: JIRA API CRUD
  │   ├── sap_service.py        SapManager: SAP GUI automation
  │   └── search_service.py     ReferenceValidator: web search + cache
  │
  ├── utils/                    Shared utilities
  │   ├── formatting.py         Console formatting, JSON helpers, encoding
  │   ├── export_core.py        Excel export by Responsavel
  │   ├── excel.py              Excel formatting & styling
  │   ├── columns.py            Column ordering & selection
  │   ├── visuals.py            Rich terminal visualizations
  │   ├── visualization_module.py  Matplotlib chart generation
  │   └── fill_template.py      Template data filling
  │
  ├── tests/                    Test suite (pytest)
  │   ├── conftest.py           Shared fixtures
  │   ├── test_config.py        Configuration tests
  │   ├── test_load.py          ETL pipeline tests
  │   ├── test_sources.py       Source definition tests
  │   ├── test_validators.py    Validator stage tests
  │   ├── test_calculate.py     Calculation logic tests
  │   └── test_analysis.py      Analysis stage tests
  │
  ├── data/                     Input/output data
  │   └── YYYY-MM/
  │       ├── input/            OP.XLSX, 0127.XLSX, 0130.XLSX
  │       └── output/           Generated reports per Responsavel
  │
  ├── templates/                Procurement document templates
  │   ├── AD/                   AD templates (CPV / Inexigibilidade)
  │   └── Pregao/               Bidding templates
  │
  └── model/                    ML model artifacts (.joblib)


EXTERNAL SERVICES
=================

  ┌────────────────┐     ┌──────────────────┐     ┌──────────────┐
  │  OpenRouter API │     │    JIRA API       │     │   SAP GUI    │
  │  (LLM + embed)  │     │  (tickets/comms)  │     │ (automation) │
  └───────┬────────┘     └────────┬─────────┘     └──────┬───────┘
          │                       │                       │
    LLMRunner              JiraModule              SapManager
    (_base.py)          (jira_service.py)       (sap_service.py)
          │                       │                       │
          └───────────────────────┼───────────────────────┘
                                  │
                          Pipeline Engine
                          (core/pipeline.py)

  ┌──────────────────┐     ┌──────────────────┐
  │  DuckDuckGo      │     │  Perplexity/Sonar│
  │  (web search)    │     │  (search LLM)    │
  └────────┬─────────┘     └────────┬─────────┘
           │                        │
           └────────────────────────┘
                       │
              ReferenceValidator
            (search_service.py)


CONFIGURATION CONSTANTS
=======================

  Business (config/business.py):
    CV_THRESHOLD      = 0.7       Variability cutoff
    TMD_THRESHOLD     = 1.32      Intermittency cutoff
    DEMAND_WINDOW     = 12        Months for calculations
    VALOR_UN_ALTO     = 1000      High unit value (BRL)
    CUSTO_FIXO_PEDIDO = 100.0     Fixed cost per order
    TAXA_MANUTENCAO   = 0.10      10% annual maintenance
    AD_VALUE_THRESHOLD= 7000      CPV vs Inexigibilidade cutoff
    ANOS_SEM_OC       = 2         Years without purchase order

  AI Models (config/ai.py):
    ai_model_text     = "qwen/qwen3-embedding-8b"
    ai_model_analysis = "google/gemini-3.1-flash-lite-preview"

  Validation (core/validators/_base.py):
    EMBED_THRESHOLD   = 0.99      Cosine similarity approval threshold
    EMBED_BATCH_SIZE  = 50        Embedding batch size
    MAX_AUDIT_WORKERS = 8         Concurrent LLM threads


STOCK POLICY DECISION TREE
===========================

  Material Classification
  ├── Suave (CV<0.7, TMD<1.32)
  │   ├── ABC A/B
  │   │   ├── Lote Obrigatório? → ZL
  │   │   └── Alto Volume? → ZL, else → ZP
  │   └── ABC C
  │       └── Not in ZSTK/SMIT/AD/ANA/FRAC? → ZO, else → ZP
  │
  ├── Intermitente (CV>0.7, TMD<1.32) → ZM
  │
  ├── Errático (CV<0.7, TMD>1.32)
  │   └── Preco > 1000? → ZE, else → ZM
  │
  └── Esporádico (CV>0.7, TMD>1.32)
      ├── Critical / S-planner / has LMR
      │   └── Preco > 1000? → ZE, else → ZM
      ├── TMD>6 and CV>3 → ZD
      └── Default → ZM
"""
