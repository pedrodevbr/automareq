"""
Backward-compatible shim — re-exports from sub-modules.

Prefer importing directly from the specific sub-module:
  from config.paths import INPUT_FOLDER
  from config.ai import ai_model_text
  from config.business import CV_THRESHOLD
  etc.
"""

# Paths
from config.paths import (
    BASE_DIR, DATA_FOLDER, MONTH_FOLDER, INPUT_FOLDER,
    OUTPUT_FOLDER, TEMPLATES_FOLDER, AD_TEMPLATE_DIR,
)

# AI models
from config.ai import ai_model_text, ai_model_analysis

# Business constants
from config.business import (
    CV_THRESHOLD, TMD_THRESHOLD, DEMAND_WINDOW,
    BAIXO_VALOR, ALTO_VALOR, VALOR_UN_ALTO, ALTO_VOLUME,
    ANOS_SEM_OC, CUSTO_FIXO_PEDIDO, TAXA_MANUTENCAO,
    AD_VALUE_THRESHOLD,
)

# Personnel
from config.personnel import (
    RESPONSAVEIS, PLANEJADORES, country_for_responsavel,
    MTSE_RECIPIENT, EMAIL_DOMAIN, EMAIL_SUBJECT,
    EMAIL_BODY_PT, EMAIL_BODY_ES, CPV_CC_EMAIL, CPV_RECIPIENTS,
)

# Prompts
from config.prompts import (
    SYSTEM_PROMPT, USER_PROMPT_TEMPLATE,
    JIRA_ANALYSIS_SYSTEM_PROMPT, JIRA_ANALYSIS_USER_TEMPLATE,
    ACTION_SUGGESTION_ADDENDUM, STOCK_POLICY_REFERENCE,
)

# Sources
from config.sources import (
    Column, SourceDef, SOURCE_OP, SOURCE_0127, SOURCE_0130,
    ALL_SOURCES, ANALYSIS_COLUMNS,
    get_all_included_columns, get_merged_type_map,
)
