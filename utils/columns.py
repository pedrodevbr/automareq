"""
columns.py — Column definitions for export and display.

All column lists used across the pipeline for selecting and ordering
DataFrame columns in various export contexts.
"""

from __future__ import annotations

import pandas as pd


def _select_export_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Return df with only the columns that exist, in the requested order."""
    return df[[c for c in columns if c in df.columns]]


# ---------------------------------------------------------------------------
# Standard export columns
# ---------------------------------------------------------------------------

# Column order for all exports.
# Only columns that actually exist in the DataFrame will be included.
# NOTE: `resumo_validacao` consolidates ALL validation flags (LT, GRPM, TXT,
#       OBS, REF, IMG, DOC) — individual flag columns like `pre_analise`,
#       `ref_obs_flag`, and `leadtime_obs` are kept only in the DEBUG sheet.
EXPORT_COLUMNS: list[str] = [
    # ── Identification ────────────────────────────────────────────────────
    "Codigo_Material", "Texto_Breve_Material",
    "Setor_Atividade", "Numero_Peca_Fabricante",
    "Classificacao_ABC", "Criticidade",
    # ── Validation Summary (first for human visibility) ───────────────────
    "classificacao_validacao", "score_validacao", "resumo_validacao",
    # ── GRPM (actionable columns only) ────────────────────────────────────
    "Grupo_Mercadoria",
    "grpm_decisao_llm", "grpm_novo_codigo", "grpm_novo_descricao", "grpm_justificativa",
    # ── Planning ──────────────────────────────────────────────────────────
    "Analise_Gestor", "Planejador_MRP", "Planejador_Sugerido", "Grupo_MRP",
    "Politica_Atual", "PR_Atual", "MAX_Atual",
    "Politica_Sugerida", "Quantidade_OP_Calculada",
    "PR_Calculado", "MAX_Calculado", "Estoque_Seguranca", "Nivel_Servico",
    # ── Financial ─────────────────────────────────────────────────────────
    "Estoque_Total", "Saldo_Virtual", "Preco_Unitario",
    "Valor Estoque", "Valor_Atualizado", "Valor_Tributado",
    # ── Market Reference (ReferenceValidator) ─────────────────────────────
    "ref_reference_found", "ref_supplier", "ref_url", "ref_search_links",
    "ref_price_estimated", "ref_currency", "ref_availability",
    "ref_part_number_confirmed", "ref_part_number_note",
    "ref_text_coverage", "ref_coverage_gaps",
    # ── OBS / PN (actionable columns) ─────────────────────────────────────
    "obs_pn_presente", "obs_referencia_extraida", "obs_sugestao_texto",
    # ── AI Analysis ───────────────────────────────────────────────────────
    "Analise_AI", "Quantidade_OP_AI", "PR_AI", "MAX_AI",
    "Politica_AI", "Comentario",
    # ── Demand ────────────────────────────────────────────────────────────
    "Consumo_Medio_Mensal", "Demanda_Mensal", "Demanda_Programada",
    "Demanda_Anual", "Perfil_Demanda", "TMD", "CV",
    "Classificacao", "Outliers", "Data_Ultimo_Consumo", "Quantidade_201_12m",
    # ── Purchase Orders ───────────────────────────────────────────────────
    "Data_Ultimo_Pedido", "Anos_Ultima_Compra", "Responsavel",
    "Quantidade_Ordem", "Valor_Total_Ordem",
    # ── Lead Time ─────────────────────────────────────────────────────────
    "Prazo_Entrega_Previsto", "Dias_Em_OP",
    # ── Logistics ────────────────────────────────────────────────────────
    "Volume", "Unidade de volume", "Volume_OP", "Adicional_Lote_Obrigatorio",
    # ── Image ─────────────────────────────────────────────────────────────
    "img_path", "img_qualidade", "img_motivo", "img_substituir",
    # ── Post-analysis ────────────────────────────────────────────────────
    "pos_analise",
    "RTP1", "RTP2", "RTP3", "RTP6", "Quantidade_LMR",
    # ── Texts ─────────────────────────────────────────────────────────────
    "AD_texto", "SMIT_texto", "FRAC_texto",
    "similarity_score", "Text_Analysis", "Texto_Sugerido",
    "Texto_PT", "Texto_ES",
    "Texto_Observacao_PT", "Texto_Observacao_ES",
    "Texto_Qualidade_Material_PT", "Texto_Qualidade_Material_ES",
    "Texto_Dados_Basicos_PT", "Texto_Dados_Basicos_ES",
    "Texto REF LMR",
    # ── JIRA Analysis ────────────────────────────────────────────────────
    "jira_historico_resumo", "jira_acao_sugerida",
    "jira_tickets_count", "jira_status_atual",
    # ── Suggested Actions ────────────────────────────────────────────────
    "acoes_sugeridas",
    # ── LTD History ───────────────────────────────────────────────────────
    *[f"LTD_{i}" for i in range(1, 13)],
]


# ---------------------------------------------------------------------------
# Analyst report columns (streamlined for human review)
# ---------------------------------------------------------------------------

ANALYST_REPORT_COLUMNS: list[str] = [
    # ── Identification ────────────────────────────────────────────────────
    "Codigo_Material", "Texto_Breve_Material",
    "Setor_Atividade", "Numero_Peca_Fabricante",
    "Classificacao_ABC", "Criticidade",
    # ── Validation Summary ────────────────────────────────────────────────
    "classificacao_validacao", "score_validacao", "resumo_validacao",
    # ── GRPM ──────────────────────────────────────────────────────────────
    "Grupo_Mercadoria",
    "grpm_decisao_llm", "grpm_novo_codigo", "grpm_justificativa",
    # ── Planning ──────────────────────────────────────────────────────────
    "Grupo_MRP", "Planejador_MRP",
    "Politica_Atual", "PR_Atual", "MAX_Atual",
    "Politica_Sugerida", "Quantidade_OP_Calculada",
    "PR_Calculado", "MAX_Calculado",
    # ── Financial ─────────────────────────────────────────────────────────
    "Estoque_Total", "Saldo_Virtual", "Preco_Unitario", "Valor_Tributado",
    # ── Reference ─────────────────────────────────────────────────────────
    "ref_reference_found", "ref_supplier", "ref_url",
    "ref_price_estimated", "ref_currency",
    # ── AI Analysis ───────────────────────────────────────────────────────
    "Analise_AI", "Quantidade_OP_AI", "Politica_AI", "Comentario",
    # ── JIRA & Actions ────────────────────────────────────────────────────
    "jira_historico_resumo", "jira_acao_sugerida", "acoes_sugeridas",
    # ── Demand ────────────────────────────────────────────────────────────
    "Consumo_Medio_Mensal", "Demanda_Anual", "Classificacao", "TMD", "CV",
    # ── Texts ─────────────────────────────────────────────────────────────
    "Texto_PT", "Texto_ES",
    "obs_sugestao_texto", "Texto_Sugerido",
    # ── Purchase ──────────────────────────────────────────────────────────
    "Data_Ultimo_Pedido", "Anos_Ultima_Compra", "Responsavel",
]


# ---------------------------------------------------------------------------
# AD requisition columns (procurement focus)
# ---------------------------------------------------------------------------

AD_REQUISITION_COLUMNS: list[str] = [
    "Codigo_Material", "Texto_Breve_Material", "Grupo_Mercadoria",
    "Numero_Peca_Fabricante", "Setor_Atividade",
    "Preco_Unitario", "Valor_Tributado", "Valor_Total_Ordem",
    "Quantidade_OP_AI", "Quantidade_OP_Calculada",
    "Texto_PT", "Texto_ES",
    "Texto_Observacao_PT", "Texto_Observacao_ES",
    "ref_url", "ref_price_estimated", "ref_supplier",
    "obs_sugestao_texto",
    "Analise_AI", "Comentario", "acoes_sugeridas",
    "pos_analise",
]


# ---------------------------------------------------------------------------
# Non-replenish report columns
# ---------------------------------------------------------------------------

NAO_REPOR_COLUMNS: list[str] = [
    "Codigo_Material", "Texto_Breve_Material", "Responsavel",
    "Grupo_MRP", "Grupo_Mercadoria",
    "Analise_AI", "Comentario", "acoes_sugeridas",
    "Estoque_Total", "Saldo_Virtual", "Preco_Unitario",
    "Consumo_Medio_Mensal", "Data_Ultimo_Consumo",
    "Data_Ultimo_Pedido", "Anos_Ultima_Compra",
    "jira_historico_resumo", "jira_acao_sugerida",
]


# ---------------------------------------------------------------------------
# ZSTK split columns
# ---------------------------------------------------------------------------

# Columns included in the final per-group ZSTK split files
ZSTK_SPLIT_COLUMNS: list[str] = [
    # ── ID + Validation ───────────────────────────────────────────────────
    "Codigo_Material", "Texto_Breve_Material", "Grupo_Mercadoria",
    "classificacao_validacao", "score_validacao", "resumo_validacao",
    # ── GRPM ──────────────────────────────────────────────────────────────
    "grpm_decisao_llm", "grpm_novo_codigo", "grpm_justificativa",
    # ── Material data ─────────────────────────────────────────────────────
    "Numero_Peca_Fabricante",
    "Texto_PT", "Texto_ES",
    # ── Pricing & Reference ───────────────────────────────────────────────
    "Preco_Unitario", "ref_price_estimated", "ref_currency",
    "ref_url", "ref_search_links", "ref_availability",
    "ref_part_number_confirmed", "ref_text_coverage", "ref_coverage_gaps",
    # ── OBS / PN ──────────────────────────────────────────────────────────
    "obs_pn_presente", "obs_referencia_extraida", "obs_sugestao_texto",
    # ── Order ─────────────────────────────────────────────────────────────
    "Quantidade_OP_Calculada", "Quantidade_OP_AI",
    # ── Texts ─────────────────────────────────────────────────────────────
    "Texto_Observacao_PT", "Texto_Observacao_ES",
    # ── Image ─────────────────────────────────────────────────────────────
    "img_path", "img_qualidade", "img_substituir",
    # ── Post-analysis ────────────────────────────────────────────────────
    "pos_analise",
]


# ---------------------------------------------------------------------------
# Debug columns
# ---------------------------------------------------------------------------

# Columns shown first in the debug sheet (remaining columns appended in original order)
# Debug sheet keeps ALL raw flags for deep inspection — the consolidated
# summary columns come first for quick triage, then raw detail columns.
_DEBUG_PRIORITY_COLUMNS: list[str] = [
    # ── Quick triage ──────────────────────────────────────────────────
    "Codigo_Material", "Texto_Breve_Material", "Responsavel",
    "classificacao_validacao", "score_validacao", "resumo_validacao",
    # ── Raw flags (detail — only in debug) ────────────────────────────
    "pre_analise", "ref_obs_flag",
    "leadtime_invalido", "leadtime_obs", "Prazo_Entrega_Previsto",
    "grpm_formato_invalido", "grpm_decisao_llm", "grpm_novo_codigo",
    "grpm_novo_descricao", "grpm_justificativa",
    "Grupo_Mercadoria", "Descricao_Grupo_Atual", "Grupo_Sugerido",
    "similarity_score", "Text_Analysis", "Texto_Sugerido",
    "obs_pn_presente", "obs_referencia_extraida", "obs_pesquisa_vale",
    "obs_motivo", "obs_sugestao_texto",
    "ref_reference_found", "ref_supplier", "ref_url", "ref_search_links",
    "ref_part_number_confirmed", "ref_part_number_note",
    "ref_text_coverage", "ref_coverage_gaps", "ref_validation_issues",
    "img_path", "img_qualidade", "img_motivo", "img_substituir",
    # ── Material texts ────────────────────────────────────────────────
    "Numero_Peca_Fabricante",
    "Texto_PT", "Texto_ES",
    "Texto_Observacao_PT", "Texto_Observacao_ES",
    "Texto_Dados_Basicos_PT", "Texto_Dados_Basicos_ES",
]


# ---------------------------------------------------------------------------
# Dashboard columns
# ---------------------------------------------------------------------------

# Columns serialised into dashboard_data.js
_DASHBOARD_COLS: list[str] = [
    "Codigo_Material", "Texto_Breve_Material", "Responsavel",
    "Grupo_Mercadoria", "Prazo_Entrega_Previsto", "Numero_Peca_Fabricante",
    "Texto_PT", "Texto_ES", "Texto_Observacao_PT", "Texto_Observacao_ES",
    # lead time
    "leadtime_invalido", "leadtime_obs",
    # grpm
    "grpm_formato_invalido", "grpm_decisao_llm",
    "grpm_novo_codigo", "grpm_novo_descricao", "grpm_justificativa",
    # similarity
    "similarity_score",
    # obs pre-check
    "obs_pn_presente", "obs_referencia_extraida", "obs_pesquisa_vale",
    "obs_motivo", "obs_sugestao_texto",
    # reference
    "ref_reference_found", "ref_supplier", "ref_url", "ref_search_links",
    "ref_price_estimated", "ref_currency", "ref_availability",
    "ref_part_number_confirmed", "ref_part_number_note",
    "ref_text_coverage", "ref_coverage_gaps", "ref_validation_issues",
    # image
    "img_path", "img_qualidade", "img_motivo", "img_substituir",
    # validation summary
    "classificacao_validacao", "resumo_validacao",
    # JIRA & actions
    "jira_historico_resumo", "jira_acao_sugerida",
    "jira_tickets_count", "jira_status_atual",
    "acoes_sugeridas",
    # AI analysis
    "Analise_AI", "Comentario", "Politica_AI",
    "Quantidade_OP_AI", "PR_AI", "MAX_AI",
    # planning
    "Grupo_MRP", "Politica_Sugerida", "PR_Calculado", "MAX_Calculado",
    "Classificacao", "TMD", "CV",
]
