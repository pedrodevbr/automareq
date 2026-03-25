"""
config/sources.py
=================
Typed column definitions for each SAP data source (OP, 0127, 0130).

Replaces the old templates/column_mapping.csv with Python dataclasses
that give IDE autocomplete, type safety, and load-time validation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Literal

# ---------------------------------------------------------------------------
# Column definition
# ---------------------------------------------------------------------------

ColumnType = Literal["str", "int", "float", "datetime", "bool"]


@dataclass(frozen=True)
class Column:
    """A single column mapping: original SAP name -> standardised name."""
    original: str
    standard: str
    dtype: ColumnType
    included: bool = True


# ---------------------------------------------------------------------------
# Source definition
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SourceDef:
    """Definition of one SAP export file (columns + merge key)."""
    name: str
    filename: str
    columns: List[Column]
    merge_key_original: str = "Material"
    merge_key_standard: str = "Codigo_Material"

    # -- derived helpers (computed once, cached) ----------------------------

    @property
    def rename_map(self) -> Dict[str, str]:
        """Original -> Standardised name mapping."""
        return {c.original: c.standard for c in self.columns}

    @property
    def drop_columns(self) -> List[str]:
        """Original column names flagged as excluded."""
        return [c.original for c in self.columns if not c.included]

    @property
    def included_columns(self) -> List[str]:
        """Standardised names for included columns (preserves order)."""
        return [c.standard for c in self.columns if c.included]

    @property
    def type_map(self) -> Dict[str, ColumnType]:
        """Standardised name -> Python type string for included columns."""
        seen: Dict[str, ColumnType] = {}
        for c in self.columns:
            if c.included and c.standard not in seen:
                seen[c.standard] = c.dtype
        return seen


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════

SOURCE_OP = SourceDef(
    name="OP",
    filename="OP.XLSX",
    columns=[
        Column("Data abertura plan.",   "Data_Abertura",              "datetime"),
        Column("Material",              "Codigo_Material",            "str"),
        Column("Txt.brv.material",      "Texto_Breve_Material",      "str"),
        Column("Grupo de mercadorias",  "Grupo_Mercadoria",          "str"),
        Column("Setor de atividade",    "Setor_Atividade",           "str"),
        Column("Nº peça fabricante",    "Numero_Peca_Fabricante",    "str"),
        Column("Planejador MRP",        "Planejador_MRP",            "str"),
        Column("Grupo MRP",             "Grupo_MRP",                 "str"),
        Column("Tipo de MRP",           "Tipo_MRP",                  "str"),
        Column("Prz.entrg.prev.",       "Prazo_Entrega_Previsto",    "int"),
        Column("Estoque total",         "Estoque_Total",             "float"),
        Column("Ponto reabastec.",      "Ponto_Reabastecimento",     "float"),
        Column("Estoque máximo",        "Estoque_Maximo",            "float"),
        Column("Estoque de segurança",  "Estoque_Seguranca",         "float"),
        Column("Valor Estoque",         "Valor_Total_Em_Estoque",    "float"),
        Column("CMM",                   "Consumo_Medio_Mensal",      "float"),
        Column("Demanda",               "Perfil_Demanda",            "str"),
        Column("Demanda Med.",          "Demanda_Mensal",            "float"),
        Column("Preço Unit.",           "Preco_Unitario",            "float"),
        Column("Qtd. RTP1",            "RTP1",                      "float"),
        Column("Qtd. RTP2",            "RTP2",                      "float"),
        Column("Qtd. RTP3",            "RTP3",                      "float"),
        Column("Qtd. RTP6",            "RTP6",                      "float"),
        Column("Sld. Virtual",         "Saldo_Virtual",             "float"),
        Column("Qtd.ordem planejada",  "Quantidade_Ordem",          "float"),
        Column("Valor OP",             "Valor_Total_Ordem",         "float"),
        Column("Responsável",          "Responsavel",               "str"),
        Column("Criticidade",          "Criticidade",               "str"),
        Column("Qtd. LMR",            "Quantidade_LMR",            "float"),
        Column("Dem. Pro.",            "Demanda_Programada",        "float"),
        Column("Dt. Ult. Pedido",      "Data_Ultimo_Pedido",        "datetime"),
        Column("Fornecedor",           "Codigo_Fornecedor",         "str"),
        Column("Nome",                 "Nome_Fornecedor",           "str"),
        Column("Dt. Ult. Requisição",  "Data_Ultima_Requisicao",    "datetime"),
        Column("Qtd. Pedido",          "Quantidade_Pedida",         "float"),
        Column("Qtd. Requisição",      "Quantidade_Requisitada",    "float"),
        Column("Qtd. RemCG",           "Quantidade_Remessa_CG",     "float"),
        Column("Dt. Ult. 201",         "Data_Ultimo_Consumo",       "datetime"),
        Column("Qt. 201 - 12 Meses",   "Quantidade_201_12m",        "float"),
        Column("1LTD",                 "LTD_1",                     "float"),
        Column("2LTD",                 "LTD_2",                     "float"),
        Column("3LTD",                 "LTD_3",                     "float"),
        Column("4LTD",                 "LTD_4",                     "float"),
        Column("5LTD",                 "LTD_5",                     "float"),
        Column("Código ABC",           "Classificacao_ABC",         "str"),
        Column("AdminLoteObrig.",      "Adicional_Lote_Obrigatorio","bool"),
        Column("Volume",               "Volume",                    "float"),
        Column("Unidade de volume",    "Unidade_Volume",            "str"),
    ],
)

SOURCE_0127 = SourceDef(
    name="0127",
    filename="0127.XLSX",
    columns=[
        Column("Descrição - pt",        "Descricao_PT",                   "str"),
        Column("Descrição - es",        "Descricao_ES",                   "str"),
        Column("Nº peça fabricante",    "Numero_Peca_Fabricante",         "str", included=False),
        Column("Texto - pt",           "Texto_PT",                       "str"),
        Column("Texto - es",           "Texto_ES",                       "str"),
        Column("Texto OBS - pt",       "Texto_Observacao_PT",            "str"),
        Column("Texto OBS - es",       "Texto_Observacao_ES",            "str"),
        Column("Texto QM - pt",        "Texto_Qualidade_Material_PT",    "str"),
        Column("Texto QM - es",        "Texto_Qualidade_Material_ES",    "str"),
        Column("Texto DB - pt",        "Texto_Dados_Basicos_PT",         "str"),
        Column("Texto DB - es",        "Texto_Dados_Basicos_ES",         "str"),
        Column("Texto REF LMR",        "Texto_REF_LMR",                  "str"),
        Column("Status",               "Status",                         "str", included=False),
        Column("Texto CLA - pt",       "Texto_CLA_PT",                   "str", included=False),
        Column("Texto CLA - es",       "Texto_CLA_ES",                   "str", included=False),
        Column("Texto LMR",            "Texto_LMR",                      "str", included=False),
        Column("Linha",                "Linha",                          "str", included=False),
    ],
)

SOURCE_0130 = SourceDef(
    name="0130",
    filename="0130.XLSX",
    columns=[
        Column("Material",             "Codigo_Material",           "str", included=False),
        Column("Txt.brv.material",     "Texto_Breve_Material",     "str", included=False),
        Column("Prz.entrg.prev.",      "Prazo_Entrega_Previsto",   "int", included=False),
        Column("1 LTD",               "LTD_1",                    "float"),
        Column("2 LTD",               "LTD_2",                    "float"),
        Column("3 LTD",               "LTD_3",                    "float"),
        Column("4 LTD",               "LTD_4",                    "float"),
        Column("5 LTD",               "LTD_5",                    "float"),
        Column("6 LTD",               "LTD_6",                    "float"),
        Column("7 LTD",               "LTD_7",                    "float"),
        Column("8 LTD",               "LTD_8",                    "float"),
        Column("9 LTD",               "LTD_9",                    "float"),
        Column("10 LTD",              "LTD_10",                   "float"),
        Column("11 LTD",              "LTD_11",                   "float"),
        Column("12 LTD",              "LTD_12",                   "float"),
        Column("13 LTD",              "LTD_13",                   "float"),
        Column("14 LTD",              "LTD_14",                   "float"),
        Column("15 LTD",              "LTD_15",                   "float"),
        Column("16 LTD",              "LTD_16",                   "float"),
        Column("17 LTD",              "LTD_17",                   "float"),
    ],
)

# All sources in merge order
ALL_SOURCES = [SOURCE_OP, SOURCE_0127, SOURCE_0130]


def get_all_included_columns() -> List[str]:
    """Returns deduplicated list of all included standardised column names, in order."""
    seen: set[str] = set()
    result: List[str] = []
    for src in ALL_SOURCES:
        for col in src.included_columns:
            if col not in seen:
                seen.add(col)
                result.append(col)
    return result


def get_merged_type_map() -> Dict[str, ColumnType]:
    """Merged type map across all sources (first definition wins)."""
    merged: Dict[str, ColumnType] = {}
    for src in ALL_SOURCES:
        for name, dtype in src.type_map.items():
            if name not in merged:
                merged[name] = dtype
    return merged


# ---------------------------------------------------------------------------
# Columns selected for AI analysis (used by ai_service)
# ---------------------------------------------------------------------------

ANALYSIS_COLUMNS = [
    "Codigo_Material", "Texto_PT", "Numero_Peca_Fabricante", "Prazo_Entrega_Previsto",
    "Estoque_Total", "Preco_Unitario", "RTP1", "RTP2", "RTP3", "RTP6", "Nivel_Servico",
    "Saldo_Virtual", "Criticidade", "Demanda_Programada", "Quantidade_Requisitada", "Quantidade_Pedida",
    "Classificacao_ABC", "Adicional_Lote_Obrigatorio",
] + [f"LTD_{i}" for i in range(1, 12)]
