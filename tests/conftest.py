"""Shared fixtures for the test suite."""

from __future__ import annotations

import pandas as pd
import pytest

from config.sources import SOURCE_OP, SOURCE_0127, SOURCE_0130


@pytest.fixture()
def sample_op_df() -> pd.DataFrame:
    """Minimal OP DataFrame with original SAP column names."""
    return pd.DataFrame({
        "Material": ["000000001234", "000000005678"],
        "Txt.brv.material": ["CONTATOR 3P 25A", "DISJUNTOR 2P 16A"],
        "Data abertura plan.": ["2025-01-15", "2025-02-20"],
        "Grupo de mercadorias": ["123456", "789012"],
        "Setor de atividade": ["E", "E"],
        "Nº peça fabricante": ["LC1D25", "C60N-2P-16A"],
        "Planejador MRP": ["S21", "S22"],
        "Grupo MRP": ["ZSTK", "AD"],
        "Tipo de MRP": ["VB", "VB"],
        "Prz.entrg.prev.": [90, 60],
        "Estoque total": [10.0, 0.0],
        "Ponto reabastec.": [5.0, 2.0],
        "Estoque máximo": [20.0, 10.0],
        "Estoque de segurança": [3.0, 1.0],
        "Valor Estoque": [500.0, 0.0],
        "CMM": [1.5, 0.3],
        "Demanda": ["Regular", "Esporádica"],
        "Demanda Med.": [1.5, 0.3],
        "Preço Unit.": [50.0, 120.0],
        "Qtd. RTP1": [2.0, 0.0],
        "Qtd. RTP2": [0.0, 0.0],
        "Qtd. RTP3": [1.0, 0.0],
        "Qtd. RTP6": [0.0, 1.0],
        "Sld. Virtual": [8.0, -1.0],
        "Qtd.ordem planejada": [10.0, 5.0],
        "Valor OP": [500.0, 600.0],
        "Responsável": ["PEDROHVB", "MTSE"],
        "Criticidade": ["1", "2"],
        "Qtd. LMR": [5.0, 0.0],
        "Dem. Pro.": [3.0, 1.0],
        "Dt. Ult. Pedido": ["2024-06-10", "2024-03-15"],
        "Fornecedor": ["FORN001", "FORN002"],
        "Nome": ["Fornecedor A", "Fornecedor B"],
        "Dt. Ult. Requisição": ["2024-07-01", "2024-04-01"],
        "Qtd. Pedido": [10.0, 5.0],
        "Qtd. Requisição": [10.0, 5.0],
        "Qtd. RemCG": [0.0, 0.0],
        "Dt. Ult. 201": ["2025-01-01", "2024-12-01"],
        "Qt. 201 - 12 Meses": [18.0, 3.6],
        "1LTD": [5.0, 1.0],
        "2LTD": [4.0, 0.5],
        "3LTD": [6.0, 1.2],
        "4LTD": [3.0, 0.8],
        "5LTD": [5.0, 1.0],
        "Código ABC": ["A", "C"],
        "AdminLoteObrig.": ["X", ""],
        "Volume": [0.5, 0.1],
        "Unidade de volume": ["M3", "M3"],
    })


@pytest.fixture()
def sample_0127_df() -> pd.DataFrame:
    """Minimal 0127 DataFrame with original SAP column names."""
    return pd.DataFrame({
        "Material": ["000000001234", "000000005678"],
        "Descrição - pt": ["Contator tripolar 25A", "Disjuntor bipolar 16A"],
        "Descrição - es": ["Contactor tripolar 25A", "Disyuntor bipolar 16A"],
        "Nº peça fabricante": ["LC1D25", "C60N"],
        "Texto - pt": ["Contator para manobra", "Disjuntor termomagnético"],
        "Texto - es": ["Contactor para maniobra", "Disyuntor termomagnetico"],
        "Texto OBS - pt": ["Ref: LC1D25 Schneider", "Ref: C60N Schneider"],
        "Texto OBS - es": ["Ref: LC1D25 Schneider", "Ref: C60N Schneider"],
        "Texto QM - pt": ["", ""],
        "Texto QM - es": ["", ""],
        "Texto DB - pt": ["", ""],
        "Texto DB - es": ["", ""],
        "Texto REF LMR": ["LMR-001", "LMR-002"],
        "Status": ["A", "A"],
        "Texto CLA - pt": ["", ""],
        "Texto CLA - es": ["", ""],
        "Texto LMR": ["", ""],
        "Linha": ["1", "1"],
    })


@pytest.fixture()
def sample_0130_df() -> pd.DataFrame:
    """Minimal 0130 DataFrame with original SAP column names."""
    data = {
        "Material": ["000000001234", "000000005678"],
        "Txt.brv.material": ["CONTATOR 3P 25A", "DISJUNTOR 2P 16A"],
        "Prz.entrg.prev.": ["90", "60"],
    }
    for i in range(1, 18):
        data[f"{i} LTD"] = [str(float(i)), str(float(i) * 0.5)]
    return pd.DataFrame(data)


@pytest.fixture()
def merged_df(sample_op_df) -> pd.DataFrame:
    """A DataFrame that looks like post-merge output with standardised names."""
    from config.sources import SOURCE_OP
    df = sample_op_df.rename(columns=SOURCE_OP.rename_map)
    # Add pipeline-initialised columns
    df["Nivel_Servico"] = 0.92
    df["Dias_Em_OP"] = 0
    df["Text_Analysis"] = ""
    df["Analise_Gestor"] = ""
    df["pre_analise"] = ""
    return df
