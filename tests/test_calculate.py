"""Tests for core/calculate.py — outliers, PR, decision tree, run_calculations."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from core.calculate import (
    calculate_pr_row,
    decision_tree_row,
    detect_outliers_row,
    run_calculations,
)


# ===========================================================================
# detect_outliers_row
# ===========================================================================

class TestDetectOutliersRow:
    def test_no_outliers(self):
        vals = np.array([10.0, 12.0, 11.0, 10.5])
        assert detect_outliers_row(vals) == []

    def test_obvious_outlier(self):
        # Values with natural variance + one extreme outlier
        vals = np.array([8.0, 10.0, 12.0, 9.0, 11.0, 10.0, 13.0, 9.5, 200.0])
        result = detect_outliers_row(vals)
        assert 200.0 in result

    def test_all_zeros(self):
        assert detect_outliers_row(np.array([0.0, 0.0, 0.0])) == []

    def test_single_positive(self):
        assert detect_outliers_row(np.array([0.0, 0.0, 5.0])) == []

    def test_empty_array(self):
        assert detect_outliers_row(np.array([])) == []

    def test_equal_values_no_outliers(self):
        vals = np.array([5.0, 5.0, 5.0, 5.0])
        assert detect_outliers_row(vals) == []


# ===========================================================================
# calculate_pr_row
# ===========================================================================

class TestCalculatePrRow:
    def test_all_zeros(self):
        assert calculate_pr_row(np.array([0.0, 0.0, 0.0])) == 1.0

    def test_single_value(self):
        result = calculate_pr_row(np.array([0.0, 0.0, 7.0]))
        assert result == 7.0

    def test_two_values_returns_second_largest(self):
        result = calculate_pr_row(np.array([0.0, 5.0, 10.0]))
        assert result == 5.0

    def test_empty_array(self):
        assert calculate_pr_row(np.array([])) == 1.0


# ===========================================================================
# decision_tree_row
# ===========================================================================

class TestDecisionTreeRow:
    def test_suave_ab_alto_volume(self):
        row = pd.Series({
            "Classificacao": "Suave",
            "Classificacao_ABC": "A",
            "Adicional_Lote_Obrigatorio": "",
            "Grupo_MRP": "ZSTK",
            "Planejador_MRP": "U09",
            "Volume_Ordem_Planejada": 2_000_000,
            "Preco_Unitario": 50,
            "Criticidade": 0,
            "Quantidade_LMR": 0,
            "TMD": 1.0,
            "CV": 0.3,
        })
        assert decision_tree_row(row) == "ZL"

    def test_suave_ab_baixo_volume(self):
        row = pd.Series({
            "Classificacao": "Suave",
            "Classificacao_ABC": "B",
            "Adicional_Lote_Obrigatorio": "",
            "Grupo_MRP": "ZSTK",
            "Planejador_MRP": "U09",
            "Volume_Ordem_Planejada": 100,
            "Preco_Unitario": 50,
            "Criticidade": 0,
            "Quantidade_LMR": 0,
            "TMD": 1.0,
            "CV": 0.3,
        })
        assert decision_tree_row(row) == "ZP"

    def test_suave_c_non_zstk(self):
        row = pd.Series({
            "Classificacao": "Suave",
            "Classificacao_ABC": "C",
            "Adicional_Lote_Obrigatorio": "",
            "Grupo_MRP": "OTHER",
            "Planejador_MRP": "U09",
            "Volume_Ordem_Planejada": 100,
            "Preco_Unitario": 50,
            "Criticidade": 0,
            "Quantidade_LMR": 0,
            "TMD": 1.0,
            "CV": 0.3,
        })
        assert decision_tree_row(row) == "ZO"

    def test_intermitente(self):
        row = pd.Series({
            "Classificacao": "Intermitente",
            "Classificacao_ABC": "A",
            "Adicional_Lote_Obrigatorio": "",
            "Grupo_MRP": "ZSTK",
            "Planejador_MRP": "U09",
            "Volume_Ordem_Planejada": 100,
            "Preco_Unitario": 50,
            "Criticidade": 0,
            "Quantidade_LMR": 0,
            "TMD": 1.0,
            "CV": 0.9,
        })
        assert decision_tree_row(row) == "ZM"

    def test_erratico_high_price(self):
        row = pd.Series({
            "Classificacao": "Errático",
            "Classificacao_ABC": "A",
            "Adicional_Lote_Obrigatorio": "",
            "Grupo_MRP": "ZSTK",
            "Planejador_MRP": "U09",
            "Volume_Ordem_Planejada": 100,
            "Preco_Unitario": 2000,
            "Criticidade": 0,
            "Quantidade_LMR": 0,
            "TMD": 2.0,
            "CV": 0.9,
        })
        assert decision_tree_row(row) == "ZE"

    def test_esporadico_with_criticidade(self):
        row = pd.Series({
            "Classificacao": "Esporádico",
            "Classificacao_ABC": "C",
            "Adicional_Lote_Obrigatorio": "",
            "Grupo_MRP": "ZSTK",
            "Planejador_MRP": "U09",
            "Volume_Ordem_Planejada": 100,
            "Preco_Unitario": 50,
            "Criticidade": 1,
            "Quantidade_LMR": 0,
            "TMD": 2.0,
            "CV": 1.0,
        })
        assert decision_tree_row(row) == "ZM"

    def test_esporadico_zd(self):
        row = pd.Series({
            "Classificacao": "Esporádico",
            "Classificacao_ABC": "C",
            "Adicional_Lote_Obrigatorio": "",
            "Grupo_MRP": "ZSTK",
            "Planejador_MRP": "U09",
            "Volume_Ordem_Planejada": 100,
            "Preco_Unitario": 50,
            "Criticidade": 0,
            "Quantidade_LMR": 0,
            "TMD": 7,
            "CV": 4,
        })
        assert decision_tree_row(row) == "ZD"

    def test_default_zm(self):
        row = pd.Series({
            "Classificacao": "Unknown",
            "Classificacao_ABC": "C",
        })
        assert decision_tree_row(row) == "ZM"


# ===========================================================================
# run_calculations — integration
# ===========================================================================

class TestRunCalculations:
    @pytest.fixture
    def calc_df(self):
        """Minimal DataFrame that run_calculations can process."""
        return pd.DataFrame({
            "Codigo_Material": ["MAT001", "MAT002", "MAT003"],
            "Texto_Breve_Material": ["Item A", "Item B", "Item C"],
            "Prazo_Entrega_Previsto": [30, 60, 90],
            "Preco_Unitario": [100.0, 50.0, 200.0],
            "Classificacao_ABC": ["A", "B", "C"],
            "Criticidade": [0, 1, 0],
            "Quantidade_LMR": [0, 0, 5],
            "Volume_Ordem_Planejada": [500, 100, 2_000_000],
            "Grupo_MRP": ["ZSTK", "ZSTK", "ZSTK"],
            "Planejador_MRP": ["U09", "S21", "U13"],
            "Adicional_Lote_Obrigatorio": ["", "X", ""],
            "Grupo_Mercadoria": ["1234", "5678", "0201"],
            "Volume": [1.0, 2.0, 0.5],
            "Quantidade_Ordem": [10, 20, 5],
            "Saldo_Virtual": [50, 30, 100],
            "Data_Ultimo_Pedido": ["2025-01-15", "2024-06-01", None],
            "Data_Abertura": ["2026-01-01", "2026-02-15", "2025-12-01"],
            "Texto_Observacao_PT": ["item sustentavel", "desenho tecnico", "normal"],
            "Tipo_MRP": ["ZP", "ZM", "ZL"],
            "Ponto_Reabastecimento": [5, 10, 15],
            "Estoque_Maximo": [20, 30, 40],
            "Estoque_Total": [15, 25, 35],
            "Consumo_Medio_Mensal": [3.0, 1.5, 0.5],
            "Demanda_Mensal": [3, 1, 0],
            "Demanda_Programada": [False, True, False],
            "Perfil_Demanda": ["Regular", "Irregular", "Esporadico"],
            "Data_Ultimo_Consumo": ["2026-01-01", "2025-06-01", None],
            "Quantidade_201_12m": [36, 12, 0],
            "Valor_Total_Ordem": [1000, 500, 200],
            "PR_Calculado": [0, 0, 0],
            "Estoque_Seguranca": [2, 5, 0],
            "RTP1": [3, 0, 0],
            "RTP2": [2, 1, 0],
            "RTP3": [5, 2, 0],
            "RTP6": [10, 5, 0],
            # LTD columns (12 months)
            "LTD_01": [10, 5, 0],
            "LTD_02": [12, 3, 0],
            "LTD_03": [8, 0, 1],
            "LTD_04": [15, 7, 0],
            "LTD_05": [10, 0, 0],
            "LTD_06": [11, 4, 0],
            "LTD_07": [9, 0, 0],
            "LTD_08": [13, 6, 0],
            "LTD_09": [10, 0, 0],
            "LTD_10": [12, 3, 0],
            "LTD_11": [8, 0, 0],
            "LTD_12": [14, 5, 0],
        })

    def test_output_columns_exist(self, calc_df):
        result = run_calculations(calc_df)
        expected = [
            "TMD", "CV", "Classificacao", "Outliers", "PR_Calculado",
            "Demanda_Anual", "Politica_Sugerida", "MAX_Calculado",
            "Valor_Atualizado", "Dias_Em_OP", "Anos_Ultima_Compra",
            "Volume_OP", "Nivel_Servico", "Valor_Tributado",
            "Politica_Atual", "PR_Atual", "MAX_Atual",
            "pos_analise", "Compras_Sustentaveis", "Desenho_Tecnico",
        ]
        for col in expected:
            assert col in result.columns, f"Missing column: {col}"

    def test_tmd_cv_calculated(self, calc_df):
        result = run_calculations(calc_df)
        # MAT001 has all 12 months nonzero → TMD = 12/12 = 1.0
        assert result.loc[0, "TMD"] == 1.0
        # MAT003 has only 1 nonzero month → TMD = 12/1 = 12.0
        assert result.loc[2, "TMD"] == 12.0

    def test_classification_suave(self, calc_df):
        result = run_calculations(calc_df)
        # MAT001: TMD=1.0 (< 1.32), CV should be < 0.7 → Suave
        assert result.loc[0, "Classificacao"] == "Suave"

    def test_policy_assigned(self, calc_df):
        result = run_calculations(calc_df)
        assert result.loc[0, "Politica_Sugerida"] in ["ZP", "ZL", "ZM", "ZE", "ZO", "ZD"]

    def test_max_calculated_positive(self, calc_df):
        result = run_calculations(calc_df)
        # Items with demand should have MAX > 0
        assert result.loc[0, "MAX_Calculado"] > 0

    def test_sustainability_flag(self, calc_df):
        result = run_calculations(calc_df)
        assert result.loc[0, "Compras_Sustentaveis"] == True
        assert result.loc[2, "Compras_Sustentaveis"] == False

    def test_drawing_flag(self, calc_df):
        result = run_calculations(calc_df)
        assert result.loc[1, "Desenho_Tecnico"] == True

    def test_valor_tributado(self, calc_df):
        result = run_calculations(calc_df)
        # S21 planner → Isento
        assert result.loc[1, "Valor_Tributado"] == "Isento"

    def test_nivel_servico(self, calc_df):
        result = run_calculations(calc_df)
        # S21 planner → 0.98
        assert result.loc[1, "Nivel_Servico"] == 0.98
        # U09 planner → 0.95
        assert result.loc[0, "Nivel_Servico"] == 0.95

    def test_no_ltd_columns(self):
        """Should handle DataFrame without LTD columns gracefully."""
        df = pd.DataFrame({
            "Codigo_Material": ["MAT001"],
            "Prazo_Entrega_Previsto": [30],
            "Preco_Unitario": [100.0],
            "Texto_Observacao_PT": ["normal"],
            "Saldo_Virtual": [10],
            "Volume": [1.0],
            "Quantidade_Ordem": [5],
            "Data_Ultimo_Pedido": ["2025-01-01"],
            "Data_Abertura": ["2026-01-01"],
            "Grupo_Mercadoria": ["1234"],
            "Planejador_MRP": ["U09"],
        })
        result = run_calculations(df)
        assert "TMD" in result.columns
        assert "Classificacao" in result.columns
