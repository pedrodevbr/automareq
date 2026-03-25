"""Tests for core/analyzers/ — business rules, stages, and orchestration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from core.analyzers._base import init_analysis_columns, update_row
from core.analyzers.stages.ad import run_ad
from core.analyzers.stages.ana import run_ana


# ===========================================================================
# _base.py helpers
# ===========================================================================

class TestUpdateRow:
    def test_appends_logs(self):
        row = pd.Series({"pre_analise": "", "Analise_AI": ""})
        result = update_row(row, ["log1", "log2"])
        assert "log1" in result["pre_analise"]
        assert "log2" in result["pre_analise"]

    def test_sets_ai_decisao(self):
        row = pd.Series({"pre_analise": "", "Analise_AI": ""})
        result = update_row(row, [], ai_decisao="REPOR")
        assert result["Analise_AI"] == "REPOR"

    def test_empty_logs_no_change(self):
        row = pd.Series({"pre_analise": "existing", "Analise_AI": ""})
        result = update_row(row, [])
        assert result["pre_analise"] == "existing"


class TestInitAnalysisColumns:
    def test_adds_flag_columns(self):
        df = pd.DataFrame({"Codigo_Material": ["001"]})
        result = init_analysis_columns(df)
        for col in ["needs_ai", "needs_market_search", "needs_jira_search", "sugestao_jira_frac"]:
            assert col in result.columns
            assert result.loc[0, col] == False


# ===========================================================================
# ZSTK business rules
# ===========================================================================

class TestRuleZSTK:
    @pytest.fixture
    def zstk_row(self):
        return pd.Series({
            "Codigo_Material": "001",
            "Grupo_MRP": "ZSTK",
            "Classificacao": "Suave",
            "Valor_Total_Ordem": 100,
            "Volume_OP": 100,
            "Saldo_Virtual": 10,
            "RTP3": 5,
            "RTP6": 0,
            "PR_Atual": 20,
            "MAX_Atual": 50,
            "Quantidade_Ordem": 10,
            "Anos_Ultima_Compra": 1.0,
            "Demanda_Programada": False,
            "pre_analise": "",
            "Analise_AI": "",
            "needs_ai": False,
            "needs_market_search": False,
            "pos_analise": "",
        })

    def test_consumo_zero_returns_nao_repor(self, zstk_row):
        from core.analyzers.stages.zstk import _rule_zstk
        zstk_row["Classificacao"] = "CONSUMO ZERO"
        result = _rule_zstk(zstk_row)
        assert result["Analise_AI"] == "NAO_REPOR"

    def test_alto_valor_flag(self, zstk_row):
        from core.analyzers.stages.zstk import _rule_zstk
        zstk_row["Valor_Total_Ordem"] = 50000
        result = _rule_zstk(zstk_row)
        assert "alto valor" in result["pre_analise"]

    def test_alto_volume_flag(self, zstk_row):
        from core.analyzers.stages.zstk import _rule_zstk
        zstk_row["Volume_OP"] = 2_000_000
        result = _rule_zstk(zstk_row)
        assert "parcelada" in result["pre_analise"]

    def test_never_purchased_sets_market_search(self, zstk_row):
        from core.analyzers.stages.zstk import _rule_zstk
        zstk_row["Anos_Ultima_Compra"] = -1
        result = _rule_zstk(zstk_row)
        assert result["needs_market_search"] == True

    def test_outdated_price_sets_market_search(self, zstk_row):
        from core.analyzers.stages.zstk import _rule_zstk
        zstk_row["Anos_Ultima_Compra"] = 3.0
        result = _rule_zstk(zstk_row)
        assert result["needs_market_search"] == True

    def test_demanda_programada_sets_needs_ai(self, zstk_row):
        from core.analyzers.stages.zstk import _rule_zstk
        zstk_row["Demanda_Programada"] = True
        result = _rule_zstk(zstk_row)
        assert result["needs_ai"] == True

    def test_rtp6_sets_needs_ai(self, zstk_row):
        from core.analyzers.stages.zstk import _rule_zstk
        zstk_row["RTP6"] = 5
        result = _rule_zstk(zstk_row)
        assert result["needs_ai"] == True

    def test_no_demanda_defaults_repor(self, zstk_row):
        from core.analyzers.stages.zstk import _rule_zstk
        result = _rule_zstk(zstk_row)
        assert result["Analise_AI"] == "REPOR"
        assert result["needs_ai"] == False

    def test_virtual_plus_op_exceeds_max(self, zstk_row):
        from core.analyzers.stages.zstk import _rule_zstk
        zstk_row["Saldo_Virtual"] = 45
        zstk_row["Quantidade_Ordem"] = 10
        zstk_row["MAX_Atual"] = 50
        result = _rule_zstk(zstk_row)
        assert "DEMPRO" in str(result.get("pos_analise", ""))


# ===========================================================================
# FRAC business rules
# ===========================================================================

class TestRuleFRAC:
    @pytest.fixture
    def frac_row(self):
        return pd.Series({
            "Codigo_Material": "002",
            "Texto_Breve_Material": "Item FRAC",
            "Grupo_MRP": "FRAC",
            "Numero_Peca_Fabricante": "REF123",
            "Quantidade_LMR": 0,
            "Data_Ultimo_Pedido": pd.Timestamp("2025-01-01"),
            "Saldo_Virtual": 10,
            "pre_analise": "",
            "Analise_AI": "",
            "needs_market_search": False,
            "sugestao_jira_frac": False,
        })

    def test_frac_with_lmr_creates_jira_suggestion(self, frac_row):
        from core.analyzers.stages.frac import _rule_frac
        frac_row["Quantidade_LMR"] = 5
        result = _rule_frac(frac_row, jira=None, sap=None)
        assert result["sugestao_jira_frac"] == True
        assert result["Analise_AI"] == "NAO_REPOR"
        assert "JIRA FRAC" in result["pre_analise"]

    def test_frac_without_lmr(self, frac_row):
        from core.analyzers.stages.frac import _rule_frac
        result = _rule_frac(frac_row, jira=None, sap=None)
        assert "outra referência" in result["pre_analise"]

    def test_frac_always_needs_market_search(self, frac_row):
        from core.analyzers.stages.frac import _rule_frac
        result = _rule_frac(frac_row, jira=None, sap=None)
        assert result["needs_market_search"] == True


# ===========================================================================
# AD stage
# ===========================================================================

class TestAD:
    def test_sets_repor_and_text(self):
        df = pd.DataFrame({
            "Codigo_Material": ["003"],
            "Texto_Breve_Material": ["Motor 5HP"],
            "Grupo_MRP": ["AD"],
            "Quantidade_Ordem": [10],
            "Texto_PT": ["Motor elétrico"],
            "Numero_Peca_Fabricante": ["MOT-5HP"],
            "Texto_Observacao_PT": ["Obs text"],
            "pre_analise": [""],
            "Analise_AI": [""],
        })
        with patch("core.analyzers.stages.ad.save_checkpoint"):
            result = run_ad(df)
        assert result.loc[0, "Analise_AI"] == "REPOR"
        assert "Motor 5HP" in result.loc[0, "AD_texto"]
        assert "isenta de impostos" in result.loc[0, "AD_texto"]

    def test_ignores_non_ad(self):
        df = pd.DataFrame({
            "Codigo_Material": ["001"],
            "Grupo_MRP": ["ZSTK"],
            "pre_analise": [""],
            "Analise_AI": [""],
        })
        with patch("core.analyzers.stages.ad.save_checkpoint"):
            result = run_ad(df)
        assert result.loc[0, "Analise_AI"] == ""


# ===========================================================================
# ANA stage
# ===========================================================================

class TestANA:
    def test_sets_nao_repor_and_manual(self):
        df = pd.DataFrame({
            "Codigo_Material": ["004"],
            "Grupo_MRP": ["ANA"],
            "pre_analise": [""],
            "Analise_AI": [""],
        })
        with patch("core.analyzers.stages.ana.save_checkpoint"):
            result = run_ana(df)
        assert result.loc[0, "Analise_AI"] == "NAO_REPOR"
        assert "ANALISE MANUAL" in result.loc[0, "pre_analise"]

    def test_ignores_non_ana(self):
        df = pd.DataFrame({
            "Codigo_Material": ["001"],
            "Grupo_MRP": ["ZSTK"],
            "pre_analise": [""],
            "Analise_AI": [""],
        })
        with patch("core.analyzers.stages.ana.save_checkpoint"):
            result = run_ana(df)
        assert "ANALISE MANUAL" not in result.loc[0, "pre_analise"]


# ===========================================================================
# SMIT stage
# ===========================================================================

class TestSMIT:
    def test_sets_nao_repor_and_jira_search(self):
        from core.analyzers.stages.smit import run_smit
        df = pd.DataFrame({
            "Codigo_Material": ["005"],
            "Grupo_MRP": ["SMIT"],
            "pre_analise": [""],
            "Analise_AI": [""],
        })
        with patch("core.analyzers.stages.smit.save_checkpoint"):
            result = run_smit(df, jira=None)
        assert result.loc[0, "Analise_AI"] == "NAO_REPOR"
        assert result.loc[0, "needs_jira_search"] == True


# ===========================================================================
# Orchestrator
# ===========================================================================

class TestOrchestrator:
    def test_stage_selection(self):
        from core.analyzers.orchestrator import run_analysis
        df = pd.DataFrame({
            "Codigo_Material": ["001", "002"],
            "Grupo_MRP": ["AD", "ANA"],
            "pre_analise": ["", ""],
        })
        with patch("core.analyzers.stages.ad.save_checkpoint"), \
             patch("core.analyzers.stages.ana.save_checkpoint"):
            result = run_analysis(df, stages=["ad", "ana"], use_jira=False, use_search=False)
        assert result.loc[0, "Analise_AI"] == "REPOR"
        assert result.loc[1, "Analise_AI"] == "NAO_REPOR"

    def test_invalid_stage_raises(self):
        from core.analyzers.orchestrator import run_analysis
        df = pd.DataFrame({"Codigo_Material": ["001"], "Grupo_MRP": ["ZSTK"]})
        with pytest.raises(ValueError, match="desconhecido"):
            run_analysis(df, stages=["invalid_stage"])

    def test_empty_df_returns_empty(self):
        from core.analyzers.orchestrator import run_analysis
        df = pd.DataFrame()
        result = run_analysis(df, stages=["ad"], use_jira=False, use_search=False)
        assert result.empty

    def test_backward_compat_import(self):
        from core.analysis import run_analysis
        assert callable(run_analysis)

    def test_analyzers_import(self):
        from core.analyzers import run_analysis
        assert callable(run_analysis)
