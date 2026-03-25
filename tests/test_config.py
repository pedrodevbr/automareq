"""Tests for config consolidation — verify the shim re-exports everything."""


class TestConfigShim:
    """All downstream imports via config.config must still resolve."""

    def test_ai_exports(self):
        from config.config import ai_model_text, ai_model_analysis
        assert isinstance(ai_model_text, str)
        assert isinstance(ai_model_analysis, str)

    def test_business_exports(self):
        from config.config import (
            CV_THRESHOLD, TMD_THRESHOLD, DEMAND_WINDOW,
            BAIXO_VALOR, ALTO_VALOR, VALOR_UN_ALTO, ALTO_VOLUME,
            ANOS_SEM_OC, CUSTO_FIXO_PEDIDO, TAXA_MANUTENCAO,
        )
        assert DEMAND_WINDOW == 12
        assert 0 < CV_THRESHOLD < 1
        assert CUSTO_FIXO_PEDIDO > 0
        assert 0 < TAXA_MANUTENCAO < 1

    def test_personnel_exports(self):
        from config.config import RESPONSAVEIS, PLANEJADORES, country_for_responsavel
        assert "PEDROHVB" in RESPONSAVEIS
        assert country_for_responsavel("PEDROHVB") == "BR"
        assert country_for_responsavel("DGOMEZ") == "PY"
        assert "S21" in PLANEJADORES

    def test_paths_exports(self):
        from config.config import INPUT_FOLDER, OUTPUT_FOLDER, DATA_FOLDER, TEMPLATES_FOLDER
        from pathlib import Path
        assert isinstance(Path(INPUT_FOLDER), Path)
        assert isinstance(Path(OUTPUT_FOLDER), Path)

    def test_prompts_exports(self):
        from config.config import SYSTEM_PROMPT, USER_PROMPT_TEMPLATE
        assert "estoque" in SYSTEM_PROMPT.lower()
        assert "{material_data}" in USER_PROMPT_TEMPLATE

    def test_sources_exports(self):
        from config.config import ANALYSIS_COLUMNS, SOURCE_OP
        assert isinstance(ANALYSIS_COLUMNS, list)
        assert len(ANALYSIS_COLUMNS) > 10


class TestDirectImports:
    """Verify the new module files can be imported directly."""

    def test_import_ai(self):
        from config.ai import ai_model_text, ai_model_analysis
        assert ai_model_text
        assert ai_model_analysis

    def test_import_business(self):
        from config.business import CV_THRESHOLD, DEMAND_WINDOW
        assert CV_THRESHOLD > 0
        assert DEMAND_WINDOW > 0

    def test_import_sources(self):
        from config.sources import SOURCE_OP, SOURCE_0127, SOURCE_0130
        assert SOURCE_OP.name == "OP"
        assert SOURCE_0127.name == "0127"
        assert SOURCE_0130.name == "0130"
