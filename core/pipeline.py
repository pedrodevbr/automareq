"""
pipeline.py — Pipeline engine with stage registry, state tracking, and runner.

Provides:
  - PipelineConfig:    Dataclass with all user-configurable options
  - StageResult:       Per-stage execution result (status, time, metrics)
  - Pipeline:          Central orchestrator that wraps all 11 top-level stages
"""

from __future__ import annotations

import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class PipelineConfig:
    """All user-configurable options for a pipeline run."""

    mode: str = "test"                          # "test" | "production"
    sample_size: int = 10
    responsavel_filter: str = ""                # "" = all
    use_parquet_cache: bool = False

    # Sub-stage selections
    validation_stages: list[str] = field(
        default_factory=lambda: ["leadtime", "grpm", "ref_obs"],
    )
    analysis_p1_stages: list[str] = field(
        default_factory=lambda: ["jira_analysis", "smit", "frac"],
    )
    analysis_p2_stages: list[str] = field(
        default_factory=lambda: ["zstk", "ad", "ana"],
    )
    emission_stages: list[str] = field(
        default_factory=lambda: ["templates", "send"],
    )

    # Feature flags
    use_jira: bool = True
    use_search: bool = True
    export_debug: bool = False


# ---------------------------------------------------------------------------
# Stage result
# ---------------------------------------------------------------------------

@dataclass
class StageResult:
    """Execution result for a single pipeline stage."""

    status: str = "pending"         # pending | running | done | error | skipped
    elapsed: float = 0.0
    error: str = ""
    summary: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Stage definition
# ---------------------------------------------------------------------------

@dataclass
class StageDefinition:
    """Metadata for one top-level pipeline stage."""

    key: str
    name: str
    group: str                      # "Extração", "Validação", etc.
    description: str = ""
    substages: list[str] = field(default_factory=list)
    optional: bool = False
    requires_df: bool = True


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

# Stage registry (order matters — this IS the pipeline order)
_STAGE_DEFS: list[StageDefinition] = [
    StageDefinition(
        key="sap_export", name="SAP Export", group="Extração",
        description="Exporta relatórios via automação SAP",
        optional=True, requires_df=False,
    ),
    StageDefinition(
        key="load", name="Carregar Dados", group="Extração",
        description="ETL: Excel → Parquet → DataFrame",
        requires_df=False,
    ),
    StageDefinition(
        key="filter", name="Filtro / Amostra", group="Preparação",
        description="Modo teste ou produção",
    ),
    StageDefinition(
        key="validate", name="Validação", group="Validação",
        description="Lead time, GRPM, textos, OBS, referência, imagem",
        substages=["leadtime", "grpm", "texts", "obs", "reference", "images", "ref_obs"],
    ),
    StageDefinition(
        key="calculate", name="Cálculo de Estoque", group="Cálculo",
        description="TMD, CV, classificação, PR, MAX, política",
    ),
    StageDefinition(
        key="summary", name="Resumo / Visuais", group="Visualização",
        description="Estatísticas e gráficos",
    ),
    StageDefinition(
        key="analysis_p1", name="Análise Fase 1", group="Análise",
        description="JIRA Analysis + SMIT + FRAC (Jira/SAP)",
        substages=["jira_analysis", "smit", "frac"], optional=True,
    ),
    StageDefinition(
        key="analysis_p2", name="Análise Fase 2", group="Análise",
        description="ZSTK + AD + ANA (IA + pesquisa + ações)",
        substages=["zstk", "ad", "ana"], optional=True,
    ),
    StageDefinition(
        key="dashboard", name="Dashboard", group="Saída",
        description="Gera dashboard HTML interativo por analista",
        optional=True,
    ),
    StageDefinition(
        key="separacao", name="Separação por Grupos", group="Saída",
        description="3 pastas: grupos, pendentes, sem reposição + resumo",
        optional=True,
    ),
    StageDefinition(
        key="relatorios", name="Relatórios Acionáveis", group="Saída",
        description="Excel multi-abas com ações por analista",
        optional=True,
    ),
    StageDefinition(
        key="emission", name="Emissão (Envio)", group="Saída",
        description="Templates + zip + rascunhos de e-mail",
        substages=["templates", "send"],
        optional=True,
    ),
]


class Pipeline:
    """Central pipeline orchestrator — drives all 11 stages."""

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        self.stages = list(_STAGE_DEFS)
        self.results: dict[str, StageResult] = {
            s.key: StageResult() for s in self.stages
        }
        self.df: Optional[pd.DataFrame] = None

    # ── public API ─────────────────────────────────────────────

    def run_stage(self, key: str) -> StageResult:
        """Execute a single stage by key.  Returns its StageResult."""
        defn = self._get_defn(key)
        result = self.results[key]

        if defn.requires_df and self.df is None:
            result.status = "error"
            result.error = "DataFrame not loaded — run 'load' first."
            return result

        result.status = "running"
        t0 = time.time()

        try:
            runner = self._get_runner(key)
            runner()
            result.status = "done"
        except Exception as exc:
            result.status = "error"
            result.error = f"{type(exc).__name__}: {exc}"
            traceback.print_exc()
        finally:
            result.elapsed = time.time() - t0

        return result

    def run_selected(self, keys: list[str]) -> None:
        """Run multiple stages in pipeline order."""
        ordered = [s.key for s in self.stages if s.key in keys]
        for key in ordered:
            self.run_stage(key)
            if self.results[key].status == "error":
                break   # stop on first error

    def run_all(self, skip_optional: bool = True) -> None:
        """Run the full pipeline, optionally skipping optional stages."""
        keys = []
        for s in self.stages:
            if skip_optional and s.optional:
                self.results[s.key].status = "skipped"
                continue
            keys.append(s.key)
        self.run_selected(keys)

    def reset(self) -> None:
        """Reset all stage results to pending."""
        for key in self.results:
            self.results[key] = StageResult()
        self.df = None

    # ── internals ──────────────────────────────────────────────

    def _get_defn(self, key: str) -> StageDefinition:
        for s in self.stages:
            if s.key == key:
                return s
        raise ValueError(f"Unknown stage: {key}")

    def _get_runner(self, key: str) -> Callable:
        """Return the callable that executes this stage."""
        runners: dict[str, Callable] = {
            "sap_export":   self._run_sap_export,
            "load":         self._run_load,
            "filter":       self._run_filter,
            "validate":     self._run_validate,
            "calculate":    self._run_calculate,
            "summary":      self._run_summary,
            "analysis_p1":  self._run_analysis_p1,
            "analysis_p2":  self._run_analysis_p2,
            "dashboard":    self._run_dashboard,
            "separacao":    self._run_separacao,
            "relatorios":   self._run_relatorios,
            "emission":     self._run_emission,
        }
        return runners[key]

    # ── stage runners ──────────────────────────────────────────

    def _run_sap_export(self) -> None:
        from services.sap_service import SapManager
        sap = SapManager()
        sap.workflow_export_reports()
        self.results["sap_export"].summary = {"exported": True}

    def _run_load(self) -> None:
        from core.load import process_excel_data
        self.df = process_excel_data(use_parquet_cache=self.config.use_parquet_cache)
        n = len(self.df)
        n_resp = self.df["Responsavel"].nunique() if "Responsavel" in self.df.columns else 0
        self.results["load"].summary = {
            "materials": n,
            "responsaveis": n_resp,
            "columns": len(self.df.columns),
        }

    def _run_filter(self) -> None:
        assert self.df is not None
        cfg = self.config
        original = len(self.df)

        if cfg.responsavel_filter:
            self.df = self.df[
                self.df["Responsavel"].str.upper() == cfg.responsavel_filter.upper()
            ].reset_index(drop=True)

        if cfg.mode == "test":
            n = min(cfg.sample_size, len(self.df))
            self.df = self.df.sample(n=n, random_state=42).reset_index(drop=True)

        self.results["filter"].summary = {
            "original": original,
            "filtered": len(self.df),
            "mode": cfg.mode,
        }

    def _run_validate(self) -> None:
        assert self.df is not None
        from core.validate import run_validations
        self.df = run_validations(
            self.df,
            stages=self.config.validation_stages,
            export_debug_sheet=self.config.export_debug,
        )
        summary: dict[str, Any] = {
            "stages_run": list(self.config.validation_stages),
        }
        if "classificacao_validacao" in self.df.columns:
            summary["classification"] = (
                self.df["classificacao_validacao"].value_counts().to_dict()
            )
        self.results["validate"].summary = summary

    def _run_calculate(self) -> None:
        assert self.df is not None
        from core.calculate import run_calculations
        self.df = run_calculations(self.df)
        summary: dict[str, Any] = {}
        if "Politica_Sugerida" in self.df.columns:
            summary["policies"] = (
                self.df["Politica_Sugerida"].value_counts().to_dict()
            )
        if "Classificacao" in self.df.columns:
            summary["classifications"] = (
                self.df["Classificacao"].value_counts().to_dict()
            )
        self.results["calculate"].summary = summary

    def _run_summary(self) -> None:
        assert self.df is not None
        from utils.visuals import build_summary
        self.results["summary"].summary = build_summary(self.df)

    def _run_analysis_p1(self) -> None:
        assert self.df is not None
        from core.analysis import run_analysis
        self.df = run_analysis(
            self.df,
            stages=self.config.analysis_p1_stages,
            use_jira=self.config.use_jira,
        )
        self.results["analysis_p1"].summary = {
            "stages_run": list(self.config.analysis_p1_stages),
        }

    def _run_analysis_p2(self) -> None:
        assert self.df is not None
        from core.analysis import run_analysis
        self.df = run_analysis(
            self.df,
            stages=self.config.analysis_p2_stages,
            use_search=self.config.use_search,
        )
        summary: dict[str, Any] = {
            "stages_run": list(self.config.analysis_p2_stages),
        }
        if "Analise_AI" in self.df.columns:
            summary["decisions"] = (
                self.df["Analise_AI"].value_counts().to_dict()
            )
        # Collect suggested actions summary
        if "acoes_sugeridas" in self.df.columns:
            has_actions = self.df["acoes_sugeridas"].notna().sum()
            summary["with_actions"] = int(has_actions)
        self.results["analysis_p2"].summary = summary

    def _run_dashboard(self) -> None:
        assert self.df is not None
        from core.emitters.stages.dashboard import export_dashboard_data
        export_dashboard_data(self.df)
        n_resp = self.df["Responsavel"].nunique() if "Responsavel" in self.df.columns else 1
        self.results["dashboard"].summary = {"analysts": n_resp}

    def _run_separacao(self) -> None:
        assert self.df is not None
        from core.emitters.stages.group_separation import separar_por_setor_grupo_taxacao
        from utils.export_core import export_by_responsavel
        # Export the full analysis first so separation has data
        export_by_responsavel(self.df, filename="Relatorio")
        separar_por_setor_grupo_taxacao(df=self.df)
        self.results["separacao"].summary = {"materials": len(self.df)}

    def _run_relatorios(self) -> None:
        assert self.df is not None
        from utils.actionable_report import generate_all_reports
        from config.paths import OUTPUT_FOLDER
        results = generate_all_reports(self.df, OUTPUT_FOLDER)
        self.results["relatorios"].summary = {
            "reports": len(results),
        }

    def _run_emission(self) -> None:
        assert self.df is not None
        from core.emission import run_emission
        run_emission(self.df, stages=self.config.emission_stages)
        self.results["emission"].summary = {
            "stages_run": list(self.config.emission_stages),
        }
