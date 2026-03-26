"""
main.py — Pipeline principal de Automação de Requisições
=========================================================

Modos de uso:
  python main.py           → Execução linear (modo legado, compatível)
  python panel.py          → Painel de controle interativo (recomendado)
  python panel.py --auto   → Execução automática com painel visual

Fluxo de execução:
  [1] Exportar relatórios do SAP          (opcional)
  [2] Carregar dados do Excel → DataFrame
  [3] Filtrar / amostrar                  (modo teste vs. produção)
  [4] Validar dados mestre                (7 estágios + consolidação)
  [5] Calcular parâmetros de estoque      (PR, MAX, políticas)
  [6] Resumo dos dados                    (exibe resultados antes da análise)
  [7] Análise Fase 1 — SMIT + FRAC       (Jira + SAP)
  [8] Análise Fase 2 — ZSTK + AD + ANA   (IA + pesquisa)
  [9] Emissão — Dashboard + Separação + Templates + Envio
"""

from __future__ import annotations

import sys
import time

import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

def _pipeline_header(step: int, title: str, detail: str = "") -> None:
    bar = "─" * 56
    print(f"\n┌{bar}┐")
    print(f"│  [{step}] {title:<50}│")
    if detail:
        print(f"│      {detail:<50}│")
    print(f"└{bar}┘")


def filter_dataframe(
    df: pd.DataFrame,
    mode: str = "test",
    sample_size: int = 10,
    responsavel: str = "",
) -> pd.DataFrame:
    """Apply filtering / sampling based on mode."""
    if responsavel:
        df = df[df["Responsavel"].str.upper() == responsavel.upper()].reset_index(drop=True)
    if mode == "test":
        n = min(sample_size, len(df))
        df = df.sample(n=n, random_state=42).reset_index(drop=True)
    return df


def print_summary(df: pd.DataFrame) -> None:
    """Print tabular summary of pipeline results."""
    print(f"\n   Total de materiais: {len(df)}")
    print(f"   Colunas: {len(df.columns)}")

    sections = [
        ("Responsavel", "Distribuição por Responsável", 20),
        ("Classificacao", "Classificação de Demanda", 25),
        ("Politica_Sugerida", "Política Sugerida", 10),
        ("Grupo_MRP", "Grupo MRP", 10),
        ("classificacao_validacao", "Validação", 15),
    ]
    for col, title, width in sections:
        if col in df.columns:
            print(f"\n   --- {title} ---")
            for val, count in df[col].value_counts().items():
                print(f"   {str(val):<{width}s} {count:>5d}")

    numeric_cols = ["PR_Calculado", "MAX_Calculado", "Demanda_Anual", "TMD", "CV", "Preco_Unitario"]
    existing = [c for c in numeric_cols if c in df.columns]
    if existing:
        print(f"\n   --- Estatísticas numéricas ---")
        stats = df[existing].describe().loc[["mean", "std", "min", "max"]]
        for col in existing:
            print(
                f"   {col:<25s}  mean={stats.loc['mean', col]:>10.1f}"
                f"  std={stats.loc['std', col]:>10.1f}"
                f"  min={stats.loc['min', col]:>10.1f}"
                f"  max={stats.loc['max', col]:>10.1f}"
            )
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Linear pipeline (legacy mode)
# ─────────────────────────────────────────────────────────────────────────────

def run_linear_pipeline() -> pd.DataFrame:
    """Execute the full pipeline in linear mode (original behavior)."""

    # [1] SAP Export (optional)
    # _pipeline_header(1, "SAP Export", "Extrai relatórios do SAP via automação")
    # from services import sap_service
    # sap_service.workflow_export_reports()

    # [2] Load
    _pipeline_header(2, "Carregamento de dados", "Lendo arquivos Excel de entrada")
    from core.load import process_excel_data
    df = process_excel_data()
    print(f"   {len(df)} materiais carregados | {df['Responsavel'].nunique() if 'Responsavel' in df.columns else '?'} responsáveis")

    # [3] Filter
    _pipeline_header(3, "Filtro / Amostragem", "Modo de execução atual: TODOS")
    # df = filter_dataframe(df, mode="test", sample_size=10)
    print(f"   Amostra: {len(df)} materiais (altere min() ou comente para produção)")

    # [4] Validate
    _pipeline_header(4, "Validação de dados mestre", "Stages: leadtime / grpm / ref_obs")
    from core.validate import run_validations
    t0 = time.time()
    df = run_validations(df, stages=["leadtime", "grpm", "ref_obs"], export_debug_sheet=False)
    print(f"   Concluído em {time.time()-t0:.1f}s")

    # [5] Calculate
    _pipeline_header(5, "Cálculo de estoque", "TMD, CV, Classificação, PR, MAX, Política")
    from core.calculate import run_calculations
    t0 = time.time()
    df = run_calculations(df)
    print(f"   Concluído em {time.time()-t0:.1f}s")

    # [6] Summary
    _pipeline_header(6, "Resumo dos dados", "Visão geral antes de iniciar análise IA")
    print_summary(df)

    # [7] Analysis Phase 1 (optional)
    # _pipeline_header(7, "Análise — Fase 1", "SMIT + FRAC (Jira + SAP)")
    # from core.analysis import run_analysis
    # df = run_analysis(df, stages=["smit", "frac"], use_jira=True)

    # [8] Analysis Phase 2 (optional)
    # _pipeline_header(8, "Análise — Fase 2", "ZSTK + AD + ANA (IA + pesquisa)")
    # from core.analysis import run_analysis
    # df = run_analysis(df, stages=["zstk", "ad", "ana"], use_search=True)

    # [9] Emission (optional)
    # _pipeline_header(9, "Emissão", "Dashboard + Separação + Templates + Envio")
    # from core.emission import run_emission
    # run_emission(df, stages=["dashboard", "groups"])

    # Done
    print("═" * 58)
    print("  PIPELINE COMPLETO — verifique os arquivos de saída em:")
    print("  data/<ano-mes>/output/<responsavel>/")
    print("═" * 58 + "\n")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_linear_pipeline()
