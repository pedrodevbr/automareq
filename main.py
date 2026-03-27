"""
main.py — Pipeline principal de Automação de Requisições
=========================================================

Modos de uso:
  python main.py           -> Execução linear (modo legado, compatível)
  python panel.py          -> Painel de controle interativo (recomendado)
  python panel.py --auto   -> Execução automática com painel visual

Fluxo de execução:
  [1] Exportar relatórios do SAP          (opcional)
  [2] Carregar dados do Excel -> DataFrame
  [3] Filtrar / amostrar                  (modo teste vs. produção)
  [4] Validar dados mestre                (7 estágios + consolidação)
  [5] Calcular parâmetros de estoque      (PR, MAX, políticas)
  [6] Resumo dos dados                    (exibe resultados antes da análise)
  [7] Análise Fase 1 — SMIT + FRAC       (Jira + SAP)
  [8] Análise Fase 2 — ZSTK + AD + ANA   (IA + pesquisa)
  [9] Emissão — Dashboard + Separação + Templates + Envio
"""

from __future__ import annotations

import pandas as pd

from utils.formatting import configure_encoding

configure_encoding()


def run_linear_pipeline() -> pd.DataFrame:
    """Execute the full pipeline in linear mode using the Pipeline engine."""
    from core.pipeline import Pipeline, PipelineConfig

    config = PipelineConfig(mode="production")
    pipeline = Pipeline(config)

    stages = ["load", "filter", "validate", "calculate", "summary"]
    pipeline.run_selected(stages)

    # Print final status
    for key in stages:
        result = pipeline.results[key]
        status = "OK" if result.status == "done" else result.status.upper()
        print(f"  [{status}] {key}: {result.elapsed:.1f}s")

    print("═" * 58)
    print("  PIPELINE COMPLETO — verifique os arquivos de saída em:")
    print("  data/<ano-mes>/output/<responsavel>/")
    print("═" * 58 + "\n")

    return pipeline.df


if __name__ == "__main__":
    run_linear_pipeline()
