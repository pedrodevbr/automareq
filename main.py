"""
main.py — Pipeline principal de Automação de Requisições
=========================================================

Fluxo de execução:
  [1] Exportar relatórios do SAP          (opcional — descomente para produção)
  [2] Carregar dados do Excel → DataFrame
  [3] Filtrar / amostrar                  (modo teste vs. produção)
  [4] Validar dados mestre                (7 estágios + consolidação)
  [5] Calcular parâmetros de estoque      (PR, MAX, políticas)
  [6] Resumo dos dados                    (exibe resultados antes da análise)
  [7] Analisar materiais                  (regras + IA + Jira + pesquisa web)
  [8] Exportar dashboard para analistas
  [9] Separar por setor / grupo           (pastas por responsável)

Para ativar etapas opcionais, remova o comentário da linha correspondente.
"""

import sys
import time
import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ─────────────────────────────────────────────────────────────────────────────
# Utilitário: cabeçalho de etapa
# ─────────────────────────────────────────────────────────────────────────────

def _pipeline_header(step: int, title: str, detail: str = "") -> None:
    bar = "─" * 56
    print(f"\n┌{bar}┐")
    print(f"│  [{step}] {title:<50}│")
    if detail:
        print(f"│      {detail:<50}│")
    print(f"└{bar}┘")


# ═════════════════════════════════════════════════════════════════════════════
# [1] EXPORTAR RELATÓRIOS DO SAP
# ═════════════════════════════════════════════════════════════════════════════
# _pipeline_header(1, "SAP Export", "Extrai relatórios do SAP via automação")
# from services import sap_service
# sap_service.workflow_export_reports()


# ═════════════════════════════════════════════════════════════════════════════
# [2] CARREGAR DADOS
# ═════════════════════════════════════════════════════════════════════════════
_pipeline_header(2, "Carregamento de dados", "Lendo arquivos Excel de entrada")

from core.load import process_excel_data
df = process_excel_data()
print(f"   {len(df)} materiais carregados | {df['Responsavel'].nunique() if 'Responsavel' in df.columns else '?'} responsáveis")


# ═════════════════════════════════════════════════════════════════════════════
# [3] FILTRAR / AMOSTRAR
# ═════════════════════════════════════════════════════════════════════════════
_pipeline_header(3, "Filtro / Amostragem", "Modo de execução atual: TESTE")

# Produção → comente as linhas abaixo e descomente o filtro por responsável
# df = df[df["Responsavel"] == "PEDROHVB"]
# df = df.sample(n=min(10, len(df))).reset_index(drop=True)
print(f"   Amostra: {len(df)} materiais (altere min() ou comente para produção)")


# ═════════════════════════════════════════════════════════════════════════════
# [4] VALIDAR DADOS MESTRE
# ═════════════════════════════════════════════════════════════════════════════
# Stages disponíveis: leadtime | grpm | texts | obs | reference | images | ref_obs
# Exemplos:
#   run_validations(df)                              → todos os stages
#   run_validations(df, stages=["grpm", "texts"])    → apenas GRPM e textos
#   run_validations(df, export_debug_sheet=False)    → sem DEBUG_Full.xlsx
_pipeline_header(4, "Validação de dados mestre", "Stages: leadtime / grpm / ref_obs")

from core.validate import run_validations
t0 = time.time()
df = run_validations(df, stages=["leadtime", "grpm", "ref_obs"], export_debug_sheet=False)
print(f"   Concluído em {time.time()-t0:.1f}s")


# ═════════════════════════════════════════════════════════════════════════════
# [5] CALCULAR PARÂMETROS DE ESTOQUE
# ═════════════════════════════════════════════════════════════════════════════
_pipeline_header(5, "Cálculo de estoque", "TMD, CV, Classificação, PR, MAX, Política")

from core.calculate import run_calculations
t0 = time.time()
df = run_calculations(df)
print(f"   Concluído em {time.time()-t0:.1f}s")


# ═════════════════════════════════════════════════════════════════════════════
# [6] RESUMO DOS DADOS — Resultados antes da análise IA
# ═════════════════════════════════════════════════════════════════════════════
_pipeline_header(6, "Resumo dos dados", "Visão geral antes de iniciar análise IA")

print(f"\n   Total de materiais: {len(df)}")
print(f"   Colunas: {len(df.columns)}")

if "Responsavel" in df.columns:
    print(f"\n   --- Distribuição por Responsável ---")
    for resp, count in df["Responsavel"].value_counts().items():
        print(f"   {resp:<20s} {count:>5d}")

if "Classificacao" in df.columns:
    print(f"\n   --- Classificação de Demanda ---")
    for cls, count in df["Classificacao"].value_counts().items():
        print(f"   {cls:<25s} {count:>5d}")

if "Politica_Sugerida" in df.columns:
    print(f"\n   --- Política Sugerida ---")
    for pol, count in df["Politica_Sugerida"].value_counts().items():
        print(f"   {pol:<10s} {count:>5d}")

if "Grupo_MRP" in df.columns:
    print(f"\n   --- Grupo MRP ---")
    for grp, count in df["Grupo_MRP"].value_counts().items():
        print(f"   {str(grp):<10s} {count:>5d}")

if "classificacao_validacao" in df.columns:
    print(f"\n   --- Validação ---")
    for v, count in df["classificacao_validacao"].value_counts().items():
        print(f"   {v:<15s} {count:>5d}")

numeric_cols = ["PR_Calculado", "MAX_Calculado", "Demanda_Anual", "TMD", "CV", "Preco_Unitario"]
existing_nums = [c for c in numeric_cols if c in df.columns]
if existing_nums:
    print(f"\n   --- Estatísticas numéricas ---")
    stats = df[existing_nums].describe().loc[["mean", "std", "min", "max"]]
    for col in existing_nums:
        print(f"   {col:<25s}  mean={stats.loc['mean', col]:>10.1f}  std={stats.loc['std', col]:>10.1f}  min={stats.loc['min', col]:>10.1f}  max={stats.loc['max', col]:>10.1f}")

print()


# ═════════════════════════════════════════════════════════════════════════════
# [7] ANALISAR MATERIAIS
# ═════════════════════════════════════════════════════════════════════════════
# Etapas internas:
#   7.1  Inicialização de flags
#   7.2  Regras de negócio   (ZSTK / FRAC / AD / SMIT / ANA)
#   7.3  Análise com IA      (para itens que precisam de decisão)
#   7.4  Pesquisa de mercado (use_search=True habilita)
#   7.5  Leitura de Jira     (SMIT → busca tickets abertos)
#   7.6  Criação de tickets  (FRAC → abre ticket e altera MRP no SAP)
# _pipeline_header(7, "Análise de materiais", "Regras + IA + Jira + pesquisa web")
# from core.analysis import run_analysis
# df = run_analysis(df, use_jira=True, use_search=False)


# ═════════════════════════════════════════════════════════════════════════════
# [8] EXPORTAR DASHBOARD PARA ANALISTAS
# ═════════════════════════════════════════════════════════════════════════════
# _pipeline_header(8, "Dashboard para analistas", "Gera dashboard.html + dashboard_data.js")
# from utils.export_module import export_dashboard_data
# export_dashboard_data(df)


# ═════════════════════════════════════════════════════════════════════════════
# [9] SEPARAR POR SETOR / GRUPO
# ═════════════════════════════════════════════════════════════════════════════
# _pipeline_header(9, "Separação por setor/grupo", "Pastas por responsável + templates AD")
# from utils.export_module import separar_por_setor_grupo_taxacao
# separar_por_setor_grupo_taxacao()


# ─────────────────────────────────────────────────────────────────────────────
# FIM DO PIPELINE
# ─────────────────────────────────────────────────────────────────────────────
print("═" * 58)
print("  PIPELINE COMPLETO — verifique os arquivos de saída em:")
print("  data/<ano-mes>/output/<responsavel>/")
print("═" * 58 + "\n")
