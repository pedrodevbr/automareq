"""
visuals.py — Rich terminal visualizations and summary generation.

Provides:
  - build_summary:       Collect key metrics from DataFrame into dict
  - render_summary:      Display summary using rich Tables and Panels
  - render_distribution: ASCII horizontal bar chart for a column
  - generate_charts:     Matplotlib charts saved to output folder
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Summary data builder (no rich dependency — pure dict)
# ---------------------------------------------------------------------------

def build_summary(df: pd.DataFrame) -> dict[str, Any]:
    """Collect key metrics from the DataFrame into a structured dict."""
    summary: dict[str, Any] = {
        "total_materials": len(df),
        "total_columns": len(df.columns),
    }

    if "Responsavel" in df.columns:
        summary["by_responsavel"] = (
            df["Responsavel"].value_counts().to_dict()
        )

    if "Classificacao" in df.columns:
        summary["by_classificacao"] = (
            df["Classificacao"].value_counts().to_dict()
        )

    if "Politica_Sugerida" in df.columns:
        summary["by_politica"] = (
            df["Politica_Sugerida"].value_counts().to_dict()
        )

    if "Grupo_MRP" in df.columns:
        summary["by_grupo_mrp"] = (
            df["Grupo_MRP"].astype(str).value_counts().to_dict()
        )

    if "classificacao_validacao" in df.columns:
        summary["by_validacao"] = (
            df["classificacao_validacao"].value_counts().to_dict()
        )

    # Numeric stats
    num_cols = [
        "PR_Calculado", "MAX_Calculado", "Demanda_Anual",
        "TMD", "CV", "Preco_Unitario",
    ]
    existing = [c for c in num_cols if c in df.columns]
    if existing:
        stats = df[existing].describe().loc[["mean", "std", "min", "max"]]
        summary["numeric_stats"] = {
            col: {
                "mean": round(stats.loc["mean", col], 2),
                "std": round(stats.loc["std", col], 2),
                "min": round(stats.loc["min", col], 2),
                "max": round(stats.loc["max", col], 2),
            }
            for col in existing
        }

    return summary


# ---------------------------------------------------------------------------
# Rich rendering (only imported when called — keeps light for test/CI)
# ---------------------------------------------------------------------------

def render_summary(df: pd.DataFrame, console=None) -> None:
    """Display comprehensive summary using rich Tables and Panels."""
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.columns import Columns

    if console is None:
        console = Console()

    summary = build_summary(df)

    # ── Header ────────────────────────────────────────────────
    console.print()
    console.print(Panel(
        f"[bold]{summary['total_materials']}[/bold] materiais  ·  "
        f"[bold]{summary['total_columns']}[/bold] colunas",
        title="[bold blue]Resumo dos Dados[/bold blue]",
        border_style="blue",
    ))

    # ── Distribution tables ───────────────────────────────────
    panels = []

    dist_sections = [
        ("by_responsavel", "Responsável", "cyan"),
        ("by_classificacao", "Classificação Demanda", "green"),
        ("by_politica", "Política Sugerida", "yellow"),
        ("by_grupo_mrp", "Grupo MRP", "magenta"),
        ("by_validacao", "Validação", "red"),
    ]

    for key, title, color in dist_sections:
        data = summary.get(key)
        if not data:
            continue

        t = Table(
            title=f"[bold {color}]{title}[/bold {color}]",
            show_header=True,
            header_style=f"bold {color}",
            border_style="dim",
            min_width=30,
        )
        t.add_column("Categoria", style="white")
        t.add_column("Qtd", justify="right", style="bold")
        t.add_column("Barra", min_width=15)

        total = sum(data.values())
        max_val = max(data.values()) if data else 1

        for label, count in sorted(data.items(), key=lambda x: -x[1]):
            bar_len = int((count / max_val) * 15)
            bar = f"[{color}]{'█' * bar_len}{'░' * (15 - bar_len)}[/{color}]"
            pct = f" {count/total*100:.0f}%" if total else ""
            t.add_row(str(label), f"{count}{pct}", bar)

        panels.append(t)

    # Show tables in pairs
    if panels:
        for i in range(0, len(panels), 2):
            batch = panels[i:i+2]
            console.print()
            console.print(Columns(batch, padding=(0, 4)))

    # ── Numeric stats ─────────────────────────────────────────
    num_data = summary.get("numeric_stats")
    if num_data:
        console.print()
        t = Table(
            title="[bold blue]Estatísticas Numéricas[/bold blue]",
            show_header=True,
            header_style="bold blue",
            border_style="dim",
        )
        t.add_column("Coluna", style="white")
        t.add_column("Média", justify="right", style="cyan")
        t.add_column("Desvio", justify="right", style="yellow")
        t.add_column("Mín", justify="right", style="green")
        t.add_column("Máx", justify="right", style="red")

        for col, stats in num_data.items():
            t.add_row(
                col,
                f"{stats['mean']:,.1f}",
                f"{stats['std']:,.1f}",
                f"{stats['min']:,.1f}",
                f"{stats['max']:,.1f}",
            )
        console.print(t)

    console.print()


def render_stage_results(results: dict, stages: list, console=None) -> None:
    """Render the pipeline status table with stage results."""
    from rich.console import Console
    from rich.table import Table

    if console is None:
        console = Console()

    t = Table(
        title="[bold]Pipeline Status[/bold]",
        show_header=True,
        header_style="bold white",
        border_style="blue",
        min_width=75,
    )
    t.add_column("#", justify="center", width=3, style="dim")
    t.add_column("Estágio", min_width=22)
    t.add_column("Status", justify="center", width=10)
    t.add_column("Tempo", justify="right", width=8)
    t.add_column("Resumo", min_width=25)

    status_styles = {
        "pending":  "[dim]PENDENTE[/dim]",
        "running":  "[bold yellow]RODANDO[/bold yellow]",
        "done":     "[bold green]FEITO[/bold green]",
        "error":    "[bold red]ERRO[/bold red]",
        "skipped":  "[dim italic]PULADO[/dim italic]",
    }

    for i, stage in enumerate(stages, 1):
        r = results.get(stage.key)
        if r is None:
            continue

        status_str = status_styles.get(r.status, r.status)
        time_str = f"{r.elapsed:.1f}s" if r.elapsed > 0 else "—"

        # Build summary string
        summ_parts = []
        if r.error:
            summ_parts.append(f"[red]{r.error[:40]}[/red]")
        elif r.summary:
            for k, v in r.summary.items():
                if isinstance(v, dict):
                    items = ", ".join(f"{kk}:{vv}" for kk, vv in list(v.items())[:3])
                    summ_parts.append(items)
                elif isinstance(v, list):
                    summ_parts.append(", ".join(str(x) for x in v[:5]))
                else:
                    summ_parts.append(f"{k}={v}")
        summ_str = " | ".join(summ_parts)[:50] if summ_parts else ""

        # Stage name with substages hint
        name_str = f"[bold]{stage.name}[/bold]"
        if stage.optional:
            name_str += " [dim](opt)[/dim]"

        t.add_row(str(i), name_str, status_str, time_str, summ_str)

    console.print(t)


# ---------------------------------------------------------------------------
# Matplotlib charts
# ---------------------------------------------------------------------------

def generate_charts(df: pd.DataFrame, output_dir: Path) -> list[Path]:
    """Generate matplotlib charts and save to output_dir.  Returns file paths."""
    saved: list[Path] = []
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Use existing visualization module
        from utils.visualization_module import visualization
        visualization(df, output_dir)
        # Collect generated files
        for f in output_dir.glob("*.png"):
            saved.append(f)
    except Exception as exc:
        logger.warning("Chart generation failed: %s", exc)

    # Additional: Validation pie chart
    if "classificacao_validacao" in df.columns:
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            fig, ax = plt.subplots(figsize=(8, 6))
            counts = df["classificacao_validacao"].value_counts()
            colors = ["#22c55e" if "OK" in str(k) else "#f59e0b" for k in counts.index]
            ax.pie(counts.values, labels=counts.index, autopct="%1.0f%%",
                   colors=colors, startangle=90)
            ax.set_title("Classificação de Validação")
            path = output_dir / "validacao_pie.png"
            fig.savefig(path, dpi=100, bbox_inches="tight")
            plt.close(fig)
            saved.append(path)
        except Exception as exc:
            logger.warning("Validation pie chart failed: %s", exc)

    return saved
