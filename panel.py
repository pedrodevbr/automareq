"""
panel.py — Interactive CLI Control Panel for AutomaReq Pipeline
================================================================

Usage:
    python panel.py              → Interactive mode (menu-driven)
    python panel.py --auto       → Run default stages automatically
    python panel.py --prod       → Production mode (all materials)

Features:
  - Visual pipeline status table with stage tracking
  - Configurable mode (test/production), sample size, stage selection
  - Rich terminal output with colored tables, progress bars, bar charts
  - Post-stage summary with distributions and statistics
  - Chart generation to output folder
"""

from __future__ import annotations

import sys
from pathlib import Path

# Encoding fix for Windows terminals
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.table import Table
from rich.rule import Rule

from core.pipeline import Pipeline, PipelineConfig

console = Console()


# ═══════════════════════════════════════════════════════════════════════════
# Banner
# ═══════════════════════════════════════════════════════════════════════════

BANNER = r"""
[bold blue]   _         _        __  __       ___
  /_\  _  _| |_ ___ |  \/  |__ _| _ \___ __ _
 / _ \| || |  _/ _ \| |\/| / _` |   / -_) _` |
/_/ \_\\_,_|\__\___/|_|  |_\__,_|_|_\___\__, |
                                            |_|[/bold blue]
[dim]Pipeline de Automação de Requisições — Itaipu Binacional[/dim]
"""


def show_banner() -> None:
    console.print(Panel(BANNER, border_style="blue", padding=(0, 2)))


# ═══════════════════════════════════════════════════════════════════════════
# Pipeline status display
# ═══════════════════════════════════════════════════════════════════════════

def show_status(pipeline: Pipeline) -> None:
    """Display the pipeline status table."""
    from utils.visuals import render_stage_results
    console.print()
    render_stage_results(pipeline.results, pipeline.stages, console)


# ═══════════════════════════════════════════════════════════════════════════
# Configuration menu
# ═══════════════════════════════════════════════════════════════════════════

def configure(pipeline: Pipeline) -> None:
    """Interactive configuration submenu."""
    cfg = pipeline.config
    console.print()
    console.print(Rule("[bold cyan]Configuração[/bold cyan]"))

    # Mode
    console.print(f"\n  Modo atual: [bold]{cfg.mode}[/bold]  |  Amostra: [bold]{cfg.sample_size}[/bold]")
    console.print(f"  Filtro responsável: [bold]{cfg.responsavel_filter or 'todos'}[/bold]")
    console.print(f"  Cache Parquet: [bold]{cfg.use_parquet_cache}[/bold]")
    console.print()

    mode = Prompt.ask(
        "  Modo de execução",
        choices=["test", "production", "skip"],
        default="skip",
    )
    if mode != "skip":
        cfg.mode = mode

    if cfg.mode == "test":
        size = Prompt.ask(
            "  Tamanho da amostra",
            default=str(cfg.sample_size),
        )
        try:
            cfg.sample_size = int(size)
        except ValueError:
            pass

    resp = Prompt.ask(
        "  Filtrar por responsável (vazio = todos)",
        default=cfg.responsavel_filter,
    )
    cfg.responsavel_filter = resp.strip()

    cache = Confirm.ask("  Usar cache Parquet?", default=cfg.use_parquet_cache)
    cfg.use_parquet_cache = cache

    # Validation stages
    console.print()
    console.print("  [bold]Substages de Validação[/bold]")
    console.print(f"  Disponíveis: leadtime, grpm, texts, obs, reference, images, ref_obs")
    console.print(f"  Atuais: [cyan]{', '.join(cfg.validation_stages)}[/cyan]")
    val_input = Prompt.ask(
        "  Nova seleção (separar por vírgula, ou Enter para manter)",
        default="",
    )
    if val_input.strip():
        cfg.validation_stages = [s.strip() for s in val_input.split(",") if s.strip()]

    # Emission stages
    console.print()
    console.print("  [bold]Substages de Emissão[/bold]")
    console.print(f"  Disponíveis: dashboard, groups, templates, send")
    console.print(f"  Atuais: [cyan]{', '.join(cfg.emission_stages)}[/cyan]")
    em_input = Prompt.ask(
        "  Nova seleção (separar por vírgula, ou Enter para manter)",
        default="",
    )
    if em_input.strip():
        cfg.emission_stages = [s.strip() for s in em_input.split(",") if s.strip()]

    console.print()
    console.print("[green]  ✓ Configuração atualizada.[/green]")
    console.print()


# ═══════════════════════════════════════════════════════════════════════════
# Stage selection menu
# ═══════════════════════════════════════════════════════════════════════════

def select_stages(pipeline: Pipeline) -> list[str]:
    """Let user pick which stages to run."""
    console.print()
    console.print(Rule("[bold cyan]Selecionar Estágios[/bold cyan]"))
    console.print()

    t = Table(show_header=True, header_style="bold", border_style="dim")
    t.add_column("#", width=3, justify="center")
    t.add_column("Chave", width=14)
    t.add_column("Estágio", width=22)
    t.add_column("Status", width=10, justify="center")
    t.add_column("Opt", width=4, justify="center")

    for i, s in enumerate(pipeline.stages, 1):
        r = pipeline.results[s.key]
        status_map = {
            "pending": "[dim]—[/dim]",
            "done": "[green]✓[/green]",
            "error": "[red]✗[/red]",
            "skipped": "[dim]~[/dim]",
        }
        st = status_map.get(r.status, r.status)
        opt = "[dim]✓[/dim]" if s.optional else ""
        t.add_row(str(i), s.key, s.name, st, opt)

    console.print(t)
    console.print()

    # Preset shortcuts
    console.print("  [bold]Atalhos:[/bold]")
    console.print("    [cyan]core[/cyan]    → load, filter, validate, calculate, summary")
    console.print("    [cyan]full[/cyan]    → todos os estágios (incluindo opcionais)")
    console.print("    [cyan]emit[/cyan]    → emission apenas")
    console.print()

    choice = Prompt.ask(
        "  Estágios (números ou chaves separados por vírgula, ou atalho)",
        default="core",
    )

    # Resolve shortcuts
    shortcuts = {
        "core": ["load", "filter", "validate", "calculate", "summary"],
        "full": [s.key for s in pipeline.stages],
        "emit": ["emission"],
        "analysis": ["analysis_p1", "analysis_p2"],
        "validate": ["validate"],
    }

    if choice.strip().lower() in shortcuts:
        return shortcuts[choice.strip().lower()]

    # Parse comma-separated list (numbers or keys)
    parts = [p.strip() for p in choice.split(",") if p.strip()]
    selected = []
    for p in parts:
        if p.isdigit():
            idx = int(p) - 1
            if 0 <= idx < len(pipeline.stages):
                selected.append(pipeline.stages[idx].key)
        else:
            # Match by key
            if any(s.key == p for s in pipeline.stages):
                selected.append(p)
    return selected or shortcuts["core"]


# ═══════════════════════════════════════════════════════════════════════════
# Run pipeline
# ═══════════════════════════════════════════════════════════════════════════

def run_pipeline(pipeline: Pipeline, stages: list[str]) -> None:
    """Execute selected stages with visual progress."""
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn

    console.print()
    console.print(Rule("[bold green]Executando Pipeline[/bold green]"))
    console.print()

    ordered = [s for s in pipeline.stages if s.key in stages]

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}[/bold blue]"),
        BarColumn(bar_width=30),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        overall = progress.add_task("Pipeline", total=len(ordered))

        for stage in ordered:
            task_id = progress.add_task(f"  {stage.name}", total=1)

            result = pipeline.run_stage(stage.key)

            if result.status == "done":
                progress.update(task_id, completed=1, description=f"  [green]✓[/green] {stage.name}")
            else:
                progress.update(task_id, completed=1, description=f"  [red]✗[/red] {stage.name}")

            progress.advance(overall)

            if result.status == "error":
                console.print(f"\n  [red bold]ERRO em {stage.name}:[/red bold] {result.error}")
                if not Confirm.ask("  Continuar com os próximos estágios?", default=False):
                    break

    console.print()
    show_status(pipeline)


# ═══════════════════════════════════════════════════════════════════════════
# Results view
# ═══════════════════════════════════════════════════════════════════════════

def show_results(pipeline: Pipeline) -> None:
    """Show detailed results with distributions."""
    console.print()
    console.print(Rule("[bold cyan]Resultados[/bold cyan]"))

    if pipeline.df is None:
        console.print("\n  [dim]Nenhum dado carregado ainda.[/dim]\n")
        return

    from utils.visuals import render_summary
    render_summary(pipeline.df, console)


# ═══════════════════════════════════════════════════════════════════════════
# Charts
# ═══════════════════════════════════════════════════════════════════════════

def show_charts(pipeline: Pipeline) -> None:
    """Generate and report on charts."""
    console.print()
    console.print(Rule("[bold cyan]Gráficos[/bold cyan]"))

    if pipeline.df is None:
        console.print("\n  [dim]Nenhum dado carregado ainda.[/dim]\n")
        return

    from config.paths import OUTPUT_FOLDER
    from utils.visuals import generate_charts

    output_dir = Path(OUTPUT_FOLDER) / "charts"
    console.print(f"\n  Gerando gráficos em: [cyan]{output_dir}[/cyan]")

    try:
        saved = generate_charts(pipeline.df, output_dir)
        if saved:
            console.print(f"  [green]✓ {len(saved)} gráficos gerados:[/green]")
            for p in saved:
                console.print(f"    [dim]{p}[/dim]")
        else:
            console.print("  [yellow]Nenhum gráfico gerado (verifique matplotlib).[/yellow]")
    except Exception as exc:
        console.print(f"  [red]Erro: {exc}[/red]")

    console.print()


# ═══════════════════════════════════════════════════════════════════════════
# Data inspector
# ═══════════════════════════════════════════════════════════════════════════

def inspect_data(pipeline: Pipeline) -> None:
    """Quick data inspection — show head, columns, dtypes."""
    console.print()
    console.print(Rule("[bold cyan]Inspeção de Dados[/bold cyan]"))

    if pipeline.df is None:
        console.print("\n  [dim]Nenhum dado carregado ainda.[/dim]\n")
        return

    df = pipeline.df
    console.print(f"\n  Shape: [bold]{df.shape[0]}[/bold] linhas × [bold]{df.shape[1]}[/bold] colunas")
    console.print(f"  Memória: [bold]{df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB[/bold]")

    # Show first few columns with their dtypes
    t = Table(title="Colunas", show_header=True, border_style="dim", min_width=50)
    t.add_column("#", width=4, justify="right", style="dim")
    t.add_column("Coluna", min_width=25)
    t.add_column("Tipo", width=12)
    t.add_column("Não-nulos", width=10, justify="right")
    t.add_column("Exemplo", min_width=20)

    for i, col in enumerate(df.columns):
        non_null = df[col].notna().sum()
        example = str(df[col].dropna().iloc[0]) if non_null > 0 else "—"
        if len(example) > 35:
            example = example[:32] + "..."
        t.add_row(
            str(i + 1),
            col,
            str(df[col].dtype),
            f"{non_null}/{len(df)}",
            example,
        )
    console.print(t)

    # Show sample rows
    console.print()
    n_preview = min(3, len(df))
    preview_cols = [c for c in ["Codigo_Material", "Texto_Breve_Material", "Responsavel",
                                 "Grupo_MRP", "classificacao_validacao", "Politica_Sugerida"]
                    if c in df.columns]
    if preview_cols:
        t2 = Table(title=f"Amostra ({n_preview} linhas)", border_style="dim")
        for c in preview_cols:
            t2.add_column(c, max_width=25)
        for _, row in df.head(n_preview).iterrows():
            t2.add_row(*[str(row.get(c, ""))[:25] for c in preview_cols])
        console.print(t2)

    console.print()


# ═══════════════════════════════════════════════════════════════════════════
# Main menu
# ═══════════════════════════════════════════════════════════════════════════

MENU = """
  [bold cyan][1][/bold cyan] Configurar         [bold cyan][2][/bold cyan] Selecionar estágios
  [bold cyan][3][/bold cyan] Executar pipeline   [bold cyan][4][/bold cyan] Ver resultados
  [bold cyan][5][/bold cyan] Gerar gráficos      [bold cyan][6][/bold cyan] Inspecionar dados
  [bold cyan][7][/bold cyan] Resetar             [bold cyan][0][/bold cyan] Sair
"""


def main_loop(pipeline: Pipeline) -> None:
    """Interactive menu loop."""
    selected_stages: list[str] = ["load", "filter", "validate", "calculate", "summary"]

    while True:
        console.print(MENU)
        console.print(f"  [dim]Estágios selecionados: {', '.join(selected_stages)}[/dim]")
        console.print(f"  [dim]Modo: {pipeline.config.mode} | Amostra: {pipeline.config.sample_size}[/dim]")

        choice = Prompt.ask("\n  Opção", choices=["0", "1", "2", "3", "4", "5", "6", "7"], default="3")

        if choice == "0":
            console.print("\n  [dim]Até logo![/dim]\n")
            break
        elif choice == "1":
            configure(pipeline)
        elif choice == "2":
            selected_stages = select_stages(pipeline)
            console.print(f"\n  [green]✓ Selecionados: {', '.join(selected_stages)}[/green]\n")
        elif choice == "3":
            run_pipeline(pipeline, selected_stages)
        elif choice == "4":
            show_results(pipeline)
        elif choice == "5":
            show_charts(pipeline)
        elif choice == "6":
            inspect_data(pipeline)
        elif choice == "7":
            pipeline.reset()
            console.print("\n  [green]✓ Pipeline resetado.[/green]\n")


# ═══════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    """Entry point — parse CLI args and launch."""
    config = PipelineConfig()

    # Simple CLI arg parsing
    if "--prod" in sys.argv or "--production" in sys.argv:
        config.mode = "production"
    if "--auto" in sys.argv:
        config.mode = "test"

    pipeline = Pipeline(config)

    show_banner()
    show_status(pipeline)

    if "--auto" in sys.argv:
        # Auto mode: run core stages, show results, exit
        stages = ["load", "filter", "validate", "calculate", "summary"]
        run_pipeline(pipeline, stages)
        show_results(pipeline)
    else:
        main_loop(pipeline)


if __name__ == "__main__":
    main()
