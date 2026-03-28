"""
app.py — Streamlit GUI for AutomaReq Pipeline
================================================

Launch with:
    streamlit run app.py

Full GUI with:
  - Pipeline execution with progress tracking
  - Interactive data exploration with filters
  - Actionable results view per analyst
  - Report generation and download
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import pandas as pd
import streamlit as st

from core.pipeline import Pipeline, PipelineConfig

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="AutomaReq",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

if "pipeline" not in st.session_state:
    st.session_state.pipeline = Pipeline(PipelineConfig())
if "run_log" not in st.session_state:
    st.session_state.run_log = []

pipeline: Pipeline = st.session_state.pipeline

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    .stMetricValue { font-size: 1.8rem !important; }
    .priority-urgente { background-color: #fed7d7; padding: 4px 8px; border-radius: 4px; color: #9b2c2c; font-weight: bold; }
    .priority-alta { background-color: #feebc8; padding: 4px 8px; border-radius: 4px; color: #7b341e; font-weight: bold; }
    .priority-media { background-color: #fefcbf; padding: 4px 8px; border-radius: 4px; color: #744210; }
    .priority-baixa { background-color: #c6f6d5; padding: 4px 8px; border-radius: 4px; color: #276749; }
    div[data-testid="stExpander"] { border-left: 3px solid #2b6cb0; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("AutomaReq")
    st.caption("Pipeline de Requisicoes")
    st.divider()

    st.subheader("Configuracao")

    mode = st.radio("Modo", ["test", "production"], index=0, horizontal=True)
    pipeline.config.mode = mode

    if mode == "test":
        pipeline.config.sample_size = st.slider(
            "Amostra", min_value=5, max_value=200,
            value=pipeline.config.sample_size, step=5,
        )

    pipeline.config.responsavel_filter = st.text_input(
        "Filtrar Responsavel", value=pipeline.config.responsavel_filter,
        placeholder="Vazio = todos",
    )

    pipeline.config.use_parquet_cache = st.checkbox(
        "Cache Parquet", value=pipeline.config.use_parquet_cache,
    )

    st.divider()
    st.subheader("Stages")

    all_val = ["leadtime", "grpm", "texts", "obs", "reference", "images", "ref_obs"]
    pipeline.config.validation_stages = st.multiselect(
        "Validacao", options=all_val, default=pipeline.config.validation_stages,
    )

    col_j, col_s = st.columns(2)
    with col_j:
        pipeline.config.use_jira = st.checkbox("JIRA", value=pipeline.config.use_jira)
    with col_s:
        pipeline.config.use_search = st.checkbox("Pesquisa Web", value=pipeline.config.use_search)

    st.divider()
    if st.button("Resetar Pipeline", use_container_width=True, type="secondary"):
        pipeline.reset()
        st.session_state.run_log = []
        st.rerun()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_stages(keys: list[str]) -> None:
    """Run pipeline stages with progress bar."""
    progress = st.progress(0, text="Iniciando...")
    for i, key in enumerate(keys):
        defn = pipeline._get_defn(key)
        progress.progress((i) / len(keys), text=f"Executando {defn.name}...")
        result = pipeline.run_stage(key)
        if result.status == "done":
            st.toast(f"{defn.name} concluido em {result.elapsed:.1f}s")
        elif result.status == "error":
            st.toast(f"ERRO {defn.name}: {result.error[:80]}", icon="🚨")
            progress.empty()
            st.rerun()
            return
    progress.progress(1.0, text="Concluido!")
    st.rerun()


def _format_action_list(val) -> str:
    """Format acoes_sugeridas for display."""
    if pd.isna(val) or val == "" or val is None:
        return ""
    if isinstance(val, list):
        items = val
    elif isinstance(val, str) and val.startswith("["):
        try:
            import ast
            items = ast.literal_eval(val)
        except Exception:
            return val
    else:
        return str(val)
    if not items:
        return ""
    return "\n".join(f"{i+1}. {a}" for i, a in enumerate(items))


def _priority_badge(prio: str) -> str:
    """Return HTML badge for priority."""
    css_class = f"priority-{prio.lower()}" if prio else "priority-baixa"
    return f'<span class="{css_class}">{prio}</span>'


def _get_df() -> pd.DataFrame | None:
    return pipeline.df


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("AutomaReq")

# Quick metrics if data loaded
df = _get_df()
if df is not None:
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Materiais", len(df))

    if "Analise_AI" in df.columns:
        n_repor = int((df["Analise_AI"] == "REPOR").sum())
        c2.metric("REPOR", n_repor)
    if "Prioridade" in df.columns:
        n_urg = int((df["Prioridade"] == "URGENTE").sum())
        c3.metric("Urgentes", n_urg)
    if "classificacao_validacao" in df.columns:
        n_rev = int(df["classificacao_validacao"].astype(str).str.contains("REVISAR", na=False).sum())
        c4.metric("Revisar", n_rev)
    if "Responsavel" in df.columns:
        c5.metric("Analistas", df["Responsavel"].nunique())

st.divider()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_pipe, tab_acoes, tab_data, tab_results, tab_export = st.tabs([
    "Pipeline", "Acoes por Material", "Dados", "Resultados", "Exportar Relatorios",
])


# ═══════════════════════════════════════════════════════════════════════════
# Tab 1: Pipeline
# ═══════════════════════════════════════════════════════════════════════════

with tab_pipe:
    # Status table
    status_data = []
    for i, stage in enumerate(pipeline.stages, 1):
        r = pipeline.results[stage.key]
        emoji = {"pending": "⏳", "running": "🔄", "done": "✅", "error": "❌", "skipped": "⏭️"}
        info = ""
        if r.summary:
            parts = []
            for k, v in r.summary.items():
                if isinstance(v, dict):
                    parts.append(", ".join(f"{kk}: {vv}" for kk, vv in list(v.items())[:3]))
                elif isinstance(v, list):
                    parts.append(", ".join(str(x) for x in v[:3]))
                else:
                    parts.append(f"{k}={v}")
            info = " | ".join(parts)[:60]
        if r.error:
            info = r.error[:60]

        status_data.append({
            "#": i,
            "Estagio": stage.name,
            "Status": f"{emoji.get(r.status, '?')} {r.status.upper()}",
            "Tempo": f"{r.elapsed:.1f}s" if r.elapsed > 0 else "-",
            "Info": info,
        })

    st.dataframe(pd.DataFrame(status_data), use_container_width=True, hide_index=True, height=430)

    # Run buttons
    st.divider()
    c1, c2, c3, c4, c5 = st.columns(5)

    with c1:
        if st.button("Core Pipeline", use_container_width=True, type="primary",
                     help="Load + Filter + Validate + Calculate + Summary"):
            _run_stages(["load", "filter", "validate", "calculate", "summary"])
    with c2:
        if st.button("Analise", use_container_width=True,
                     help="Fase 1 (JIRA) + Fase 2 (AI + Pesquisa)"):
            _run_stages(["analysis_p1", "analysis_p2"])
    with c3:
        if st.button("Dashboard", use_container_width=True):
            _run_stages(["dashboard"])
    with c4:
        if st.button("Separacao", use_container_width=True,
                     help="Separa materiais por grupo + gera resumo"):
            _run_stages(["separacao"])
    with c5:
        if st.button("Emissao", use_container_width=True):
            _run_stages(["emission"])

    # Individual stages
    with st.expander("Executar estagio individual"):
        cols = st.columns(4)
        for i, stage in enumerate(pipeline.stages):
            with cols[i % 4]:
                if st.button(stage.name, key=f"ind_{stage.key}", use_container_width=True):
                    _run_stages([stage.key])

    # Errors
    for stage in pipeline.stages:
        r = pipeline.results[stage.key]
        if r.status == "error":
            st.error(f"**{stage.name}**: {r.error}")


# ═══════════════════════════════════════════════════════════════════════════
# Tab 2: Acoes por Material (main actionable view)
# ═══════════════════════════════════════════════════════════════════════════

with tab_acoes:
    if df is not None:
        st.subheader("O que fazer com cada material")

        # Filters
        fc1, fc2, fc3, fc4 = st.columns([1, 1, 1, 2])
        with fc1:
            resp_opts = ["Todos"] + sorted(df["Responsavel"].dropna().unique().tolist()) if "Responsavel" in df.columns else ["Todos"]
            resp_filter = st.selectbox("Responsavel", options=resp_opts, key="acoes_resp")
        with fc2:
            prio_opts = ["Todas"]
            if "Prioridade" in df.columns:
                prio_opts += sorted(df["Prioridade"].dropna().unique().tolist())
            prio_filter = st.selectbox("Prioridade", options=prio_opts, key="acoes_prio")
        with fc3:
            decisao_opts = ["Todas"]
            if "Analise_AI" in df.columns:
                decisao_opts += sorted(df["Analise_AI"].dropna().astype(str).unique().tolist())
            dec_filter = st.selectbox("Decisao", options=decisao_opts, key="acoes_dec")
        with fc4:
            search = st.text_input("Buscar (codigo/descricao)", key="acoes_search")

        # Apply filters
        df_view = df.copy()
        if resp_filter != "Todos" and "Responsavel" in df_view.columns:
            df_view = df_view[df_view["Responsavel"] == resp_filter]
        if prio_filter != "Todas" and "Prioridade" in df_view.columns:
            df_view = df_view[df_view["Prioridade"] == prio_filter]
        if dec_filter != "Todas" and "Analise_AI" in df_view.columns:
            df_view = df_view[df_view["Analise_AI"].astype(str) == dec_filter]
        if search:
            mask = (
                df_view["Codigo_Material"].astype(str).str.contains(search, case=False, na=False)
                | df_view["Texto_Breve_Material"].astype(str).str.contains(search, case=False, na=False)
            )
            df_view = df_view[mask]

        # Sort by priority
        prio_order = {"URGENTE": 0, "ALTA": 1, "MÉDIA": 2, "BAIXA": 3}
        if "Prioridade" in df_view.columns:
            df_view = df_view.copy()
            df_view["_sort"] = df_view["Prioridade"].map(prio_order).fillna(4)
            df_view = df_view.sort_values("_sort").drop(columns=["_sort"])

        st.caption(f"Mostrando {len(df_view)} materiais")

        # Material cards
        for _, row in df_view.iterrows():
            code = str(row.get("Codigo_Material", ""))
            desc = str(row.get("Texto_Breve_Material", ""))[:60]
            prio = str(row.get("Prioridade", ""))
            decisao = str(row.get("Analise_AI", ""))
            grupo = str(row.get("Grupo_MRP", ""))

            # Color-coded prefix
            prio_icon = {"URGENTE": "🔴", "ALTA": "🟠", "MÉDIA": "🟡", "BAIXA": "🟢"}.get(prio, "⚪")
            dec_icon = {"REPOR": "✅", "NAO_REPOR": "⛔", "VERIFICAR": "🔍"}.get(decisao, "❓")

            label = f"{prio_icon} **{code}** — {desc}  |  {grupo}  |  {dec_icon} {decisao}  |  {prio}"

            with st.expander(label):
                # Main action area
                acoes = row.get("acoes_sugeridas", "")
                comentario = str(row.get("Comentario", "") or "")
                acoes_fmt = _format_action_list(acoes)

                col_main, col_side = st.columns([3, 2])

                with col_main:
                    if acoes_fmt:
                        st.markdown("**Acoes para o Analista:**")
                        st.info(acoes_fmt)
                    elif comentario:
                        st.markdown(f"**Comentario IA:** {comentario}")
                    else:
                        st.markdown("*Sem acoes sugeridas*")

                    # Validation issues
                    resumo_val = str(row.get("resumo_validacao", "") or "")
                    if resumo_val:
                        st.markdown("**Problemas de Validacao:**")
                        st.warning(resumo_val[:300])

                    # JIRA
                    jira_acao = str(row.get("jira_acao_sugerida", "") or "")
                    if jira_acao:
                        st.markdown(f"**Acao JIRA:** {jira_acao}")

                    # Post-analysis
                    pos = str(row.get("pos_analise", "") or "")
                    if pos:
                        st.markdown(f"**Pos-analise:** {pos}")

                with col_side:
                    # Key numbers
                    data_pairs = [
                        ("Estoque", row.get("Estoque_Total")),
                        ("Saldo Virtual", row.get("Saldo_Virtual")),
                        ("Preco Unit.", row.get("Preco_Unitario")),
                        ("PR Calc.", row.get("PR_Calculado")),
                        ("MAX Calc.", row.get("MAX_Calculado")),
                        ("Politica", row.get("Politica_Sugerida")),
                        ("Demanda Anual", row.get("Demanda_Anual")),
                        ("Qtd Sugerida", row.get("Quantidade_OP_AI") or row.get("Quantidade_OP_Calculada")),
                        ("Classificacao", row.get("Classificacao")),
                        ("TMD / CV", f"{row.get('TMD', '-')} / {row.get('CV', '-')}"),
                    ]
                    for label, val in data_pairs:
                        if pd.notna(val) and str(val).strip() not in ("", "nan", "None"):
                            st.markdown(f"**{label}:** {val}")

                    # Reference
                    ref = str(row.get("ref_reference_found", "") or "")
                    if ref:
                        ref_price = row.get("ref_price_estimated", "")
                        ref_url = str(row.get("ref_url", "") or "")
                        st.markdown(f"**Ref. Mercado:** {ref}")
                        if ref_price:
                            st.markdown(f"**Preco Ref.:** {ref_price}")
                        if ref_url:
                            st.markdown(f"[Link Produto]({ref_url})")
    else:
        st.info("Execute o pipeline Core primeiro para ver as acoes por material.")


# ═══════════════════════════════════════════════════════════════════════════
# Tab 3: Data
# ═══════════════════════════════════════════════════════════════════════════

with tab_data:
    if df is not None:
        c1, c2, c3 = st.columns(3)
        c1.metric("Materiais", len(df))
        c2.metric("Colunas", len(df.columns))
        c3.metric("Memoria", f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")

        default_cols = [c for c in [
            "Codigo_Material", "Texto_Breve_Material", "Responsavel",
            "Prioridade", "Grupo_MRP", "Analise_AI",
            "classificacao_validacao", "Politica_Sugerida",
            "Estoque_Total", "Preco_Unitario",
        ] if c in df.columns]

        selected = st.multiselect("Colunas", options=list(df.columns), default=default_cols)
        if selected:
            st.dataframe(df[selected], use_container_width=True, height=500)
    else:
        st.info("Nenhum dado carregado.")


# ═══════════════════════════════════════════════════════════════════════════
# Tab 4: Results
# ═══════════════════════════════════════════════════════════════════════════

with tab_results:
    if df is not None:
        # Distribution charts
        chart_configs = [
            ("Prioridade", "Prioridade de Analise"),
            ("Analise_AI", "Decisao IA"),
            ("Responsavel", "Por Responsavel"),
            ("Classificacao", "Classificacao Demanda"),
            ("Politica_Sugerida", "Politica Sugerida"),
            ("Grupo_MRP", "Grupo MRP"),
            ("classificacao_validacao", "Validacao"),
        ]

        cols_row = st.columns(2)
        chart_idx = 0
        for col_name, title in chart_configs:
            if col_name in df.columns:
                with cols_row[chart_idx % 2]:
                    st.subheader(title)
                    counts = df[col_name].value_counts()
                    st.bar_chart(counts, horizontal=True)
                chart_idx += 1

        # Numeric stats
        num_cols = ["PR_Calculado", "MAX_Calculado", "Demanda_Anual", "TMD", "CV", "Preco_Unitario"]
        existing = [c for c in num_cols if c in df.columns]
        if existing:
            st.subheader("Estatisticas Numericas")
            st.dataframe(
                df[existing].describe().T[["mean", "std", "min", "max"]].round(2),
                use_container_width=True,
            )
    else:
        st.info("Execute o pipeline para ver resultados.")


# ═══════════════════════════════════════════════════════════════════════════
# Tab 5: Export Reports
# ═══════════════════════════════════════════════════════════════════════════

with tab_export:
    if df is not None:
        st.subheader("Gerar Relatorios para Analistas")
        st.markdown("""
        Gera relatorios Excel multi-abas com:
        - **Resumo** — KPIs e materiais prioritarios
        - **Acoes** — O que fazer com cada material (ordenado por prioridade)
        - **Reposicao** — Materiais prontos para requisicao
        - **Pendentes** — Materiais que precisam de analise manual
        - **Sem Reposicao** — Informativo
        """)

        col1, col2 = st.columns(2)
        with col1:
            export_resp = st.selectbox(
                "Gerar para",
                options=["Todos os analistas", "Completo (MTSE)"] +
                        sorted(df["Responsavel"].dropna().unique().tolist()) if "Responsavel" in df.columns else ["Completo"],
                key="export_resp",
            )

        with col2:
            st.markdown("&nbsp;", unsafe_allow_html=True)
            gen_btn = st.button("Gerar Relatorio", type="primary", use_container_width=True)

        if gen_btn:
            with st.spinner("Gerando relatorios..."):
                try:
                    from utils.actionable_report import generate_analyst_report, generate_all_reports
                    from config.paths import OUTPUT_FOLDER

                    if export_resp == "Todos os analistas":
                        results = generate_all_reports(df, OUTPUT_FOLDER)
                        st.success(f"Relatorios gerados para {len(results)} analistas em {OUTPUT_FOLDER}")
                        for resp, path in results.items():
                            st.markdown(f"- **{resp}**: `{path}`")
                    elif export_resp == "Completo (MTSE)":
                        path = generate_analyst_report(df, Path(OUTPUT_FOLDER) / "MTSE" / "Relatorio_Acoes.xlsx")
                        st.success(f"Relatorio gerado: {path}")
                    else:
                        safe = export_resp.replace("/", "_").strip()
                        path = generate_analyst_report(
                            df, Path(OUTPUT_FOLDER) / safe / f"Relatorio_Acoes_{safe}.xlsx",
                            responsavel=export_resp,
                        )
                        st.success(f"Relatorio gerado: {path}")
                except Exception as exc:
                    st.error(f"Erro ao gerar relatorio: {exc}")

        # Quick CSV download
        st.divider()
        st.subheader("Download Rapido")
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV Completo", csv, "automareq_export.csv", "text/csv")
    else:
        st.info("Execute o pipeline para exportar relatorios.")
