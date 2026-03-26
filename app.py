"""
app.py — Streamlit GUI for AutomaReq Pipeline
================================================

Launch with:
    streamlit run app.py
"""

from __future__ import annotations

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
    page_title="AutomaReq — Pipeline",
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
# Sidebar — Configuration
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("⚙️ Configuração")

    mode = st.radio("Modo", ["test", "production"], index=0)
    pipeline.config.mode = mode

    if mode == "test":
        pipeline.config.sample_size = st.number_input(
            "Tamanho da amostra", min_value=1, max_value=1000,
            value=pipeline.config.sample_size,
        )

    pipeline.config.responsavel_filter = st.text_input(
        "Filtrar por Responsável (vazio = todos)",
        value=pipeline.config.responsavel_filter,
    )

    pipeline.config.use_parquet_cache = st.checkbox(
        "Cache Parquet", value=pipeline.config.use_parquet_cache,
    )

    st.divider()
    st.subheader("Substages")

    all_val_stages = ["leadtime", "grpm", "texts", "obs", "reference", "images", "ref_obs"]
    pipeline.config.validation_stages = st.multiselect(
        "Validação",
        options=all_val_stages,
        default=pipeline.config.validation_stages,
    )

    all_p1_stages = ["jira_analysis", "smit", "frac"]
    pipeline.config.analysis_p1_stages = st.multiselect(
        "Análise Fase 1",
        options=all_p1_stages,
        default=pipeline.config.analysis_p1_stages,
    )

    all_p2_stages = ["zstk", "ad", "ana"]
    pipeline.config.analysis_p2_stages = st.multiselect(
        "Análise Fase 2",
        options=all_p2_stages,
        default=pipeline.config.analysis_p2_stages,
    )

    st.divider()
    pipeline.config.use_jira = st.checkbox("JIRA ativo", value=pipeline.config.use_jira)
    pipeline.config.use_search = st.checkbox("Pesquisa web ativa", value=pipeline.config.use_search)
    pipeline.config.export_debug = st.checkbox("Export DEBUG sheet", value=pipeline.config.export_debug)

    st.divider()
    if st.button("🔄 Resetar Pipeline", use_container_width=True):
        pipeline.reset()
        st.session_state.run_log = []
        st.rerun()


# ---------------------------------------------------------------------------
# Helper: run stages with spinner
# ---------------------------------------------------------------------------

def _run_stages(keys: list[str]) -> None:
    """Run pipeline stages with Streamlit spinner feedback."""
    for key in keys:
        defn = pipeline._get_defn(key)
        with st.spinner(f"Executando {defn.name}..."):
            result = pipeline.run_stage(key)

        if result.status == "done":
            st.toast(f"✅ {defn.name} concluído em {result.elapsed:.1f}s")
        elif result.status == "error":
            st.toast(f"❌ {defn.name}: {result.error[:50]}", icon="🚨")
            break

    st.rerun()


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("📋 AutomaReq — Pipeline de Automação de Requisições")
st.caption("Itaipu Binacional — Gestão de Materiais")

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_pipeline, tab_data, tab_results, tab_actions, tab_dashboard = st.tabs([
    "🔧 Pipeline", "📊 Dados", "📈 Resultados", "✅ Ações Sugeridas", "🌐 Dashboard",
])


# ═══════════════════════════════════════════════════════════════════════════
# Tab 1: Pipeline
# ═══════════════════════════════════════════════════════════════════════════

with tab_pipeline:
    st.subheader("Status do Pipeline")

    # Status table
    status_data = []
    for i, stage in enumerate(pipeline.stages, 1):
        r = pipeline.results[stage.key]
        status_emoji = {
            "pending": "⏳", "running": "🔄", "done": "✅",
            "error": "❌", "skipped": "⏭️",
        }
        status_data.append({
            "#": i,
            "Estágio": stage.name,
            "Grupo": stage.group,
            "Status": f"{status_emoji.get(r.status, '?')} {r.status.upper()}",
            "Tempo": f"{r.elapsed:.1f}s" if r.elapsed > 0 else "—",
            "Opcional": "✓" if stage.optional else "",
        })

    st.dataframe(
        pd.DataFrame(status_data),
        use_container_width=True,
        hide_index=True,
        height=450,
    )

    # Run controls
    st.divider()
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        if st.button("▶️ Core", use_container_width=True, help="Load → Filter → Validate → Calculate → Summary"):
            _run_stages(["load", "filter", "validate", "calculate", "summary"])

    with col2:
        if st.button("▶️ Análise", use_container_width=True, help="Fase 1 + Fase 2"):
            _run_stages(["analysis_p1", "analysis_p2"])

    with col3:
        if st.button("▶️ Dashboard", use_container_width=True, help="Gera dashboard HTML"):
            _run_stages(["dashboard"])

    with col4:
        if st.button("▶️ Separação", use_container_width=True, help="Separa por grupos"):
            _run_stages(["separacao"])

    with col5:
        if st.button("▶️ Envio", use_container_width=True, help="Templates + email"):
            _run_stages(["emission"])

    st.divider()

    # Individual stage buttons
    st.caption("Executar estágio individual:")
    cols = st.columns(4)
    for i, stage in enumerate(pipeline.stages):
        col = cols[i % 4]
        with col:
            if st.button(f"{stage.name}", key=f"run_{stage.key}", use_container_width=True):
                _run_stages([stage.key])

    # Error details
    for stage in pipeline.stages:
        r = pipeline.results[stage.key]
        if r.status == "error" and r.error:
            st.error(f"**{stage.name}**: {r.error}")


# ═══════════════════════════════════════════════════════════════════════════
# Tab 2: Data
# ═══════════════════════════════════════════════════════════════════════════

with tab_data:
    if pipeline.df is not None:
        df = pipeline.df
        col1, col2, col3 = st.columns(3)
        col1.metric("Materiais", len(df))
        col2.metric("Colunas", len(df.columns))
        col3.metric("Memória", f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")

        # Column filter
        all_cols = list(df.columns)
        default_cols = [c for c in [
            "Codigo_Material", "Texto_Breve_Material", "Responsavel",
            "Grupo_MRP", "classificacao_validacao", "Politica_Sugerida",
            "Analise_AI", "acoes_sugeridas",
        ] if c in all_cols]

        selected_cols = st.multiselect(
            "Colunas visíveis",
            options=all_cols,
            default=default_cols or all_cols[:10],
        )

        if selected_cols:
            st.dataframe(df[selected_cols], use_container_width=True, height=500)
        else:
            st.dataframe(df, use_container_width=True, height=500)

        # Download
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Download CSV", csv, "automareq_export.csv", "text/csv")
    else:
        st.info("Nenhum dado carregado. Execute o estágio **Carregar Dados** primeiro.")


# ═══════════════════════════════════════════════════════════════════════════
# Tab 3: Results
# ═══════════════════════════════════════════════════════════════════════════

with tab_results:
    if pipeline.df is not None:
        df = pipeline.df

        # Distribution charts
        dist_cols = [
            ("Responsavel", "Distribuição por Responsável"),
            ("Classificacao", "Classificação de Demanda"),
            ("Politica_Sugerida", "Política Sugerida"),
            ("Grupo_MRP", "Grupo MRP"),
            ("classificacao_validacao", "Validação"),
        ]

        cols_row = st.columns(2)
        for i, (col, title) in enumerate(dist_cols):
            if col in df.columns:
                with cols_row[i % 2]:
                    counts = df[col].value_counts()
                    st.subheader(title)
                    st.bar_chart(counts, horizontal=True)

        # Numeric stats
        num_cols = ["PR_Calculado", "MAX_Calculado", "Demanda_Anual", "TMD", "CV", "Preco_Unitario"]
        existing = [c for c in num_cols if c in df.columns]
        if existing:
            st.subheader("Estatísticas Numéricas")
            st.dataframe(
                df[existing].describe().T[["mean", "std", "min", "max"]].round(2),
                use_container_width=True,
            )

        # JIRA stats
        if "jira_tickets_count" in df.columns:
            st.subheader("JIRA Analysis")
            col1, col2 = st.columns(2)
            with_tickets = (pd.to_numeric(df["jira_tickets_count"], errors="coerce").fillna(0) > 0).sum()
            col1.metric("Materiais com tickets JIRA", int(with_tickets))
            col2.metric("Sem tickets JIRA", len(df) - int(with_tickets))
    else:
        st.info("Execute o pipeline para ver resultados.")


# ═══════════════════════════════════════════════════════════════════════════
# Tab 4: Suggested Actions
# ═══════════════════════════════════════════════════════════════════════════

with tab_actions:
    if pipeline.df is not None:
        df = pipeline.df

        st.subheader("Ações Sugeridas por Material")

        # Filter
        col1, col2 = st.columns([1, 1])
        with col1:
            resp_filter = st.selectbox(
                "Filtrar por Responsável",
                options=["Todos"] + sorted(df["Responsavel"].dropna().unique().tolist())
                if "Responsavel" in df.columns else ["Todos"],
            )
        with col2:
            search = st.text_input("Buscar material", placeholder="Código ou descrição...")

        df_view = df.copy()
        if resp_filter != "Todos" and "Responsavel" in df_view.columns:
            df_view = df_view[df_view["Responsavel"] == resp_filter]
        if search:
            mask = (
                df_view["Codigo_Material"].astype(str).str.contains(search, case=False, na=False)
                | df_view["Texto_Breve_Material"].astype(str).str.contains(search, case=False, na=False)
            )
            df_view = df_view[mask]

        # Display per-material cards
        for _, row in df_view.iterrows():
            code = row.get("Codigo_Material", "?")
            desc = row.get("Texto_Breve_Material", "")
            grupo = row.get("Grupo_MRP", "")
            validacao = row.get("classificacao_validacao", "")
            analise = row.get("Analise_AI", "")
            acoes = row.get("acoes_sugeridas", "")
            jira_resumo = row.get("jira_historico_resumo", "")
            jira_acao = row.get("jira_acao_sugerida", "")
            comentario = row.get("Comentario", "")

            # Status color
            if validacao and "REVISAR" in str(validacao).upper():
                status_icon = "⚠️"
            else:
                status_icon = "✅"

            with st.expander(f"{status_icon} **{code}** — {desc[:60]}  |  {grupo}  |  {analise}"):
                col1, col2 = st.columns([2, 1])

                with col1:
                    if comentario:
                        st.markdown(f"**Comentário IA:** {comentario}")
                    if acoes:
                        st.markdown("**Ações Sugeridas:**")
                        if isinstance(acoes, list):
                            for a in acoes:
                                st.markdown(f"- {a}")
                        else:
                            st.markdown(str(acoes))

                with col2:
                    if jira_resumo:
                        st.markdown(f"**JIRA:** {jira_resumo}")
                    if jira_acao:
                        st.markdown(f"**Ação JIRA:** {jira_acao}")

                # Key data
                data_cols = [
                    "Estoque_Total", "Saldo_Virtual", "Preco_Unitario",
                    "PR_Calculado", "MAX_Calculado", "Politica_Sugerida",
                    "Consumo_Medio_Mensal", "Demanda_Anual",
                ]
                existing = {c: row.get(c) for c in data_cols if pd.notna(row.get(c))}
                if existing:
                    st.dataframe(
                        pd.DataFrame([existing]),
                        use_container_width=True,
                        hide_index=True,
                    )
    else:
        st.info("Execute o pipeline para ver ações sugeridas.")


# ═══════════════════════════════════════════════════════════════════════════
# Tab 5: Dashboard
# ═══════════════════════════════════════════════════════════════════════════

with tab_dashboard:
    from config.paths import OUTPUT_FOLDER

    dashboard_path = Path(OUTPUT_FOLDER) / "MTSE" / "dashboard.html"
    if dashboard_path.exists():
        st.subheader("Dashboard HTML (gerado)")
        html_content = dashboard_path.read_text(encoding="utf-8")
        # Check for dashboard_data.js
        data_js_path = Path(OUTPUT_FOLDER) / "MTSE" / "dashboard_data.js"
        if data_js_path.exists():
            js_content = data_js_path.read_text(encoding="utf-8")
            # Inject JS into HTML
            html_content = html_content.replace(
                '<script src="dashboard_data.js"></script>',
                f'<script>{js_content}</script>',
            )
        st.components.v1.html(html_content, height=800, scrolling=True)
    else:
        st.info("Execute o estágio **Dashboard** para gerar o painel HTML.")
        if pipeline.df is not None and st.button("🔧 Gerar Dashboard agora"):
            _run_stages(["dashboard"])
