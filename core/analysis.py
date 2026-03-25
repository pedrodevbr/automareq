"""
analysis.py — Stock analysis pipeline.

Contains:
  - StockAnalyzerPipeline   (6-step pipeline: init → rules → AI → search → Jira read → Jira create)
  - run_analysis(df)        (Entry point)
"""

from __future__ import annotations

import logging
import time

import pandas as pd
from tqdm import tqdm

from config.ai import ai_model_analysis
from config.business import ALTO_VALOR, ALTO_VOLUME, ANOS_SEM_OC
from services.ai_service import AIModule
from services.jira_service import JiraModule
from services.sap_service import SapManager
from services.search_service_leg import SearchModule
from utils.export_module import export_by_responsavel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Flow display helpers
# ---------------------------------------------------------------------------

def _step_header(step_num: int, title: str, description: str = "") -> None:
    bar = "─" * 56
    print(f"\n┌{bar}┐")
    print(f"│  [{step_num}] {title:<50}│")
    if description:
        print(f"│      {description:<50}│")
    print(f"└{bar}┘")

class StockAnalyzerPipeline:
    def __init__(self, ai_model_analysis_name=ai_model_analysis, use_jira=True, use_search=True):
        self.ai = AIModule(model_name=ai_model_analysis_name)
        self.jira = JiraModule() if use_jira else None
        self.sap = SapManager()
        # Inicializa Search apenas se necessário para economizar recursos/setup
        self.search = SearchModule(model_name="google/gemini-3.1-flash-lite-preview:online") if use_search else None
        self.use_search = use_search

    # --- HELPERS ---

    def _save_checkpoint(self, df: pd.DataFrame, step_name: str) -> None:
        """Exports current state per responsible analyst as an Excel checkpoint."""
        export_by_responsavel(df, filename=f"0{step_name}")

    # --- REGRAS DE NEGÓCIO (BUSINESS LOGIC) ---

    def _rule_zstk(self, row: pd.Series) -> pd.Series:
        """Lógica detalhada para itens ZSTK."""
        log = []
        try:
            val = lambda k: float(row.get(k, 0) or 0)
            
            # Extração de valores
            v_total = val('Valor_Total_Ordem')
            vol_op = val('Volume_OP')
            virt = val('Saldo_Virtual')
            rtp3 = val('RTP3')
            rtp6 = val('RTP6')
            pr = val('PR_Atual')
            max_at = val('MAX_Atual')
            op = val('Quantidade_Ordem')
            anos_oc = float(row.get('Anos_Ultima_Compra', -1) if pd.notna(row.get('Anos_Ultima_Compra')) else -1)
            
            # Flags de ação
            needs_mkt = False
            
            # Verificações
            if str(row.get('Classificacao')).strip().upper() == 'CONSUMO ZERO':
                log.append('CONSUMO ZERO')
                return self._update_row(row, log, ai_decisao='NAO_REPOR')

            if v_total > ALTO_VALOR: log.append('Verificar demanda (alto valor)')
            if vol_op > ALTO_VOLUME: log.append('Verificar entrega parcelada')
            if virt + rtp3 > pr: log.append('Saldo Virtual + RTP3 > PR')
            if virt + op > max_at: row['pos_analise'] = 'Incluir (DEMPRO) na requisição'
            
            # Pesquisa de Mercado
            if anos_oc < 0:
                log.append('Nunca comprou')
                needs_mkt = True
            elif anos_oc > ANOS_SEM_OC:
                log.append(f'Preço desatualizado (anos sem compra: {anos_oc})')
                needs_mkt = True
            
            if needs_mkt: row['needs_market_search'] = True
            
            # Decisão IA
            if row.get('Demanda_Programada') == True or rtp6 > 0:
                row['needs_ai'] = True
            else:
                row['needs_ai'] = False
                row['Analise_AI'] = 'REPOR'

        except Exception as e:
            log.append(f"Erro regra ZSTK: {e}")
        
        return self._update_row(row, log)

    def _rule_frac(self, row: pd.Series) -> pd.Series:
        log = []
        try:
            row['needs_market_search'] = True
            qtde_lmr = float(row.get('Quantidade_LMR', 0) or 0)
            
            if qtde_lmr > 0:
                texto = (f"Prezados,\nA licitação do código {row.get('Codigo_Material')} resultou deserta.\n"
                         f"Referência: {row.get('Numero_Peca_Fabricante')}\n"
                         f"Data da ultima compra: {row['Data_Ultimo_Pedido'].strftime("%d/%m/%Y") or "Nunca comprou"}\n"
                         f"Favor indicar uma referencia substituta\n")
                row['FRAC_texto'] = texto
                log.append("Sugerido abrir JIRA FRAC")
                row['sugestao_jira_frac'] = True
                row['Analise_AI'] = 'NAO_REPOR'
            else:
                log.append("Encontrar outra referência")
        except Exception as e:
            log.append(f"Erro FRAC: {e}")
            
        return self._update_row(row, log)

    def _update_row(self, row, logs, ai_decisao=None):
        """Helper para atualizar logs e decisão."""
        if logs:
            curr = str(row.get('pre_analise', ''))
            sep = ' | ' if curr else ''
            row['pre_analise'] = curr + sep + ' | '.join(logs)
        if ai_decisao:
            row['Analise_AI'] = ai_decisao
        return row

    # --- STEPS DO PIPELINE ---

    def step_1_initialization(self, df: pd.DataFrame) -> pd.DataFrame:
        _step_header(1, "Inicialização", f"{len(df)} materiais carregados")
        for col in ['needs_ai', 'needs_market_search', 'needs_jira_search', 'sugestao_jira_frac']:
            if col not in df.columns:
                df[col] = False
        self._save_checkpoint(df, "1_Init")
        return df

    def step_2_apply_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        _step_header(2, "Regras de Negócio", "ZSTK / FRAC / AD / SMIT / ANA")
        
        def router(row):
            grp = str(row.get('Grupo_MRP', '')).upper()
            if grp == 'FRAC':
                return self._rule_frac(row)
            elif grp == 'AD':
                row['Analise_AI'] = 'REPOR'
                row['AD_texto'] = (
                    f"Prezado Fornecedor,\n\n"
                    f"Favor cotar {row.get('Quantidade_Ordem', 0)} unidades do material "
                    f"{row.get('Texto_Breve_Material', 'N/A')}:\n"
                    f"- A Itaipu Binacional é isenta de impostos.\n"
                    f"- Frete CIF.\n\n"
                    f"Descrição:\n{row.get('Texto_PT', 'N/A')}\n\n"
                    f"Referência:\n{row.get('Numero_Peca_Fabricante', 'N/A')}\n\n"
                    f"Observações:\n{row.get('Texto_Observacao_PT', 'N/A')}\n"
                )
                return row
            elif grp == 'SMIT':
                row['needs_jira_search'] = True
                row['SMIT_texto'] = ''
                row['Analise_AI'] = 'NAO_REPOR'
                return row
            elif grp == 'ANA':
                row['pre_analise'] = str(row.get('pre_analise', '')) + ' | ANALISE MANUAL'
                row['Analise_AI'] = 'NAO_REPOR'
                return row
            else:
                return self._rule_zstk(row)

        df = df.apply(router, axis=1)

        grp_counts = df.groupby('Grupo_MRP').size().to_dict() if 'Grupo_MRP' in df.columns else {}
        needs_ai   = int((df['needs_ai'] == True).sum())
        needs_mkt  = int((df['needs_market_search'] == True).sum())
        print(f"   Grupos: {grp_counts}")
        print(f"   Necessitam IA: {needs_ai} | Pesquisa de mercado: {needs_mkt}")

        self._save_checkpoint(df, "2_Rules")
        return df

    def step_3_ai_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = df['needs_ai'] == True
        _step_header(3, "Análise IA", f"{mask.sum()} itens aguardam decisão do modelo")
        if not mask.any():
            print("   Nenhum item requer análise de IA.")
            return df

        t0    = time.time()
        df_ai = self.ai.analyze_batch(df.loc[mask].copy(), max_workers=3)
        df.update(df_ai)

        decisions = df.loc[mask, 'Analise_AI'].value_counts().to_dict() if 'Analise_AI' in df.columns else {}
        print(f"   Concluído em {time.time()-t0:.1f}s | Decisões: {decisions}")
        self._save_checkpoint(df, "3_AI")
        return df

    def step_4_market_search(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.use_search:
            return df

        mask = df['needs_market_search'] == True
        _step_header(4, "Pesquisa de Mercado", f"{mask.sum()} itens para pesquisa web")
        if not mask.any():
            print("   Nenhum item requer pesquisa de mercado.")
            return df

        cols_search = ["produto_identificado", "preco_unitario_estimado", "moeda", "url_fonte",
                       "disponibilidade", "analise_confianca", "fornecedor_principal"]
        for col in cols_search:
            if col not in df.columns:
                df[col] = None

        df_search = self.search.run_search_batch(df.loc[mask].copy(), max_workers=3)
        if not df_search.empty:
            df.update(df_search[cols_search])

        self._save_checkpoint(df, "4_Search")
        return df

    def step_5_jira_enrichment(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.jira:
            return df
        mask = df['needs_jira_search'] == True
        _step_header(5, "Jira — Leitura", f"{mask.sum()} itens SMIT para consultar")
        if not mask.any():
            return df

        for idx, row in tqdm(df[mask].iterrows(), total=mask.sum(), desc="Jira Read"):
            try:
                comments, key = self.jira.find_last_comment(row['Codigo_Material'])
                if comments:
                    last = comments[-1]['body']
                    df.at[idx, 'SMIT_texto']  = f"{str(df.at[idx,'SMIT_texto'])}\nMsg ({key}): {last}"
                    df.at[idx, 'pre_analise'] = f"{str(df.at[idx,'pre_analise'])} | Ticket: {key}"
            except Exception as e:
                logger.error("Jira Read Error %s: %s", row['Codigo_Material'], e)

        self._save_checkpoint(df, "5_Jira_Read")
        return df

    def step_6_jira_creation(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.jira:
            return df
        mask = df['sugestao_jira_frac'] == True
        _step_header(6, "Jira — Criação FRAC", f"{mask.sum()} tickets a criar")
        if not mask.any():
            return df

        for idx, row in df[mask].iterrows():
            try:
                if self.jira.verificar_consultas_abertas(row['Codigo_Material']):
                    continue
                key = self.jira.create_frac_ticket(
                    code=row['Codigo_Material'],
                    short_text=row['Texto_Breve_Material'],
                    text=row['FRAC_texto'],
                    saldo_virtual=row['Saldo_Virtual'],
                )
                df.at[idx, 'pre_analise'] = f"{str(df.at[idx, 'pre_analise'])} | FRAC criado: {key}"
                try:
                    self.sap.change_tipo_mrp(row['Codigo_Material'], "SMIT")
                except Exception as e:
                    logger.error("SAP MRP change error %s: %s", row['Codigo_Material'], e)
            except Exception as e:
                logger.error("Jira Create Error %s: %s", row['Codigo_Material'], e)

        self._save_checkpoint(df, "6_Jira_Create")
        return df

    # --- EXECUÇÃO ---

    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        bar = "═" * 58
        n   = len(df)
        print(f"\n╔{bar}╗")
        print(f"║  PIPELINE DE ANÁLISE — {n} materiais{' ' * max(0, 27 - len(str(n)))}║")
        print(f"║  Modelo: {self.ai.model_name:<46}║")
        print(f"║  Jira: {'ativo' if self.jira else 'desativado':<49}║")
        print(f"║  Pesquisa web: {'ativa' if self.use_search else 'desativada':<42}║")
        print(f"╚{bar}╝")

        t_total = time.time()

        df = self.step_1_initialization(df)
        df = self.step_2_apply_rules(df)
        df = self.step_3_ai_analysis(df)
        df = self.step_4_market_search(df)
        df = self.step_5_jira_enrichment(df)
        df = self.step_6_jira_creation(df)

        self._save_checkpoint(df, "_Final")

        repor     = int((df.get('Analise_AI', pd.Series()) == 'REPOR').sum())
        nao_repor = int((df.get('Analise_AI', pd.Series()) == 'NAO_REPOR').sum())
        outros    = n - repor - nao_repor

        print(f"\n{'═' * 58}")
        print(f"  ANÁLISE CONCLUÍDA em {time.time()-t_total:.1f}s")
        print(f"  REPOR     : {repor:>4d}")
        print(f"  NAO_REPOR : {nao_repor:>4d}")
        print(f"  OUTROS    : {outros:>4d}  (VERIFICAR / ERRO_API / etc.)")
        print(f"{'═' * 58}\n")
        return df


def run_analysis(df: pd.DataFrame, use_jira: bool = True, use_search: bool = True) -> pd.DataFrame:
    """Entry point: creates the StockAnalyzerPipeline and processes the DataFrame."""
    pipeline = StockAnalyzerPipeline(
        ai_model_analysis_name=ai_model_analysis,
        use_jira=use_jira,
        use_search=use_search,
    )
    return pipeline.process_dataframe(df)