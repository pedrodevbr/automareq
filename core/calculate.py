"""
calculate.py — Stock parameter calculations.

Contains:
  - detect_outliers_row     (IQR-based outlier detection)
  - calculate_pr_row        (Reorder Point from cleaned consumption)
  - decision_tree_row       (Policy decision: ZP/ZL/ZM/ZE/ZO/ZD)
  - run_calculations(df)    (Main entry point)
"""

from __future__ import annotations

import logging
from typing import List

import numpy as np
import pandas as pd

from config.business import (
    ALTO_VOLUME,
    ANOS_SEM_OC,
    CUSTO_FIXO_PEDIDO,
    CV_THRESHOLD,
    DEMAND_WINDOW,
    TAXA_MANUTENCAO,
    TMD_THRESHOLD,
    VALOR_UN_ALTO,
)

logger = logging.getLogger(__name__)


def detect_outliers_row(row_values: np.ndarray) -> List[float]:
    """
    Detecta outliers em um array de valores positivos usando IQR.
    Otimizado para receber um numpy array direto.
    """
    try:
        # Filtra apenas positivos
        consumo_pos = row_values[row_values > 0]
        
        if consumo_pos.size == 0:
            return [] # Retorna lista vazia se não houver consumo positivo
            
        # Ordena decrescente (comportamento original mantido, embora IQR não exija ordem específica)
        consumo_pos = np.sort(consumo_pos)[::-1]
        
        q1 = np.percentile(consumo_pos, 25)
        q3 = np.percentile(consumo_pos, 75)
        iqr = q3 - q1
        
        if iqr == 0:
            return []
            
        threshold = q3 + 1.5 * iqr
        outliers = consumo_pos[consumo_pos > threshold]
        
        return outliers.tolist()
    except Exception:
        return []

def calculate_pr_row(row_values: np.ndarray) -> float:
    """Calcula Ponto de Reabastecimento (PR) baseado nos valores históricos limpos de outliers."""
    try:
        consumo_pos = row_values[row_values > 0]
        if consumo_pos.size == 0:
            return 1.0
            
        # Ordena decrescente
        consumo_pos = np.sort(consumo_pos)[::-1]
        
        # Recalcula outliers internamente para consistência ou usa lógica simplificada
        q1 = np.percentile(consumo_pos, 25)
        q3 = np.percentile(consumo_pos, 75)
        iqr = q3 - q1
        
        if iqr > 0:
            threshold = q3 + 1.5 * iqr
            consumo_filtered = consumo_pos[consumo_pos <= threshold]
        else:
            consumo_filtered = consumo_pos

        if consumo_filtered.size == 0:
            return 1.0
        elif consumo_filtered.size == 1:
            return round(float(consumo_filtered[0]), 1)
        else:
            # Pega o segundo maior valor (comportamento original iloc[1])
            return round(float(consumo_filtered[1]), 1)
    except Exception:
        return 1.0

def decision_tree_row(row: pd.Series) -> str:
    """
    Lógica de decisão de política. Mantida como row-wise devido à complexidade condicional.
    """
    try:
        # Extração segura com defaults tipados
        cls = row.get('Classificacao', 'Indefinido')
        abc = row.get('Classificacao_ABC', 'C')
        adicional = row.get('Adicional_Lote_Obrigatorio', '')
        grupo = row.get('Grupo_MRP', '')
        planejador = str(row.get('Planejador_MRP', ''))
        
        # Valores numéricos já devem estar tratados no passo anterior (fillna(0))
        volume_ordem = row.get('Volume_Ordem_Planejada', 0)
        preco = row.get('Preco_Unitario', 0)
        criticidade = row.get('Criticidade', 0)
        quantidade_lmr = row.get('Quantidade_LMR', 0)
        tmd = row.get('TMD', 0)
        cv = row.get('CV', 0)

        politica = 'ZM' # Default

        if cls == 'Suave':
            if abc in ['A', 'B']:
                if adicional == 'X':
                    politica = 'ZL'
                else:
                    politica = 'ZL' if volume_ordem > ALTO_VOLUME else 'ZP'
            else:
                politica = 'ZO' if grupo not in ['ZSTK', 'SMIT', 'AD', 'ANA', 'FRAC'] else 'ZP'
        
        elif cls == 'Intermitente':
            politica = 'ZM'
            
        elif cls in ['Erratico', 'Errático']:
            politica = 'ZE' if preco > VALOR_UN_ALTO else 'ZM'
            
        elif cls in ['Esporádico', 'Esporadico']:
            if (criticidade > 0) or ('S' in planejador) or (quantidade_lmr > 0):
                politica = 'ZE' if preco > VALOR_UN_ALTO else 'ZM'
            elif (tmd > 6) and (cv > 3):
                politica = 'ZD'
            else:
                politica = 'ZM'
                
        return politica
    except Exception:
        return 'ZM'

def _calculate_priority(df: pd.DataFrame) -> pd.Series:
    """
    Calcula prioridade de análise para cada material usando operações vetorizadas.

    Retorna uma Series com valores: URGENTE, ALTA, MÉDIA, BAIXA.
    """
    # Identificar colunas LTD disponíveis
    ltd_cols = [col for col in df.columns if "LTD_" in col]

    # --- Máscaras auxiliares ---

    # Estoque zerado ou negativo
    estoque_col = 'Saldo_Virtual' if 'Saldo_Virtual' in df.columns else 'Estoque_Total'
    estoque = pd.to_numeric(df.get(estoque_col, 0), errors='coerce').fillna(0)
    estoque_zerado = estoque <= 0

    # Consumo recente: qualquer LTD > 0 nos últimos 3 períodos
    if len(ltd_cols) >= 3:
        consumo_recente_3 = (
            df[ltd_cols[:3]].apply(pd.to_numeric, errors='coerce').fillna(0) > 0
        ).any(axis=1)
    elif ltd_cols:
        consumo_recente_3 = (
            df[ltd_cols].apply(pd.to_numeric, errors='coerce').fillna(0) > 0
        ).any(axis=1)
    else:
        consumo_recente_3 = pd.Series(False, index=df.index)

    # Consumo nos últimos 6 LTDs
    if len(ltd_cols) >= 6:
        consumo_recente_6 = (
            df[ltd_cols[:6]].apply(pd.to_numeric, errors='coerce').fillna(0) > 0
        ).any(axis=1)
    elif ltd_cols:
        consumo_recente_6 = (
            df[ltd_cols].apply(pd.to_numeric, errors='coerce').fillna(0) > 0
        ).any(axis=1)
    else:
        consumo_recente_6 = pd.Series(False, index=df.index)

    # Helper to safely get a numeric column (returns Series of zeros when missing)
    def _safe_numeric(col_name: str) -> pd.Series:
        if col_name in df.columns:
            return pd.to_numeric(df[col_name], errors='coerce').fillna(0)
        return pd.Series(0, index=df.index, dtype=float)

    # Criticidade
    criticidade = _safe_numeric('Criticidade')

    # PR_Calculado
    pr_calc = _safe_numeric('PR_Calculado')

    # Estoque abaixo do PR
    estoque_abaixo_pr = estoque < pr_calc

    # Preço desatualizado
    anos_ult_compra = _safe_numeric('Anos_Ultima_Compra')
    preco_desatualizado = anos_ult_compra > 2

    # Valor alto da ordem
    valor_total_ordem = _safe_numeric('Valor_Total_Ordem')
    valor_alto = valor_total_ordem > 10000

    # Demanda anual positiva
    demanda_anual = _safe_numeric('Demanda_Anual')
    tem_demanda = demanda_anual > 0

    # Validação com REVISAR
    if 'classificacao_validacao' in df.columns:
        class_val = df['classificacao_validacao'].astype(str).fillna('')
    else:
        class_val = pd.Series('', index=df.index)
    tem_revisar = class_val.str.contains('REVISAR', case=False, na=False)

    # --- Regras de prioridade (ordem importa: mais restritiva primeiro) ---

    # URGENTE: estoque zerado com consumo recente OU criticidade == 1
    mask_urgente = (estoque_zerado & consumo_recente_3) | (criticidade == 1)

    # ALTA: abaixo do PR com consumo em 6 períodos, OU preço desatualizado com valor alto
    mask_alta = (estoque_abaixo_pr & consumo_recente_6) | (preco_desatualizado & valor_alto)

    # MÉDIA: tem demanda mas estoque cobre curto prazo, OU tem REVISAR
    mask_media = (tem_demanda & ~estoque_zerado & ~estoque_abaixo_pr) | tem_revisar

    # Aplicação com np.select (primeira condição verdadeira vence)
    priority = np.select(
        [mask_urgente, mask_alta, mask_media],
        ['URGENTE', 'ALTA', 'MÉDIA'],
        default='BAIXA',
    )

    return pd.Series(priority, index=df.index)


def run_calculations(df_input: pd.DataFrame) -> pd.DataFrame:
    df = df_input.copy()
    
    # ---------------------------------------------------------
    # 1. PREPARAÇÃO DOS DADOS (Conversão em massa)
    # ---------------------------------------------------------
    cols_to_numeric = [
        'Preco_Unitario', 'Criticidade', 'Quantidade_LMR', 'Volume_Ordem_Planejada',
        'Prazo_Entrega_Previsto', 'Valor_Total_Ordem', 'PR_Calculado', 
        'Volume', 'Quantidade_Ordem'
    ]
    for col in cols_to_numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Identificar colunas LTD
    ltd_cols = [col for col in df.columns if "LTD_" in col][:DEMAND_WINDOW]
    # Cria subset numérico apenas das colunas LTD para cálculos vetorizados
    df_ltd = df[ltd_cols].apply(pd.to_numeric, errors='coerce').fillna(0)

    # ---------------------------------------------------------
    # 2. CÁLCULOS ESTATÍSTICOS VETORIZADOS (TMD, CV)
    # ---------------------------------------------------------
    # TMD: Total Periods / Non-Zero Periods
    count_total = df_ltd.shape[1] # Janela fixa ou df_ltd.count(axis=1) se NaN fosse possível
    count_nonzero = (df_ltd != 0).sum(axis=1)
    
    df['TMD'] = np.where(
        (count_nonzero != 0) & (count_total > 1),
        round(count_total / count_nonzero, 2),
        -1
    )

    # CV: Std Dev / Mean
    std_val = df_ltd.std(axis=1, ddof=0)
    mean_val = df_ltd.mean(axis=1)
    
    df['CV'] = np.where(
        (mean_val != 0) & (count_total > 1),
        round(std_val / mean_val, 2),
        -1
    )

    # ---------------------------------------------------------
    # 3. CLASSIFICAÇÃO VETORIZADA
    # ---------------------------------------------------------
    conditions = [
        (df['TMD'] == -1) | (df['CV'] == -1),
        (df['CV'] > CV_THRESHOLD) & (df['TMD'] > TMD_THRESHOLD),
        (df['CV'] > CV_THRESHOLD) & (df['TMD'] < TMD_THRESHOLD),
        (df['CV'] < CV_THRESHOLD) & (df['TMD'] > TMD_THRESHOLD),
        (df['CV'] < CV_THRESHOLD) & (df['TMD'] < TMD_THRESHOLD)
    ]
    choices = ['Sem classificação', 'Esporádico', 'Intermitente', 'Errático', 'Suave']
    df['Classificacao'] = np.select(conditions, choices, default='Outro')

    # ---------------------------------------------------------
    # 4. OUTLIERS E PR (Row-wise otimizado)
    # ---------------------------------------------------------
    # Convertendo para numpy array para iteração mais rápida
    ltd_values = df_ltd.values
    
    # List comprehension costuma ser mais rápido que df.apply para operações complexas de lista
    df['Outliers'] = [detect_outliers_row(row) for row in ltd_values]
    df['PR_Calculado'] = [calculate_pr_row(row) for row in ltd_values]

    # ---------------------------------------------------------
    # 5. DEMANDA ANUAL (Vetorizado)
    # ---------------------------------------------------------
    lt_days = df['Prazo_Entrega_Previsto'].astype(int)
    # Regra: LT deve ser múltiplo de 30 e > 0
    valid_lt = (lt_days > 0) & (lt_days % 30 == 0)
    
    # Periods = DEMAND_WINDOW * 360 / lt
    periods = (DEMAND_WINDOW * 360 / lt_days.replace(0, 1)).astype(int).clip(lower=1)
    
    # Como o slice varia por linha, precisamos de uma abordagem híbrida ou aproximação.
    # Para manter a lógica exata de "slice_len", usaremos apply apenas na soma se necessário,
    # mas uma aproximação segura é calcular a média total se periods >= DEMAND_WINDOW.
    
    def get_demand_slice(idx, p_val):
        # Helper interno para slicing variável
        row_vals = df_ltd.iloc[idx].values
        slice_len = min(p_val, len(row_vals))
        if slice_len <= 0: return 0
        return row_vals[:slice_len].sum()

    # Se performance for crítica aqui, pode-se otimizar mais, mas isso preserva a lógica exata original
    sums = [get_demand_slice(i, p) for i, p in enumerate(periods)]
    
    df['Demanda_Anual'] = np.where(
        valid_lt,
        np.ceil(np.array(sums) / DEMAND_WINDOW),
        0
    )

    # ---------------------------------------------------------
    # 6. POLÍTICA SUGERIDA (Decision Tree)
    # ---------------------------------------------------------
    # Como a árvore é complexa, usamos apply, mas garantimos que os inputs estão limpos
    df['Politica_Sugerida'] = df.apply(decision_tree_row, axis=1)

    # ---------------------------------------------------------
    # 7. VALOR ATUALIZADO E DATAS (Vetorizado)
    # ---------------------------------------------------------
    now = pd.Timestamp.now()
    
    # Conversão segura de datas
    df['Data_Ultimo_Pedido'] = pd.to_datetime(df['Data_Ultimo_Pedido'], errors='coerce')
    df['Data_Abertura'] = pd.to_datetime(df['Data_Abertura'], errors='coerce')
    
    # Anos Sem OC Check
    days_since_pedido = (now - df['Data_Ultimo_Pedido']).dt.days
    df['Valor_Atualizado'] = ~ (days_since_pedido > (ANOS_SEM_OC * 360))
    df['Valor_Atualizado'] = df['Valor_Atualizado'].fillna(False) # Se Data for NaT, considera antigo? Original logic says 'True' for exceptions, let's match logic.
    # Correção lógica original: se last is None return False.
    df.loc[df['Data_Ultimo_Pedido'].isna(), 'Valor_Atualizado'] = False

    # Dias em OP
    df['Dias_Em_OP'] = (now - df['Data_Abertura']).dt.days.fillna(-1).clip(lower=0)
    
    # Anos Ultima Compra
    df['Anos_Ultima_Compra'] = round(days_since_pedido / 365.25, 1).fillna(-1)

    # ---------------------------------------------------------
    # 8. CÁLCULOS FINAIS (Max via Lote Econômico de Compra - LEC)
    # ---------------------------------------------------------
    
    # --- PREPARAÇÃO DAS VARIÁVEIS ---
    demanda = df['Demanda_Anual'].fillna(0)
    pr = df['PR_Calculado'].fillna(0)
    # Garante que preço não seja zero para não dividir por zero na fórmula
    preco = df['Preco_Unitario'].fillna(0).clip(lower=0.01) 
    
    # --- CÁLCULO DO CUSTO DE MANUTENÇÃO (H) ---
    # H = Preço Unitário * Taxa de Manutenção Anual
    custo_manutencao = preco * TAXA_MANUTENCAO
    
    # --- FÓRMULA DO LEC (EOQ) ---
    # LEC = Sqrt( (2 * Demanda * Custo_Pedido) / Custo_Manutencao )
    numerador = 2 * demanda * CUSTO_FIXO_PEDIDO
    
    # np.sqrt aplica a raiz quadrada vetorizada
    # np.divide trata a divisão (embora tenhamos tratado o preço, é boa prática)
    lec_calculado = np.sqrt(numerador / custo_manutencao)
    
    # --- REGRAS DE ARREDONDAMENTO E LIMITES ---
    # O Lote não pode ser menor que 1 se houver demanda
    lec_final = np.where(demanda > 0, np.ceil(lec_calculado), 0)
    
    # Se o LEC der muito baixo (ex: itens muito baratos sugerindo lotes gigantes),
    # ou muito alto, você pode colocar travas aqui. 
    # Por padrão, vamos apenas garantir que seja pelo menos igual a uma demanda mensal mínima se > 0.
    
    # --- CÁLCULO DO MÁXIMO ---
    # MAX = Ponto de Reabastecimento + Lote Econômico
    df['MAX_Calculado'] = pr + lec_final

    # Fallback: Se o cálculo resultar em 0 (item sem demanda histórica), 
    # mantém regra de segurança simples (2x PR) ou zera.
    df['MAX_Calculado'] = np.where(
        df['MAX_Calculado'] == 0, 
        pr * 2, 
        df['MAX_Calculado']
    )

    # Calculo da OP com Max calculado

    df['Quantidade_OP_Calculada'] = df['MAX_Calculado'] - df['Saldo_Virtual']

    # Volume OP
    df['Volume_OP'] = df['Volume'] * df['Quantidade_Ordem']

    # Nível de Serviço
    # 'S' in Planejador -> 0.98, 'U' -> 0.95, else 0.95
    p_mrp = df['Planejador_MRP'].astype(str).fillna('')
    df['Nivel_Servico'] = np.where(p_mrp.str.contains('S'), 0.98, 0.95)

    # Valor Tributado
    # Isento se Planejador startswith 'S' OR (Planejador in [U13, U18] AND Grupo startswith prefixos)
    prefixes = ('0201', '2901', '2803')
    g_merc = df['Grupo_Mercadoria'].astype(str).fillna('')
    
    cond_s = p_mrp.str.startswith('S')
    cond_u = p_mrp.isin(['U13', 'U18'])
    cond_grp = g_merc.str.startswith(prefixes)
    
    df['Valor_Tributado'] = np.where(cond_s | (cond_u & cond_grp), 'Isento', 'Tributado')

    # ---------------------------------------------------------
    # 9. ANÁLISE DE TEXTO (Pós Análise)
    # ---------------------------------------------------------
    df['pos_analise'] = ''
    df['Compras_Sustentaveis'] = False
    df['Desenho_Tecnico'] = False
    df['Especificacoes'] = False
    
    txt_obs = df['Texto_Observacao_PT'].astype(str).str.lower().fillna('')
    
    # Máscaras booleanas
    mask_sust = txt_obs.str.contains('sustent', regex=False)
    mask_des = txt_obs.str.contains('desenh', regex=False)
    mask_esp = txt_obs.str.contains('especifica', regex=False)
    
    # Atribuição vetorizada
    df.loc[mask_sust, 'Compras_Sustentaveis'] = True
    df.loc[mask_sust, 'pos_analise'] += '| Anexar requisitos de Compras sustentaveis\n'
    
    df.loc[mask_des, 'Desenho_Tecnico'] = True
    df.loc[mask_des, 'pos_analise'] += '| Anexar desenho tecnico\n'
    
    df.loc[mask_esp, 'Especificacoes'] = True
    df.loc[mask_esp, 'pos_analise'] += '| Anexar especificações\n'

    # ---------------------------------------------------------
    # 10. FORMATAÇÃO FINAL E EXPORTAÇÃO
    # ---------------------------------------------------------
    df['Politica_Atual'] = df.get('Tipo_MRP', 'NAO IDENTIFICADA')
    df['PR_Atual'] = df.get('Ponto_Reabastecimento', 0)
    df['MAX_Atual'] = df.get('Estoque_Maximo', 0)

    # Colunas vazias para preenchimento manual/IA posterior
    empty_cols = [
        'Analise_AI', 'Quantidade_OP_AI', 'PR_AI', 'MAX_AI', 'Politica_AI', 'Comentario',
        'Referencia_Encontrada', 'Atende_Descritivo', 'Disponibilidade_Mercado',
        'Preco_Mercado', 'Link_Produto', 'Erro',
        'preco_estimado', 'disponibilidade', 'fornecedor_principal', 
        'url_fonte', 'analise_confianca', 'moeda', 'produto_identificado'
    ]
    
    # Maneira eficiente de adicionar colunas vazias
    df = df.reindex(columns=df.columns.tolist() + empty_cols)
    df[empty_cols] = ''

    # ---------------------------------------------------------
    # 11. PRIORIDADE DE ANÁLISE
    # ---------------------------------------------------------
    df['Prioridade'] = _calculate_priority(df)

    return df