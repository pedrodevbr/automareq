import os
import pandas as pd
from config.config import INPUT_FOLDER, OUTPUT_FOLDER, TEMPLATES_FOLDER
from utils.export_module import export_by_responsavel


def clean_and_convert(s):
    """Limpa e converte strings para numérico."""
    s = str(s).strip().replace('.', '').replace(',', '.')
    multiplier = 1 if '-' not in s else -1
    s = s.replace('-', '')
    return multiplier * pd.to_numeric(s, errors='coerce')


def _load_column_mapping(path):
    """Carrega o column_mapping.csv e retorna o DataFrame completo."""
    return pd.read_csv(path, dtype=str)


def _build_rename_map(mapping_df, fonte):
    """Cria dicionário de renomeação para uma fonte específica (OP, 0127, 0130)."""
    subset = mapping_df[mapping_df['Fonte'] == fonte]
    return dict(zip(subset['Coluna_Original'], subset['Coluna_Padronizada']))


def _get_drop_columns(mapping_df, fonte):
    """Retorna colunas marcadas como Incluida=False para uma fonte."""
    subset = mapping_df[(mapping_df['Fonte'] == fonte) & (mapping_df['Incluida'] == 'False')]
    return list(subset['Coluna_Original'])


def _get_type_map(mapping_df):
    """Retorna dict {Coluna_Padronizada: Tipo_Variavel_Python} para colunas incluídas."""
    included = mapping_df[mapping_df['Incluida'] == 'True']
    # Deduplica — pega a primeira ocorrência de cada coluna padronizada
    included = included.drop_duplicates(subset='Coluna_Padronizada', keep='first')
    return dict(zip(included['Coluna_Padronizada'], included['Tipo_Variavel_Python']))


def process_excel_data(
    file_op: str = os.path.join(INPUT_FOLDER, 'OP.XLSX'),
    file_0127: str = os.path.join(INPUT_FOLDER, '0127.XLSX'),
    file_0130: str = os.path.join(INPUT_FOLDER, '0130.XLSX'),
    file_column_mapping: str = os.path.join(TEMPLATES_FOLDER, 'column_mapping.csv')
) -> pd.DataFrame:
    """Processa e mescla dados de Excel usando column_mapping.csv."""

    mapping = _load_column_mapping(file_column_mapping)

    # --- OP ---
    op = pd.read_excel(file_op, parse_dates=True, dtype={'Material': str})
    rename_op = _build_rename_map(mapping, 'OP')
    op.rename(columns=rename_op, inplace=True)

    # --- 0127 ---
    t0127 = pd.read_excel(file_0127).astype(str)
    drop_0127 = _get_drop_columns(mapping, '0127')
    t0127 = t0127.drop(columns=drop_0127, errors='ignore')
    rename_0127 = _build_rename_map(mapping, '0127')
    # Agrupar por material (concatenar textos)
    t0127 = t0127.replace('nan', pd.NA).groupby('Material').agg(
        lambda x: '\n'.join(x.dropna().astype(str))
    ).reset_index()
    t0127.rename(columns={'Material': 'Codigo_Material'}, inplace=True)
    t0127.rename(columns=rename_0127, inplace=True)

    # --- 0130 ---
    t0130 = pd.read_excel(file_0130, dtype=str)
    # Renomear Material antes de dropar (necessário para merge)
    t0130.rename(columns={'Material': 'Codigo_Material'}, inplace=True)
    drop_0130 = _get_drop_columns(mapping, '0130')
    t0130 = t0130.drop(columns=drop_0130, errors='ignore')
    for col in t0130.columns:
        if col != 'Codigo_Material':
            t0130[col] = t0130[col].apply(clean_and_convert)
    rename_0130 = _build_rename_map(mapping, '0130')
    t0130.rename(columns=rename_0130, inplace=True)
    t0130['Codigo_Material'] = t0130['Codigo_Material'].astype(str)

    # --- Merge ---
    df = op.merge(t0127, on='Codigo_Material', how='left', suffixes=('', '_t0127'))
    df = df.merge(t0130, on='Codigo_Material', how='left', suffixes=('', '_t0130'))

    # Se houver colunas duplicadas do merge (ex: LTD_1 do OP e LTD_1 do 0130),
    # preferir o valor do 0130 (mais LTDs) e preencher lacunas com OP
    for col in df.columns:
        if col.endswith('_t0130'):
            base_col = col.replace('_t0130', '')
            if base_col in df.columns:
                df[base_col] = df[col].combine_first(df[base_col])
            df.drop(columns=[col], inplace=True)
        elif col.endswith('_t0127'):
            base_col = col.replace('_t0127', '')
            if base_col in df.columns:
                df[base_col] = df[col].combine_first(df[base_col])
            df.drop(columns=[col], inplace=True)

    # --- Aplicar tipos de dados ---
    type_map = _get_type_map(mapping)
    for col_name, target_type in type_map.items():
        if col_name not in df.columns:
            continue
        df[col_name] = df[col_name].fillna('')
        if target_type == 'str':
            df[col_name] = df[col_name].astype(str)
        elif target_type == 'int':
            df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype(pd.Int64Dtype())
        elif target_type == 'float':
            df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
        elif target_type == 'datetime':
            df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
        elif target_type == 'bool':
            df[col_name] = df[col_name].apply(lambda x: True if str(x).strip().upper() == 'X' else False)

    # Filtrar apenas colunas incluídas, na ordem do mapping
    included = mapping[mapping['Incluida'] == 'True']
    colunas_finais = list(dict.fromkeys(
        col for col in included['Coluna_Padronizada'] if col in df.columns
    ))
    df = df[colunas_finais]

    # Inicializar colunas adicionais
    df['Nivel_Servico'] = .92
    df['Dias_Em_OP'] = 0
    df['Text_Analysis'] = ''
    df['Analise_Gestor'] = ''
    df['pre_analise'] = ''

    export_by_responsavel(df, filename='Step1-ETL')

    return df
