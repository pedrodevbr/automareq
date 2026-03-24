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

def process_excel_data(
    file_op: str = os.path.join(INPUT_FOLDER, 'OP.XLSX'),
    file_0127: str = os.path.join(INPUT_FOLDER, '0127.XLSX'),
    file_0130: str = os.path.join(INPUT_FOLDER, '0130.XLSX'),
    file_column_mapping: str = os.path.join(TEMPLATES_FOLDER, 'column_mapping.csv')
) -> pd.DataFrame:
    """Processa e mescla dados de Excel."""
    # Carregar dados
    op = pd.read_excel(file_op, parse_dates=True, dtype={'Material': str})
    t0127 = pd.read_excel(file_0127).astype(str)
    t0127 = t0127.drop(columns=['Status', 'Texto CLA - pt','Texto CLA - es','Texto LMR','Linha'], errors='ignore')
    t0130 = pd.read_excel(file_0130, dtype=str)
    t0130 = t0130.drop(columns=['Txt.brv.material', 'Prz.entrg.prev.'], errors='ignore')
    for col in t0130.columns:
        t0130[col] = t0130[col].apply(clean_and_convert)
    t0130.rename(columns={'Material': 'Codigo_Material'}, inplace=True)
    t0130['Codigo_Material'] = t0130['Codigo_Material'].astype(str)

    column_mapping = pd.read_csv(file_column_mapping, dtype=str)
    rename_mapping = dict(zip(column_mapping['Coluna_Original'], column_mapping['Coluna_Padronizada']))

    # Mesclar dados
    t0127_concat = t0127.replace('nan', pd.NA).groupby('Material').agg(lambda x: '\n'.join(x.dropna().astype(str))).reset_index()
    df = op.merge(t0127_concat, on='Material', how='left', suffixes=('', '_t0127'))
    df.rename(columns=rename_mapping, inplace=True)
    t0130.rename(columns=rename_mapping, inplace=True)
    df = df.merge(t0130, on='Codigo_Material', how='left')

    # Aplicar tipos de dados
    for _, row in column_mapping.iterrows():
        col_name = row['Coluna_Padronizada']
        target_type = row['Tipo_Variavel_Python']
        if col_name in df.columns:
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

    # Permitir apenas as colunas do column_mapping, na ordem definida
    colunas_finais = [row['Coluna_Padronizada'] for _, row in column_mapping.iterrows() if row['Coluna_Padronizada'] in df.columns]
    df = df[colunas_finais]

    # inicializar colunas adicionais
    df['Nivel_Servico'] = .92
    df['Dias_Em_OP'] = 0
    df['Text_Analysis'] = ''
    df['Analise_Gestor'] = ''
    df['pre_analise'] = ''

    export_by_responsavel(df, filename='Step1-ETL')

    return df