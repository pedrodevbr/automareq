import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pathlib import Path

# Supondo que 'df' seja o dataframe retornado pela sua função run_calculations(df_input)
def visualization(df,DIR):
    # --- 1. Mapa de Calor (Heatmap) ---
    plt.figure(figsize=(12, 8))

    # politica diferente
    heatmap_data = df[df['Politica_Atual']!=df['Politica_Sugerida']]

    # Criar tabela cruzada (Contagem de itens por par de políticas)
    # Usamos 'Tipo_MRP' como Atual e 'Politica_Sugerida' como a da IA
    heatmap_data = pd.crosstab(
        heatmap_data['Politica_Atual'].fillna('N/A'), 
        heatmap_data['Politica_Sugerida'].fillna('N/A')
    )

    sns.heatmap(heatmap_data, annot=True, fmt='d', cmap='YlGnBu')
    plt.title('Matriz de Migração: Política Atual vs Sugerida (Algoritmo)')
    plt.ylabel('Política Atual (Tipo_MRP)')
    plt.xlabel('Política Sugerida (Árvore de Decisão)')
    plt.tight_layout()
    plt.savefig(DIR / Path('heatmap_politicas.png'))
    #plt.show()

    # --- 2. Scatter Plot (Classificação) ---
    plt.figure(figsize=(12, 8))

    # Filtrar outliers extremos para melhor visualização (opcional)
    # df_plot = df[(df['CV'] < 5) & (df['TMD'] < 10)] 
    df_plot = df.copy()

    sns.scatterplot(
        data=df_plot,
        x='TMD',
        y='CV',
        hue='Classificacao',
        palette='viridis',
        alpha=0.6,
        s=60
    )

    # Adicionar linhas de corte (Thresholds)
    # Você precisará importar as constantes do seu config ou definir manualmente aqui
    # from config.config import CV_THRESHOLD, TMD_THRESHOLD
    CV_THRESHOLD = 0.7  # Exemplo, ajuste conforme seu config
    TMD_THRESHOLD = 1.32 # Exemplo, ajuste conforme seu config

    plt.axhline(y=CV_THRESHOLD, color='r', linestyle='--', alpha=0.5, label=f'CV Cutoff ({CV_THRESHOLD})')
    plt.axvline(x=TMD_THRESHOLD, color='r', linestyle='--', alpha=0.5, label=f'TMD Cutoff ({TMD_THRESHOLD})')

    plt.title('Distribuição dos Materiais: Coeficiente de Variação x TMD')
    plt.xlabel('TMD (Periodicidade da Demanda)')
    plt.ylabel('CV (Volatilidade da Demanda)')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(DIR / Path('scatter_classificacao.png'))
    #plt.show()