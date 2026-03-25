ANALYSIS_COLUMNS = [
    "Codigo_Material", "Texto_PT", "Numero_Peca_Fabricante", "Prazo_Entrega_Previsto",
    "Estoque_Total", "Preco_Unitario", "RTP1", "RTP2", "RTP3", "RTP6", "Nivel_Servico",
    "Saldo_Virtual", "Criticidade", "Demanda_Programada", "Quantidade_Requisitada", "Quantidade_Pedida",
    "Classificacao_ABC", "Adicional_Lote_Obrigatorio"
] + [f'LTD_{i}' for i in range(1, 12)]

DEPENDENCIES = [
    "pandas",
    "openai",
    "google-generativeai",
    "requests",
    "matplotlib",
    "seaborn",
    "joblib",
    "tenacity",
    "jira"
]
