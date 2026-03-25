"""Business constants and thresholds for stock analysis."""

# Thresholds de análise
CV_THRESHOLD = 0.7
TMD_THRESHOLD = 1.32
DEMAND_WINDOW = 12

# Faixas de valor
BAIXO_VALOR = 100
ALTO_VALOR = 10000
VALOR_UN_ALTO = 1000
ALTO_VOLUME = 1000000

# Tempo
ANOS_SEM_OC = 2

# Constantes de negócio
CUSTO_FIXO_PEDIDO = 100.0   # Custo administrativo estimado por pedido (S)
TAXA_MANUTENCAO = 0.10      # 10% ao ano sobre o valor do item (i)

# Emissão — limiar para seleção de template AD
AD_VALUE_THRESHOLD = 7_000  # BRL — determina CPV (≤7k) vs Inexigibilidade (>7k)
