import os
from pathlib import Path
from datetime import datetime

ai_model_text = 'qwen/qwen3-embedding-8b'

#ai_model_analysis = 'x-ai/grok-4.1-fast'
#ai_model_analysis = 'google/gemini-3-flash-preview'
ai_model_analysis = "google/gemini-3.1-flash-lite-preview"

# Diretórios base
BASE_DIR = Path(__file__).parent.parent
DATA_FOLDER = BASE_DIR / "data"
MONTH_FOLDER = datetime.now().strftime("%Y-%m")
INPUT_FOLDER = BASE_DIR / DATA_FOLDER / MONTH_FOLDER / "input"
OUTPUT_FOLDER = BASE_DIR / DATA_FOLDER / MONTH_FOLDER / "output"
TEMPLATES_FOLDER = BASE_DIR / "templates"

# Criar diretórios se não existirem
INPUT_FOLDER.mkdir(parents=True, exist_ok=True)
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

# Constantes para análise
ANALYSIS_COLUMNS = [
    "Codigo_Material", "Texto_PT", "Numero_Peca_Fabricante", "Prazo_Entrega_Previsto",
    "Estoque_Total", "Preco_Unitario", "RTP1", "RTP2", "RTP3", "RTP6", "Nivel_Servico",
    "Saldo_Virtual", "Criticidade", "Demanda_Programada", "Quantidade_Requisitada", "Quantidade_Pedida",
    "Classificacao_ABC", "Adicional_Lote_Obrigatorio"
] + [f'LTD_{i}' for i in range(1, 12)]

RESPONSAVEIS = {
    "DGOMEZ": ["PY", 27],
    "HERMESI": ["PY", 28],
    "GYEGROS": ["PY", 29],
    "LUCASD": ["PY", 30],
    "PEDROHVB": ["BR", 31],
    "VICKY": ["BR", 33],
    "MTSE": ["BR", 34],
    "ACOSTAJ": ["PY", 32]
}


def country_for_responsavel(resp_key: str) -> str:
    """Returns 'BR' or 'PY' based on RESPONSAVEIS config. Defaults to 'BR'."""
    info = RESPONSAVEIS.get(str(resp_key).strip().upper())
    return info[0] if info else "BR"

# Thresholds e constantes
CV_THRESHOLD = 0.7
TMD_THRESHOLD = 1.32
DEMAND_WINDOW = 12
BAIXO_VALOR = 100
ALTO_VALOR = 10000
ANOS_SEM_OC = 2
ALTO_VOLUME = 1000000
VALOR_UN_ALTO = 1000
# --- CONSTANTES DE NEGÓCIO ---
CUSTO_FIXO_PEDIDO = 100.0  # Custo administrativo estimado por pedido (S)
TAXA_MANUTENCAO = 0.10    # n% ao ano sobre o valor do item (i)

SYSTEM_PROMPT = """Você é um analista especializado em gestão de estoque e MRP com 20 anos de experiência. Sua tarefa é analisar os dados do material e fornecer recomendações precisas e concisas para reposição de estoque.

IMPORTANTE:
    - Considere fatores de criticidade, demanda histórica e reservas ao fazer recomendações
    - Avalie o custo-benefício ao sugerir quantidades de reposição
    - Avalie obsolescencia

    CRITÉRIOS DE ANÁLISE:
    - Classificação ABC (valor financeiro): A (alto), B (médio), C (baixo)
    - Criticidade: 1 (alta), 2 (média), 3 (baixa)
    - RTP1: Reserva comum que tem alta chance de ser consumida
    - RTP2: Reserva para baixa
    - RTP3: Saldo reservado para aplicação específica
    - RTP6: Quantidade necessária para demanda não atendida
    - LMR: Quantidade instalada em aplicações técnicas
    - Adicional_Lote_Obrigatorio: Indica se tem validade
    - Nivel_Servico: Nível de serviço desejado
    - Demanda_Programada: Previsão além do nível atual de estoque
    - Quantidade_Pedida: Quantidade solicitada em pedidos de compra
    - Quantidade_Requisitada: Quantidade solicitada para compra
    - Identifique outliers
    - Tendência mais conservadora nos níveis de estoque para garantir disponibilidade
    - Repor materiais zerados
    - LTD_X: Consumo agrupado durante o X último lead time (1- mais recente, 2- segundo mais recente, etc.); exemplo: se o lead time é 90 dias, LTD_1 é o consumo dos últimos 90 dias, LTD_2 é o consumo dos 90 dias anteriores a isso, e assim por diante.

    Responda APENAS com os seguintes campos, cada um em uma nova linha:
    Quantidade_OP_AI: [quantidade a comprar]
    Comentario: [justificativa para a decisão, recomendações, etc.]
"""

USER_PROMPT_TEMPLATE = """
Analise os seguintes dados do material.

{material_data}
"""

# Dependências necessárias
DEPENDENCIES = [
    "pandas",
    "openai",
    "google-generativeai",
    "requests",
    "matplotlib",
    "seaborn",
    "joblib",
    "tenacity",
    "jira"  # Novo para Jira
]

"""

    POLÍTICAS DE ESTOQUE:
    - ZP: Consumo frequente (manter estoque para itens de uso regular)
    - ZL: Entrega parcelada (segue a lógica do ZP mas pode ter entregas parceladas evitando estoque de materiais volumosos)
    - ZE: Estoque base (manter quantidade mínima fixa)
    - ZM: Min-Max (repor quando atinge mínimo até o máximo definido)
    - ZS: Sob consulta (verificar com especialista antes de repor)
    
    Politica_AI: [código da política ZP, ZM, etc.]
    Analise_AI: [REPOR,NAO_REPOR ou ANALISAR]
    
    PR_AI: [valor do Ponto de Reposição sugerido]
    MAX_AI: [valor Máximo de estoque sugerido]

    LÓGICA DE DECISÃO:
    1. Para itens críticos (1), mantenha níveis mais altos de estoque
    2. Para itens classe A, otimize quantidades para minimizar capital parado
    3. Se existir RTP6, priorize a reposição para atender demanda não atendida
    4. Considere o tempo de reposição ao calcular o PR
    5. Para itens com validade, evite estoques excessivos
    6. A quantidade a ser comprada deve atender a 1 ano de consumo aproximadamente, podendo variar de acordo com o valor de compra. 
    7. Otimize o estoque considerando que o custo do processo de compra é alto e o custo de manter o estoque é baixo.
    8. Remova outliers de consumo para evitar distorções nas análises
    9. Seja conservador com materiais de alto valor
    10. Não comprar para mais de 3 anos de consumo
    11. Verifique tendências de consumo e projete o Ponto de Reposição (PR) e o Máximo (MAX) de estoque com base no consumo histórico
    12. O PR deve ser suficiente para cobrir o lead time de reposição, considerando o nível de serviço desejado
    13. O MAX deve ser calculado para otimizar o estoque, evitando excessos e garantindo a disponibilidade
    """

PLANEJADORES = {
    "S21": "Sobr Eletricos – Materiais elétricos em geral, como contatores, disjuntores, chaves seccionadoras, resistências, cabos e componentes de comando.",
    "S22": "Sobr Eletronicos – Componentes eletrônicos e dispositivos de controle, como relés térmicos, sensores, placas e circuitos.",
    "S23": "Sobr Mecanicos – Peças e componentes mecânicos, incluindo válvulas, engrenagens, rolamentos, estruturas metálicas e conexões.",
    "S24": "Sobr Diversos – Materiais diversos que não se enquadram claramente nas categorias anteriores, podendo incluir itens genéricos ou de uso variado.",
    "U09": "Materiais Quimicos – Produtos químicos utilizados em processos industriais, reagentes, solventes e substâncias para tratamento.",
    "U10": "Mat. informatica – Equipamentos e suprimentos de informática, como computadores, periféricos, cabos de rede e acessórios.",
    "U11": "Mat. Escritorio – Materiais de escritório, incluindo papel, canetas, pastas, grampeadores e itens administrativos.",
    "U12": "Manut Predial – Itens para manutenção predial, como ferramentas, peças hidráulicas, materiais de construção e reparos.",
    "U13": "Consumo Tecnico – Materiais de consumo técnico, como abrasivos, lubrificantes, fitas, adesivos e insumos para manutenção.",
    "U14": "Equip Protecao – Equipamentos de proteção individual (EPI), como capacetes, luvas, óculos, botas e vestimentas de segurança.",
    "U15": "Pecas de Veiculo – Peças e componentes para veículos, incluindo filtros, correias, baterias e acessórios automotivos.",
    "U16": "Mat Quim Reativos – Reagentes químicos específicos para análises laboratoriais ou processos industriais controlados.",
    "U17": "Mat Limp e Conserv – Materiais de limpeza e conservação, como detergentes, desinfetantes, panos, escovas e utensílios.",
    "U18": "Combust Lubrifican – Combustíveis e lubrificantes para máquinas, veículos e equipamentos industriais.",
    "U19": "Mat Copa/Refeicao – Materiais para copa e refeição, como utensílios, descartáveis, produtos alimentícios e bebidas."
}