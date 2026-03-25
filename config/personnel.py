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


def country_for_responsavel(resp_key: str) -> str:
    """Returns 'BR' or 'PY' based on RESPONSAVEIS config. Defaults to 'BR'."""
    info = RESPONSAVEIS.get(str(resp_key).strip().upper())
    return info[0] if info else "BR"
