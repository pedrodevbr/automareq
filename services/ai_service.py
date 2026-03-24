import os
import logging
import json
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm
from pydantic import BaseModel, Field
from typing import Optional

# Import config
from config.config import ANALYSIS_COLUMNS, SYSTEM_PROMPT, USER_PROMPT_TEMPLATE

load_dotenv("./config/.env")
logger = logging.getLogger(__name__)

# --- MODELO DE DADOS (Schema Rígido) ---
class MaterialAnalysis(BaseModel):
    Analise_AI: str = Field(..., description="Resumo da decisão: 'REPOR', 'NAO_REPOR', 'SEM_CONSUMO', 'ANALISE_MANUAL', etc.")
    Quantidade_OP_AI: Optional[float] = Field(None, description="Quantidade sugerida de compra (apenas números)")
    PR_AI: Optional[float] = Field(None, description="Ponto de Reposição sugerido")
    MAX_AI: Optional[float] = Field(None, description="Estoque Máximo sugerido")
    Politica_AI: Optional[str] = Field(None, description="Política de estoque sugerida (ex: ZS, ES, ZD)")
    Comentario: str = Field(..., description="Explicação detalhada e técnica da decisão tomada e recomendações")

class AIModule:
    def __init__(self, model_name=None):
        self.api_key = os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError("OPENROUTER_API_KEY not found in .env file.")

        self.model_name = model_name or os.getenv("DEFAULT_LLM_MODEL", "google/gemini-2.0-flash-001")
        
        # Cliente OpenAI apontando para OpenRouter
        self.client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=self.api_key,
        )
        logger.info(f"AIModule initialized with model: {self.model_name}")

    def format_row(self, row):
        """Formata uma linha do DataFrame para o prompt de texto."""
        lines = []
        for k in ANALYSIS_COLUMNS:
            v = row.get(k)
            if pd.notna(v) and v != '':
                lines.append(f"{k}: {v}")
        return "\n".join(lines)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def analyze_material_raw(self, row) -> dict:
        """
        Analisa um único material e retorna um dicionário validado.
        """
        material = row.get('Codigo_Material')

        # Validação Rápida de Pré-requisitos
        if pd.isna(row.get('LTD_1')) or row['LTD_1'] == '':
            return {
                'Analise_AI': 'REPOR', 
                'Quantidade_OP_AI': None,
                'PR_AI': None,
                'MAX_AI': None,
                'Politica_AI': None,
                'Comentario': 'Codigo novo - LTD_1 vazio ou inválido (sem histórico recente).'
            }
        
        ltd_cols = [c for c in row.index if str(c).startswith("LTD_")]
        ltd_vals = pd.to_numeric(row[ltd_cols], errors="coerce")
        
        # 1. Drop NaN (ignora valores nulos)
        # 2. Verifica se o que sobrou é igual a 0
        # 3. .all() com parenteses para retornar True/False
        all_zero = ltd_vals.dropna().eq(0).all()

        #se todos os consumos sao 0
        if all_zero:
            return {
                'Analise_AI': 'VERIFICAR', 
                'Quantidade_OP_AI': None,
                'PR_AI': None,
                'MAX_AI': None,
                'Politica_AI': None,
                'Comentario': 'Codigo velho - Todos os consumos zero.'
            }

        prompt = USER_PROMPT_TEMPLATE.format(material_data=self.format_row(row))
        
        try:
            # Chamada com JSON Schema (Garante estrutura)
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                # Força resposta JSON estrita baseada no modelo Pydantic
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "material_analysis",
                        "schema": MaterialAnalysis.model_json_schema()
                    }
                },
                extra_headers={
                    "HTTP-Referer": "https://localhost", 
                    "X-Title": "StockAnalyzer"
                },
                temperature=0.1
            )
            
            content = response.choices[0].message.content
            
            # Parse e Validação
            parsed_data = json.loads(content)
            validated = MaterialAnalysis(**parsed_data)
            
            # Retorna como dict para o Pandas
            return validated.model_dump()

        except Exception as e:
            logger.error(f"Erro AI para material {material}: {e}")
            raise e 

    def _safe_analyze_wrapper(self, args):
        """Wrapper interno para passar (index, row) no ThreadPool."""
        index, row = args
        try:
            result = self.analyze_material_raw(row)
            return index, result
        except Exception as e:
            # Em caso de erro fatal na thread, retorna erro estruturado
            error_res = {
                'Analise_AI': 'ERRO_API',
                'Comentario': f"Falha técnica: {str(e)}"
            }
            return index, error_res

    def analyze_batch(self, df: pd.DataFrame, max_workers=3) -> pd.DataFrame:
        """
        Processa um DataFrame em lote (paralelo) e retorna um DataFrame 
        com as colunas de resultado, alinhado pelo índice original.
        """
        if df.empty:
            return pd.DataFrame()
            
        print(f"🚀 Iniciando IA ({self.model_name}) para {len(df)} itens em {max_workers} threads...")
        
        # Prepara lista de (index, row) para manter rastreabilidade
        items_to_process = list(df.iterrows())
        results_map = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Dispara tarefas
            futures = list(tqdm(
                executor.map(self._safe_analyze_wrapper, items_to_process), 
                total=len(df), 
                desc="Processando IA"
            ))
            
            # Coleta resultados
            for idx, data in futures:
                results_map[idx] = data

        # Reconstrói DataFrame usando o índice original (orient='index')
        df_results = pd.DataFrame.from_dict(results_map, orient='index')
        
        # Garante que todas as colunas do schema existam (preenche com None se faltar)
        for field in MaterialAnalysis.model_fields.keys():
            if field not in df_results.columns:
                df_results[field] = None

        return df_results

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def simple_chat(self, system_message, user_message):
        """
        Generic method for text-based tasks (translations, comparisons).
        Returns the raw string response (not JSON).
        """
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": user_message}
                ],
                temperature=0.2 # Low temperature for factual comparison
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Simple chat error: {e}")
            raise e
