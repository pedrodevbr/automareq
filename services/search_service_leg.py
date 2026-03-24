import os
import logging
import json
import pandas as pd
import re
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

class SearchModule:
    def __init__(self, model_name="google/gemini-3.1-flash-lite-preview"):
        self.api_key = os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError("OPENROUTER_API_KEY not found in .env")

        self.model_name = model_name
        self.client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=self.api_key,
        )
        logger.info(f"SearchModule iniciado com: {self.model_name}")

    def _clean_json_string(self, text):
        """Remove markdown (```json ... ```) se o modelo adicionar."""
        text = text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\n", "", text)
            text = re.sub(r"\n```$", "", text)
        return text.strip()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def search_item(self, row):
        """Pesquisa item e retorna dict com as chaves exatas solicitadas."""
        
        codigo = row.get('Codigo_Material', '')
        texto = row.get('Texto_Breve_Material', '')
        ref = row.get('Numero_Peca_Fabricante', '')
        desc = row.get('Texto_PT', '')

        # Definição das chaves vazias para caso de erro
        empty_result = {
            "produto_identificado": None,
            "preco_unitario_estimado": None,
            "moeda": "",
            "url_fonte": "",
            "disponibilidade": "Não Verificado",
            "analise_confianca": "Erro na execução",
            "fornecedor_principal": ""
        }

        # Prompt ajustado para as colunas específicas
        prompt = f"""
        Atue como um comprador industrial especialista. Pesquise na web o preço e disponibilidade atual deste item:
        
        INPUT:
        - Código: {codigo}
        - Nome: {texto}
        - Referência: {ref}
        - Detalhes: {desc}
        
        OBJETIVO: Encontrar fornecedor no Brasil (prioridade) ou Internacional.
        
        Responda APENAS um JSON válido com estas chaves exatas:
        {{
            "produto_identificado": "Nome exato da referencia do produto que você encontrou no site",
            "preco_unitario_estimado": "Valor numérico (ex: 120.50) ou null se não achar preço, se necessário divida o valor pela quantidade de unidades",
            "moeda": "BRL, USD ou EUR",
            "url_fonte": "Link direto para o produto",
            "disponibilidade": "Em Estoque, Sob Encomenda ou Indisponível",
            "analise_confianca": "Breve explicação se o produto encontrado é igual, similar ou substituto ao solicitado",
            "fornecedor_principal": "Nome do fornecedor"
        }}
        """

        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": "You are a JSON-only market researcher. Never include explanations outside the JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1
            )
            
            raw_content = response.choices[0].message.content
            clean_content = self._clean_json_string(raw_content)
            
            data = json.loads(clean_content)
            return data

        except json.JSONDecodeError:
            err_res = empty_result.copy()
            err_res['analise_confianca'] = "Erro de Formatação JSON da IA"
            logger.warning(f"Erro JSON {codigo}: {raw_content}...")
            return err_res
            
        except Exception as e:
            err_res = empty_result.copy()
            err_res['analise_confianca'] = f"Erro API: {str(e)}"
            logger.error(f"Erro pesquisa {codigo}: {e}")
            return err_res

    def run_search_batch(self, df: pd.DataFrame, max_workers=5) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        records = df.to_dict('records')
        results = []
        
        print(f"🌍 Iniciando Pesquisa Perplexity para {len(records)} itens...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(tqdm(executor.map(self.search_item, records), total=len(records), desc="Pesquisando"))
            
        # Retorna DataFrame alinhado ao índice original
        return pd.DataFrame(results, index=df.index)