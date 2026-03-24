import os
import logging
import pandas as pd
from typing import List, Dict, Any, Optional, Union
from jira import JIRA, Issue
from jira.resources import Comment
from dotenv import load_dotenv
from tqdm import tqdm

# Carrega as variáveis do arquivo .env
load_dotenv("./config/.env")

logger = logging.getLogger(__name__)

class JiraModule:
    # Constantes do Jira (IDs de campos customizados e Projeto)
    PROJECT_KEY = "GCSMIT"
    FIELD_TIPO_CONSULTA = "customfield_17633"
    FIELD_SALDO_VIRTUAL = "customfield_17601"

    def __init__(self):
        """
        Inicializa a conexão com o Jira usando variáveis de ambiente.
        """
        self.server = os.getenv('JIRA_SERVER')
        self.user = os.getenv('JIRA_USER') or os.getenv('JIRA_EMAIL')
        self.token = os.getenv('JIRA_API_TOKEN')
        self.password = os.getenv('JIRA_PASSWORD')
        
        # Tratamento do certificado SSL (pode ser caminho str ou bool)
        cert_env = os.getenv('JIRA_CERT', 'True')
        if cert_env.lower() == 'false':
            self.cert = False
        elif cert_env.lower() == 'true':
            self.cert = True
        else:
            self.cert = cert_env # Caminho para o arquivo .crt/.pem

        # Validação básica
        if not self.server:
            raise ValueError("JIRA_SERVER não definido no arquivo .env")

        auth_secret = self.token or self.password
        if not self.user or not auth_secret:
            raise ValueError("Credenciais (JIRA_USER e TOKEN/PASSWORD) incompletas no .env")

        try:
            self.jira = JIRA(
                server=self.server,
                basic_auth=(self.user, auth_secret),
                options={'verify': self.cert},
                max_retries=1,
            )
            logger.info(f"Conectado ao Jira em: {self.server}")
        except Exception as e:
            logger.warning(f"Não foi possível conectar ao Jira ({self.server}): {e}")
            self.jira = None

    def search_tickets(self, code: str, max_results: int = 5) -> List[Issue]:
        """Busca issues cujo summary contenha o código do material."""
        if not self.jira:
            return []
        try:
            # Aspas escapadas para garantir busca exata se tiver caracteres especiais
            jql = f'project = {self.PROJECT_KEY} AND summary ~ "{code}" ORDER BY updated DESC'
            issues = self.jira.search_issues(jql, maxResults=max_results)
            
            if not issues:
                logger.debug(f"Nenhum ticket encontrado para: {code}")
                return []
            
            return issues
        except Exception as e:
            logger.error(f"Erro ao buscar tickets para {code}: {e}")
            return []

    def read_comments(self, ticket_key_or_issue: Union[str, Issue]) -> List[Dict[str, Any]]:
        """Retorna uma lista de dicionários com os comentários."""
        if not self.jira:
            return []
        try:
            # Garante que temos um objeto Issue
            if isinstance(ticket_key_or_issue, str):
                issue = self.jira.issue(ticket_key_or_issue)
            else:
                issue = ticket_key_or_issue

            # Acessa os comentários de forma segura
            comments_data = []
            if hasattr(issue.fields, 'comment') and hasattr(issue.fields.comment, 'comments'):
                for c in issue.fields.comment.comments:
                    comments_data.append({
                        'author': c.author.displayName if hasattr(c, 'author') else 'Desconhecido',
                        'body': getattr(c, 'body', ''),
                        'created': getattr(c, 'created', ''),
                        'updated': getattr(c, 'updated', '')
                    })
            return comments_data
        except Exception as e:
            key = ticket_key_or_issue.key if hasattr(ticket_key_or_issue, 'key') else str(ticket_key_or_issue)
            logger.error(f"Erro ao ler comentários do ticket {key}: {e}")
            return []

    def create_ticket(self, title: str, description: str, tipo: str = 'Otros', pieces_in_stock: Union[int, str] = 0) -> Optional[Issue]:
        """Cria um issue genérico."""
        if not self.jira:
            return None
        try:
            issue_dict = {
                'project': {'key': self.PROJECT_KEY},
                'summary': title,
                'description': description,
                'issuetype': {'name': 'Task'},
                self.FIELD_TIPO_CONSULTA: {'value': tipo},
                self.FIELD_SALDO_VIRTUAL: str(pieces_in_stock),
            }
            new_issue = self.jira.create_issue(fields=issue_dict)
            logger.info(f"Issue criado com sucesso: {new_issue.key} | {title}")
            return new_issue
        except Exception as e:
            logger.exception(f"Erro ao criar issue '{title}': {e}")
            return None

    def add_comment(self, ticket_key: str, comment_body: str) -> bool:
        """Adiciona um comentário a um ticket existente."""
        if not self.jira:
            return False
        try:
            # Se passar objeto Issue, extrai a key
            key = ticket_key.key if hasattr(ticket_key, 'key') else ticket_key
            self.jira.add_comment(key, comment_body)
            logger.info(f"Comentário adicionado ao ticket {key}")
            return True
        except Exception as e:
            logger.error(f"Erro ao comentar em {ticket_key}: {e}")
            return False

    # --- Wrappers de Negócio ---

    def create_zs_ticket(self, code: str, short_text: str, reference: str, saldo_virtual: str = "0") -> Optional[Issue]:
        """Cria ticket para ZSTK (Reposição ZS)."""
        title = f"{code} - {short_text}"
        description = (
            f"Prezados\n"
            f"Favor verificar a necessidade de reposição do material: {code} - {short_text}\n"
            f"Caso seja necessária reposição favor indicar referência atualizada.\n"
            f"Referência atual: {reference}"
        )
        return self.create_ticket(
            title=title, 
            description=description, 
            tipo='Reposición ZS (sobre consulta)', 
            pieces_in_stock=saldo_virtual
        )

    def create_frac_ticket(self, code: str, short_text: str, text: str, saldo_virtual: str = "0") -> Optional[Issue]:
        """Cria ticket para FRAC (Licitação Deserta)."""
        title = f"{code} - {short_text}"
        return self.create_ticket(
            title=title, 
            description=text, 
            tipo='Licitación Desierta', 
            pieces_in_stock=saldo_virtual
        )

    def find_last_comment(self, code: str):
        """Busca o último comentário do ticket mais recente de um material."""
        issues = self.search_tickets(code, max_results=1)
        if not issues:
            return [], None
        
        last_issue = issues[0]
        comments = self.read_comments(last_issue)
        return comments, last_issue.key

    def search_lote_tickets(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Busca tickets em lote para um DataFrame.
        Retorna um DataFrame auxiliar com as colunas do Jira.
        """
        results = []
        logger.info(f"Iniciando busca Jira para {len(df)} itens...")
        
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Consultando Jira"):
            code = str(row.get('Codigo_Material', ''))
            
            jira_data = {
                'Codigo_Material': code,
                'JIRA_CODIGO': None,
                'JIRA_TITULO': None,
                'JIRA_COMENTARIO': None
            }

            if code:
                issues = self.search_tickets(code, max_results=1)
                if issues:
                    issue = issues[0]
                    comments = self.read_comments(issue)
                    last_comment_body = comments[-1]['body'] if comments else ''
                    
                    jira_data.update({
                        'JIRA_CODIGO': issue.key,
                        'JIRA_TITULO': issue.fields.summary,
                        'JIRA_COMENTARIO': last_comment_body
                    })

            results.append(jira_data)

        return pd.DataFrame(results)

    def verificar_consultas_abertas(self,codigo):
        # retorna False se a ultima consulta esta concluida
        logger.info(f"Verificando se há consultas abertas para {codigo}...")

        try:
            # Nota: search_tickets não é um método padrão, verifique sua lib jira_service
            # Supondo que retorne uma lista de issues
            issues = self.search_tickets(codigo,max_results=1)
            
            if issues:
                # Nota: status_issue deve ser uma função auxiliar sua ou propriedade do objeto issue
                # Exemplo comum: issue.fields.status.name
                current_status = getattr(issues[0].fields.status, 'name', str(issues[0].fields.status))
                
                if current_status in ["Terminado", "Concluído", "Done"]: # Ajuste conforme seu workflow
                    logger.info(f"Ticket {issues[0].key} finalizado.")
                    return False
                else:
                    logger.debug(f"Ticket {issues[0].key} ainda em andamento ({current_status}).")
                    return True
            else:
                logger.warning(f"Nenhum ticket encontrado para o material {codigo}")
                return True

        except Exception as e:
            logger.error(f"Erro ao verificar consulta para {codigo}: {e}")
            return True


# --- Bloco de Teste ---
if __name__ == "__main__":
    # Configuração básica de log para visualizar no console
    logging.basicConfig(level=logging.INFO)
    
    print("--- Teste Jira Module ---")
    try:
        jira = JiraModule()
        
        # Teste 1: Busca
        cod_teste = '267163' # Exemplo
        print(f"\n1. Buscando material {cod_teste}...")
        print(jira.verificar_consultas_abertas(cod_teste))
    except Exception as e:
        print(e)