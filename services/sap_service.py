import win32com.client
import win32clipboard
import pandas as pd
import os
import sys
import time
import logging
import subprocess
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Callable, Any
from functools import wraps
from config.config import DATA_FOLDER

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sap_automation.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# --- Decorator para Tratamento de Erros ---
def sap_error_handler(func):
    """Decorator para capturar exceções, logar erros e evitar crash da aplicação."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            func_name = func.__name__
            logging.error(f"Erro no método '{func_name}': {str(e)}")
            # Opcional: traceback.print_exc() se debugging for necessário
            return None # Ou False, dependendo do contexto, mas None é seguro para falhas
    return wrapper

# --- Utilitários ---
class ClipboardUtils:
    @staticmethod
    def copy(text: str) -> bool:
        """Copia texto para o clipboard com fallback para clip.exe."""
        try:
            win32clipboard.OpenClipboard()
            win32clipboard.EmptyClipboard()
            win32clipboard.SetClipboardText(text, win32clipboard.CF_UNICODETEXT)
            win32clipboard.CloseClipboard()
            return True
        except Exception as e:
            logging.warning(f"Erro win32clipboard: {e}. Tentando fallback clip.exe.")
            try:
                process = subprocess.Popen(['clip'], stdin=subprocess.PIPE, close_fds=True, shell=True)
                process.communicate(input=text.encode('utf-16'))
                return True
            except Exception as e2:
                logging.error(f"Falha total no clipboard: {e2}")
                return False

class ExcelUtils:
    @staticmethod
    def read_materials(file_path: Path, possible_columns: List[str] = None) -> List[str]:
        """Lê um Excel e extrai uma lista única de materiais."""
        if not possible_columns:
            possible_columns = ['Codigo_Material', 'Material', 'MATERIAL', 'material', 'Código', 'Codigo', 'MATNR']
        
        try:
            if not file_path.exists():
                logging.error(f"Arquivo não encontrado: {file_path}")
                return []
            
            df = pd.read_excel(file_path)
            # Remove espaços dos nomes das colunas
            df.columns = df.columns.str.strip()
            
            target_col = next((col for col in possible_columns if col in df.columns), None)
            
            if not target_col:
                logging.warning(f"Nenhuma coluna de material encontrada em {file_path.name}")
                return []
                
            materials = df[target_col].dropna().astype(str).str.strip().unique().tolist()
            # Remove sufixos como '.0' se houver conversão numérica indevida
            materials = [m.split('.')[0] for m in materials if m.lower() not in ['nan', 'none', '']]
            
            logging.info(f"Extraídos {len(materials)} materiais de {file_path.name}")
            return materials
        except Exception as e:
            logging.error(f"Erro ao ler Excel {file_path}: {e}")
            return []

# --- Classe Principal SAP ---
class SapManager:
    def __init__(self):
        self.session = self._connect()

    def _connect(self):
        """Estabelece a conexão com a sessão ativa do SAP GUI."""
        try:
            sap_gui = win32com.client.GetObject("SAPGUI")
            application = sap_gui.GetScriptingEngine
            connection = application.Children(0)
            session = connection.Children(0)
            logging.info("Conectado ao SAP GUI com sucesso.")
            return session
        except Exception as e:
            logging.warning(f"SAP GUI não disponível: {e}")
            return None

    # --- Métodos de Baixo Nível (Wrappers) ---
    def _find(self, id_str: str):
        return self.session.findById(id_str)

    def _set_text(self, id_str: str, text: str):
        element = self._find(id_str)
        element.text = text
        element.setFocus()
        #element.caretPosition = len(text)

    def _press(self, id_str: str):
        self._find(id_str).press()

    def _select(self,id_str:str):
        self._find(id_str).selected = True

    def _send_key(self, key_code: int):
        self.session.findById("wnd[0]").sendVKey(key_code)

    def go_home(self):
        """Retorna à tela inicial pressionando F3 ou o botão de voltar repetidamente."""
        try:
            # Tenta comando /n (novo) ou voltar
            #self._send_key(3) # F3 - Back
            #self._send_key(3) 
            self.run_transaction("/n")
            # Alternativa mais robusta: self.run_transaction("/n") se quiser limpar tudo
        except:
            pass

    def run_transaction(self, tcode: str):
        """Executa uma transação pelo código."""
        logging.info(f"Executando transação: {tcode}")
        self._set_text("wnd[0]/tbar[0]/okcd", tcode)
        self._send_key(0) # Enter
        #self._send_key(0)

    def save(self):
        """Pressiona o botão de salvar (Ctrl+S / F11)."""
        self._send_key(11)

    def export_excel_dialog(self, folder_path: str, file_name: str):
        """Manipula as janelas padrão de exportação para Excel do SAP."""
        try:
            # Caminho padrão do menu de exportação (pode variar por relatório, ajustado para o padrão ALV)
            # Geralmente Lista -> Exportar -> Planilha
            self.session.findById("wnd[0]/mbar/menu[0]/menu[3]/menu[1]").select() 
            
            # Seleciona formato Excel (se abrir popup)
            if self.session.findById("wnd[1]", False):
                 self.session.findById("wnd[1]/tbar[0]/btn[0]").press()

            # Define caminho e nome
            self._set_text("wnd[1]/usr/ctxtDY_PATH", str(folder_path))
            self._set_text("wnd[1]/usr/ctxtDY_FILENAME", file_name)
            
            # Botão Gerar/Substituir
            self.session.findById("wnd[1]/tbar[0]/btn[11]").press() 
            logging.info(f"Arquivo exportado: {file_name}")
        except Exception as e:
            logging.error(f"Erro no diálogo de exportação: {e}")

    # --- Lógica de Negócio (Relatórios e Ações) ---

    @sap_error_handler
    def run_report_zmmordenspla(self, export_path: str):
        self.run_transaction("zmmordenspla")
        self._set_text("wnd[0]/usr/ctxtSO_PLWRK-LOW", "chi2")
        self._send_key(8) # F8 - Executar
        self.export_excel_dialog(export_path, "OP.XLSX")
        self.go_home()

    @sap_error_handler
    def run_clipboard_report(self, tcode: str, export_path: str, file_name: str, 
                             materials: List[str], extra_setup: Callable = None):
        """Roda relatórios que aceitam input de materiais via clipboard (botão de seta amarela)."""
        if not materials:
            return

        # Copia materiais para o clipboard
        if not ClipboardUtils.copy("\r\n".join(materials)):
            logging.error("Falha ao copiar materiais para o clipboard.")
            return

        self.run_transaction(tcode)
        
        if tcode =='zmm0130':
            self._set_text("wnd[0]/usr/ctxtP_WERKS",'CHI2')
            self._select("wnd[0]/usr/chkP_AGRLTD")
            self._select("wnd[0]/usr/chkP_ALV")

        # Clica no botão de seleção múltipla do campo Material
        self._press("wnd[0]/usr/btn%_S_MATNR_%_APP_%-VALU_PUSH")
        # Cola do clipboard
        self._press("wnd[1]/tbar[0]/btn[24]") 
        self._send_key(8) # F8 (Confirmar popup)
        
        if extra_setup:
            extra_setup(self.session)
            
        self._send_key(8) # F8 (Executar relatório)
        self.export_excel_dialog(export_path, file_name)
        self.go_home()

    @sap_error_handler
    def create_requisition_zmm0030(self, materials: List[str]) -> bool:
        """Cria requisição em massa via ZMM0030."""
        if not materials:
            return False
            
        ClipboardUtils.copy("\r\n".join(materials))
        try:
            self.run_transaction("zmm0030")
            self._set_text("wnd[0]/usr/ctxtS_WERKS-LOW", "CHI2")
            self._set_text("wnd[0]/usr/ctxtS_PLNUM-LOW", "")
            self._set_text("wnd[0]/usr/ctxtS_DISPO-LOW", "")
            self._set_text("wnd[0]/usr/ctxtS_PAART-LOW", "")

            # Cola materiais
            self._press("wnd[0]/usr/btn%_S_MATNR_%_APP_%-VALU_PUSH")
            self._press("wnd[1]/tbar[0]/btn[24]")
            self._send_key(8)
            
            # Foca e Executa
            self._find("wnd[0]/usr/ctxtS_MATNR-LOW").setFocus()
            self._send_key(0)
            self._send_key(8) # F8
            
            # Confirma
            self._press("wnd[0]/usr/btnCONT")
            logging.info("Requisição ZMM0030 criada.")
            self.go_home()
        except Exception as e:
            logging.error("Falha ao criar requisição ZMM0030.")
            self.go_home()
            return False

        return True

    @sap_error_handler
    def find_requisition_number(self, material: str) -> Optional[str]:
        """Busca o número da BANFN na ME5A."""
        self.run_transaction("me5a")
        
        # Filtro de data (Hoje)
        self._press("wnd[0]/tbar[1]/btn[16]")
        today_str = date.today().strftime("%d%m%Y")
        
        # Caminho longo do campo de data dinâmica, encapsulado em try/catch interno se necessário
        try:
            self._set_text("wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/ssubSUBSCREEN_CONTAINER:SAPLSSEL:1106/ctxt%%DYN002-LOW", today_str)
        except:
            # Fallback se o ID mudar, tenta achar pelo label ou posição (simplificado aqui)
            pass
        
        self._set_text("wnd[0]/usr/ctxtBA_MATNR-LOW", material)
        self._send_key(8) # Executar
        
        # Pega resultado do Grid
        grid = self._find("wnd[0]/usr/cntlGRID1/shellcont/shell")
        if grid.RowCount > 0:
            grid.currentCellRow = -1 # Seleciona tudo ou reseta
            grid.selectColumn("BANFN")
            # Ordenar ou filtrar se necessário
            req_value = grid.getCellValue(1, "BANFN") # Linha 1
            self.go_home()
            return req_value
        
        logging.warning(f"Nenhuma requisição encontrada para {material}")
        self.go_home()
        return None

    @sap_error_handler
    def adjust_requisition_me53n(self, req_num: str,tributacao) -> bool:
        """Ajusta texto e aprova requisição na ME53N."""
        self.run_transaction("me53n")
        
        # Selecionar outra requisição
        self._press("wnd[0]/tbar[1]/btn[17]")
        self._set_text("wnd[1]/usr/subSUB0:SAPLMEGUI:0003/ctxtMEPO_SELECT-BANFN", req_num)
        self._send_key(0) 
        
        # Verificar se precisa fechar popup de mensagens
        if self.session.findById("wnd[1]", False):
            self.session.findById("wnd[1]").sendVKey(0)

        # Modo Edição
        self._press("wnd[0]/tbar[1]/btn[7]")
        
        # Inserir Texto (Header Note)
        # O ID abaixo é complexo e pode variar dependendo da configuração da tela do usuário.
        # Mantendo o original, mas idealmente seria dinâmico.
        try:
            editor = self._find("wnd[0]/usr/subSUB0:SAPLMEGUI:0010/subSUB1:SAPLMEVIEWS:1100/subSUB2:SAPLMEVIEWS:1200/subSUB1:SAPLMEGUI:3102/tabsREQ_HEADER_DETAIL/tabpTABREQHDT1/ssubTABSTRIPCONTROL3SUB:SAPLMEGUI:1230/subTEXTS:SAPLMMTE:0100/subEDITOR:SAPLMMTE:0101/cntlTEXT_EDITOR_0101/shellcont/shell")
            editor.text = f"Reposição de estoque\n{tributacao}"
        except Exception:
            logging.warning("Não foi possível localizar o campo de texto da requisição.")

        # Selecionar Itens e Aprovar
        try:
            grid_itens = self._find("wnd[0]/usr/subSUB0:SAPLMEGUI:0010/subSUB2:SAPLMEVIEWS:1100/subSUB2:SAPLMEVIEWS:1200/subSUB1:SAPLMEGUI:3212/cntlGRIDCONTROL/shellcont/shell")
            grid_itens.setCurrentCell(0, "BNFPO")
            grid_itens.selectAll()
            grid_itens.pressToolbarButton("&MEREQDCMALL") # Flag Documento
        except Exception:
            pass

        # Liberar/Aprovar
        try:
            self._press("wnd[0]/tbar[1]/btn[39]")
            self._send_key(11) # Salvar
        except Exception as e:
            logging.error(f"Erro ao tentar salvar/liberar: {e}")
            self.save() # Tenta salvar direto

        logging.info(f"Requisição {req_num} ajustada.")
        self.go_home()
        return True

    @sap_error_handler
    def run_mrp_md03(self, material: str):
        """Roda MRP individual (MD03)."""
        self.run_transaction("md03")
        self._set_text("wnd[0]/usr/ctxtRM61X-MATNR", material)
        self._set_text("wnd[0]/usr/ctxtRM61X-BERID", "CHI2")
        self._send_key(0)
        
        # Parâmetros
        self._set_text("wnd[0]/usr/ctxtRM61X-BANER", "3")
        self._send_key(0)
        self._send_key(0) # Confirmar execução
        
        # Verifica status
        status = self._find("wnd[0]/sbar").text
        if "erro" in status.lower():
            logging.error(f"Erro MRP {material}: {status}")
        else:
            logging.info(f"MRP executado para {material}")
            
        self.go_home()

    @sap_error_handler
    def change_tipo_mrp(self, material, new_type):
        self.run_transaction("mm02")
        self._set_text("wnd[0]/usr/ctxtRMMG1-MATNR",material)
        self._send_key(0)
        self._find("wnd[0]/usr/tabsTABSPR1/tabpSP12").select()
        self._set_text("wnd[0]/usr/tabsTABSPR1/tabpSP12/ssubTABFRA1:SAPLMGMM:2000/subSUB2:SAPLZMGD1:2481/ctxtMARC-DISGR",new_type)
        self._send_key(11)
        self.go_home()
        print(f"Status alterado: {material}")

    @sap_error_handler
    def set_parametros(self,material, pr,max):
        self.run_transaction("mm02")
        self._set_text("wnd[0]/usr/ctxtRMMG1-MATNR",material)
        self._send_key(0)
        self._find("wnd[0]/usr/tabsTABSPR1/tabpSP12").select()
        self._set_text("wnd[0]/usr/tabsTABSPR1/tabpSP12/ssubTABFRA1:SAPLMGMM:2000/subSUB3:SAPLMGD1:2482/txtMARC-MINBE",str(pr))
        self._set_text("wnd[0]/usr/tabsTABSPR1/tabpSP12/ssubTABFRA1:SAPLMGMM:2000/subSUB3:SAPLMGD1:2482/txtMARC-MABST",str(max))
        self._send_key(11)
        self.go_home()

    

# --- Fluxos de Trabalho ---

def workflow_export_reports():
    """Fluxo para exportar relatórios mensais."""
    base_dir = DATA_FOLDER
    mes_pasta = datetime.now().strftime("%Y-%m")
    export_path = Path(base_dir) / mes_pasta / "input"
    export_path.mkdir(parents=True, exist_ok=True)
    
    sap = SapManager()
    
    # 1. Exportar base
    sap.run_report_zmmordenspla(str(export_path))
    
    # 2. Ler materiais exportados
    op_file = export_path / "OP.XLSX"
    materials = ExcelUtils.read_materials(op_file)
    
    if materials:
        # 3. Relatório ZMM0130
        def setup_0130(session):
            session.findById("wnd[0]/usr/ctxtP_WERKS").text= "CHI2"
            session.findById("wnd[0]/usr/chkP_AGRLTD").selected = True
            session.findById("wnd[0]/usr/chkP_ALV").selected = True

        sap.run_clipboard_report("zmm0130", str(export_path), "0130.XLSX", materials, setup_0130)
        
        # 4. Relatório ZMM0127
        sap.run_clipboard_report("zmm0127", str(export_path), "0127.XLSX", materials)
        
        print("\nProcesso de exportação finalizado.")
    else:
        logging.error("Nenhum material encontrado no OP.XLSX para prosseguir.")

def workflow_process_files(folder_path: str):
    """Fluxo para processar arquivos Excel em uma pasta e criar requisições."""
    folder = Path(folder_path)
    if not folder.exists():
        logging.error("Pasta não encontrada.")
        return

    sap = SapManager()
    files = [f for f in folder.glob('*.xls*') if not f.name.startswith("~")]

    for file in files:
        logging.info(f"Processando arquivo: {file.name}")
        materials = ExcelUtils.read_materials(file)
        
        if not materials:
            continue
            
        # 1. Criar Requisição
        if sap.create_requisition_zmm0030(materials):
            # 2. Encontrar número (usa o primeiro material como referência)
            req_num = sap.find_requisition_number(materials[0])
            if req_num:
                # 3. Ajustar e Aprovar
                sap.adjust_requisition_me53n(req_num)
            else:
                logging.warning("Requisição criada mas número não encontrado para ajuste.")


# Process files for all responsaveis
def workflow_process_all_responsaveis(df: pd.DataFrame):
    responsaveis = df['Responsavel'].unique()
    df['Numero_Requisicao'] = None
    sap = SapManager()

    for resp in responsaveis:
        if resp != "PEDROHVB": continue
        logging.info(f"Processando responsável: {resp}")
        df_resp = df[df['Responsavel'] == resp]
        folder_path = Path(DATA_FOLDER) / datetime.now().strftime("%Y-%m") / "output" / resp / "grupos" / "ZSTK"
        folder_path.mkdir(parents=True, exist_ok=True)
        files = [f for f in folder_path.glob('*.xls*') if not f.name.startswith("~")]
        print(f"Arquivos encontrados para {resp}: {[f.name for f in files]}")
        report_data = []
        for file in files:
            logging.info(f"Processando arquivo: {file.name} para {resp}")
            #input("\n\nPressione para continuar...") # Commented out for automation flow
            
            materials = ExcelUtils.read_materials(file)
            tributacao = file.stem.split('_')[-1] 
            
            if not materials:
                continue
            
            # 2. Convert materials from file to string/strip to match DataFrame
            materials = [str(m).strip() for m in materials]

            # Create and Find Requisition
            sap.create_requisition_zmm0030(materials)
            req_num = sap.find_requisition_number(materials[0]) # Assuming first material is reliable anchor
            
            if req_num:
                sap.adjust_requisition_me53n(req_num, tributacao)
                
                # solicitar assinatura cpv

                # 3. UPDATE LOGIC
                # We iterate through materials to ensure we only update the specific ones in this file
                # We also check 'Responsavel' to ensure we don't update a different user's row
                for mat in materials:
                    condition = (df['Codigo_Material'] == mat) & (df['Responsavel'] == resp)
                    df.loc[condition, 'Numero_Requisicao'] = req_num
                    
                logging.info(f"Req {req_num} atribuída aos materiais: {materials}")
            else:
                logging.warning(f"Requisição criada mas número não encontrado para o arquivo {file.name}.")

        # 4. SAVE EXCEL (Moved outside the 'files' loop, but inside 'resp' loop)
        # This ensures we save once per Responsible, containing all their files processed so far
        df_reqs_criadas = df[df['Responsavel'] == resp][['Codigo_Material', 'Numero_Requisicao', 'Responsavel', 'pos_analise']]
        df_reqs_criadas.to_excel(Path(DATA_FOLDER) / datetime.now().strftime("%Y-%m") / "output" / resp / "grupos" / "Requisicoes_Criadas.xlsx", index=False)

    # 5. RETURN (Moved outside the 'resp' loop)
    # This ensures the function processes ALL responsaveis, not just the first one
    return df_reqs_criadas

