from pathlib import Path
from datetime import datetime

# Diretórios base
BASE_DIR = Path(__file__).parent.parent
DATA_FOLDER = BASE_DIR / "data"
MONTH_FOLDER = datetime.now().strftime("%Y-%m")
INPUT_FOLDER = DATA_FOLDER / MONTH_FOLDER / "input"
OUTPUT_FOLDER = DATA_FOLDER / MONTH_FOLDER / "output"
TEMPLATES_FOLDER = BASE_DIR / "templates"

# Criar diretórios se não existirem
AD_TEMPLATE_DIR = TEMPLATES_FOLDER / "AD"

INPUT_FOLDER.mkdir(parents=True, exist_ok=True)
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
