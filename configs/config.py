import os
from pathlib import Path
from dotenv import load_dotenv

# Carga las variables del archivo .env en el entorno
load_dotenv()

# Ahora podés acceder a ellas de forma segura
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
API_KEY = os.getenv("API_KEY")

# 1. Definir la ruta raíz del proyecto.
ROOT_DIR = Path(__file__).resolve().parent.parent

# 2. Construir otras rutas importantes a partir de la ruta raíz.
# La sintaxis con "/" es la magia de pathlib, funciona en Windows, Mac y Linux.
DATA_DIR = ROOT_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
SRC_DIR = ROOT_DIR / "src"
NOTEBOOKS_DIR = ROOT_DIR / "notebooks"