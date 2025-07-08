# configs/config.py

from pathlib import Path

# --- Rutas del Proyecto ---
ROOT_PATH = Path().resolve().parent
DATA_PATH = ROOT_PATH / "data"
RAW_DATA_PATH= DATA_PATH / "raw"
PROCESSED_DATA_PATH = DATA_PATH / "processed"
SRC_PATH = ROOT_PATH / "src"