from pathlib import Path
import yaml

# --- 1. Define the Project Root Directory ---
# This robustly finds the project's root folder by going up two levels
# from the current file's location (/configs/config.py).
# Path(__file__).parent -> /configs
# Path(__file__).parent.parent -> / (the project root)
ROOT_DIR = Path(__file__).resolve().parent.parent

# --- 2. Build Other Important Paths from the Root ---
# Using the "/" operator from pathlib makes these paths work on any OS (Windows, Mac, Linux).
DATA_DIR = ROOT_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
SOURCE_DATA_DIR = DATA_DIR / "source"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

SRC_DIR = ROOT_DIR / "src"
TESTS_DIR = ROOT_DIR / "tests"
CONFIGS_DIR = ROOT_DIR / "configs"
NOTEBOOKS_DIR = ROOT_DIR / "notebooks"
CONFIG_DIR = 'configs/cleaning_config.yaml'

# Cargar la configuración de limpieza desde el archivo YAML
with open(CONFIG_DIR, 'r') as f:
    CONFIG_DIR = yaml.safe_load(f)

# --- INICIO DE LA NUEVA SECCIÓN ---
# Añadimos las reglas de imputación, que ahora controlan el proceso
IMPUTATION_RULES = {
    "fireonlypermit": {"strategy": "constant", "value": "N"},
    "streetsuffix":   {"strategy": "mode"},
    # Ejemplo para demostrar escalabilidad, aunque 'zipcode' no tenga nulos ahora
    "zipcode":        {"strategy": "median"} 
}

# El config principal ahora incluye las reglas de imputación
CONFIG_DIR['imputation_rules'] = IMPUTATION_RULES
# --- FIN DE LA NUEVA SECCIÓN ---

# --- Optional: Print paths for verification during development ---
# You can uncomment this block and run `python configs/config.py` to check paths.
#if __name__ == '__main__':
#     print(f"Project Root Directory: {ROOT_DIR}")
#     print(f"Source Data Directory: {SOURCE_DATA_DIR}")
#     print(f"Processed Data Directory: {PROCESSED_DATA_DIR}")
#     print(f"Source Code Directory: {SRC_DIR}")

