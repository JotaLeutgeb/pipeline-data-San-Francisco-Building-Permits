from pathlib import Path
import yaml

# -----------------------------------------------------------------------------
# CONFIGURACIÓN DEL PROYECTO
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# CONFIGURACIÓN PARA EL MANEJO DE VALORES NULOS
# -----------------------------------------------------------------------------
# Define las estrategias para imputar o eliminar valores nulos en columnas específicas.
# El pipeline aplicará estas reglas en el orden definido.
# -----------------------------------------------------------------------------
NULL_HANDLING_CONFIG = {
    # Estrategia 1: Eliminar filas si estas columnas clave son nulas.
    # Justificación: Un registro sin un identificador único es inútil.
    "drop_rows_if_null": [
        "Record ID"
        ],

    # Estrategia 2: Imputar como una categoría específica.
    # Justificación: El nulo es informativo (MNAR). Un permiso sin fecha de
    # finalización significa que sigue "En Curso".
    "impute_as_category": {
        "Completed Date": "Ongoing",
        "Expiration Date": "Does Not Expire"
    },

    # Estrategia 3: Imputar con la mediana (para variables numéricas).
    # Justificación: La mediana es robusta a valores extremos (outliers).
    # Evita que un permiso de construcción de un rascacielos distorsione la media.
    "impute_with_median": [
        "Estimated Cost",
        "Revised Cost",
        "Existing Units",
        "Proposed Units"
    ],

    # Estrategia 4: Imputar con la moda (para variables categóricas).
    # Justificación: Usar el valor más frecuente es una suposición segura
    # para datos categóricos cuando la ausencia parece aleatoria (MAR).
    "impute_with_mode": [
        "Permit Type Definition",
        "Street Suffix",
        "Existing Construction Type Description",
        "Proposed Construction Type Description",
        "Supervisor District",
        "Zipcode"
    ]
}
