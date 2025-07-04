# configs/config.py

from pathlib import Path

# --- Rutas del Proyecto (Esto está bien como está) ---
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
SRC_DIR = ROOT_DIR / "src"

# --- CONFIGURACIÓN CENTRALIZADA DEL PIPELINE ---
# Un solo diccionario que contiene toda la configuración necesaria.
# Es más fácil de pasar a través del pipeline.
PIPELINE_CONFIG = {
    # Ruta al archivo de datos de entrada
    "data_path": RAW_DATA_DIR / "building-permits.csv", # Asegúrate que el nombre del archivo es correcto

    # Columnas para eliminar
    "cols_to_drop": [
        "Street Number Suffix", 
        "Unit Suffix",
        "Site Permit"
        # Agrega aquí otras columnas que quieras eliminar
    ],

    # Columnas para convertir a formato de fecha
    "date_cols": [
        "Permit Creation Date",
        "Current Status Date",
        "Issued Date",
        "Filed Date"
    ],

    # Estrategias para el manejo de valores nulos
    "NULL_HANDLING_CONFIG": {
        # Eliminar filas si estas columnas clave son nulas
        "drop_rows_if_null": ["Record ID"],

        # Imputar como una nueva categoría (el nulo tiene significado)
        "impute_as_category": {
            "Completed Date": "Ongoing",
            "Expiration Date": "Does Not Expire"
        },

        # Imputar con la mediana (para variables numéricas robustas a outliers)
        "impute_with_median": [
            "Estimated Cost",
            "Revised Cost",
            "Existing Units",
            "Proposed Units"
        ],

        # Imputar con la moda (para variables categóricas)
        "impute_with_mode": [
            "Permit Type Definition",
            "Street Suffix",
            "Existing Construction Type Description",
            "Proposed Construction Type Description",
            "Supervisor District",
            "Zipcode"
        ]
    }
}