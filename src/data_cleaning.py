import pandas as pd
from unidecode import unidecode
import re

# Importar la configuración de directorios
from configs.config import RAW_DATA_DIR, PROCESSED_DATA_DIR


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes column names by converting to ASCII, lowercasing, 
    and replacing non-alphanumeric chars with a single underscore.
    """
    new_columns = []
    for col in df.columns:
        # 1. Transliterar a ASCII (e.g., 'Año' -> 'Ano')
        s = unidecode(col)
        # 2. Convertir a minúsculas
        s = s.lower()
        # 3. Reemplazar cualquier caracter no alfanumérico por un espacio
        s = re.sub(r'[^a-z0-9]+', ' ', s)
        # 4. Reemplazar uno o más espacios por un solo guion bajo
        s = s.strip().replace(' ', '_')
        new_columns.append(s)
    
    df.columns = new_columns

    # Opcional pero recomendado: Manejar columnas duplicadas después de estandarizar
    # Aquí puedes implementar una lógica para renombrar duplicados, como agregar '_1', '_2', etc.
    # Por ahora, la dejamos así para mostrar la limpieza.

    return df
