# src/mi_paquete/config_schema.py

from typing import List, Dict
from pydantic import BaseModel

class NullHandlingConfig(BaseModel):
    """Esquema para las configuraciones de manejo de nulos."""
    drop_rows_if_null: List[str] = []
    impute_with_category: Dict[str, str] = {} 
    impute_with_median: List[str] = []
    impute_with_mode: List[str] = []

class CleaningConfig(BaseModel):
    """Esquema para todas las configuraciones de limpieza."""
    cols_to_drop: List[str]
    date_cols: List[str]
    null_handling_config: NullHandlingConfig

class MainConfig(BaseModel):
    """Esquema principal que representa todo el archivo config.yml."""
    data_path: str
    processed_data_path: str
    cleaning_config: CleaningConfig