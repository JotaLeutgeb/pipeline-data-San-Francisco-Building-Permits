# src/config_schema.py

from typing import List, Dict
from pydantic import BaseModel, Field, validator

class NullHandlingConfig(BaseModel):
    """Esquema para las configuraciones de manejo de nulos."""
    drop_rows_if_null: List[str] = Field(default_factory=list, description="Columnas donde nulos implican eliminación de fila")
    impute_with_category: Dict[str, str] = Field(default_factory=dict, description="Columnas a imputar con categoría fija")
    impute_with_median: List[str] = Field(default_factory=list, description="Columnas a imputar con mediana")
    impute_with_mode: List[str] = Field(default_factory=list, description="Columnas a imputar con moda")

    @validator('drop_rows_if_null', 'impute_with_median', 'impute_with_mode')
    def validate_column_lists(cls, v):
        if not isinstance(v, list):
            raise ValueError("Debe ser una lista de nombres de columna")
        return v

    @validator('impute_with_category')
    def validate_impute_dict(cls, v):
        if not isinstance(v, dict):
            raise ValueError("Debe ser un diccionario de columna -> valor")
        return v

class CleaningConfig(BaseModel):
    """Esquema para todas las configuraciones de limpieza."""
    cols_to_drop: List[str] = Field(description="Columnas a eliminar del dataset")
    date_cols: List[str] = Field(description="Columnas que contienen fechas para conversión")
    null_handling_config: NullHandlingConfig = Field(description="Configuración para manejo de valores nulos")

    @validator('cols_to_drop', 'date_cols')
    def validate_column_lists(cls, v):
        if not isinstance(v, list):
            raise ValueError("Debe ser una lista de nombres de columna")
        return v

class MainConfig(BaseModel):
    """Esquema principal que representa todo el archivo config.yml."""
    data_path: str = Field(description="Ruta al archivo de datos de entrada")
    processed_data_path: str = Field(description="Ruta donde guardar los datos procesados")
    cleaning_config: CleaningConfig = Field(description="Configuración de limpieza de datos")

    @validator('data_path', 'processed_data_path')
    def validate_paths(cls, v):
        if not isinstance(v, str) or not v.strip():
            raise ValueError("Las rutas deben ser strings no vacíos")
        return v

    class Config:
        """Configuración de Pydantic."""
        validate_assignment = True
        extra = "forbid"  # No permite campos extra en la configuración