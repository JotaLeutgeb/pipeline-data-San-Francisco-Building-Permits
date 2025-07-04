# src/data_cleaner.py

import logging
import pandas as pd
from typing import Dict, Any
from collections import defaultdict
from src.data_processing import DataProcessor # Asumo que este archivo existe y funciona

logger = logging.getLogger(__name__)

class DataCleaner:
    """
    Una clase robusta para limpiar un DataFrame de pandas basada en un 
    diccionario de configuración.
    """
    
    def __init__(self, df: pd.DataFrame, config: Dict[str, Any]):
        """
        CAMBIO: Incorporamos una Inyección de Dependencias y hace la clase autocontenida y fácil de probar.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Se esperaba un DataFrame de pandas.")
        if not isinstance(config, dict):
            raise TypeError("La configuración debe ser un diccionario.")
            
        self.df = df.copy() # Copiamos para evitar efectos secundarios (Side Effects)
        self.config = config
        self.processor = DataProcessor()
        self.report = {}

    def _standardize_column_names(self):
        # Tu método de estandarización es robusto y determinista. ¡Lo mantenemos!
        logger.info("Estandarizando nombres de columnas de forma determinista.")
        original_cols = self.df.columns.tolist()
        cleaned_to_originals = defaultdict(list)
        for col in original_cols:
            cleaned_name = ''.join(filter(str.isalnum, col.lower().replace(' ', '_')))
            cleaned_to_originals[cleaned_name].append(col)
        
        renaming_map: Dict[str, str] = {}
        for cleaned_name, original_group in cleaned_to_originals.items():
            if len(original_group) == 1:
                renaming_map[original_group[0]] = cleaned_name
            else:
                original_group.sort() # Ordenar asegura determinismo
                for i, original_col in enumerate(original_group):
                    renaming_map[original_col] = f"{cleaned_name}_{i}"
        
        self.df.rename(columns=renaming_map, inplace=True)
        # Reordenar columnas alfabéticamente para un output consistente
        self.df = self.df[sorted(self.df.columns)]
        logger.info(f"Columnas estandarizadas y ordenadas: {self.df.columns.tolist()}")

    def _remove_duplicates(self):
        logger.info("Invocando el paso de eliminación de duplicados.")
        # Asumiendo que DataProcessor.remove_duplicates existe y funciona
        self.df = self.processor.remove_duplicates(self.df)

    def _convert_to_datetime(self):
        date_cols = self.config.get('date_cols', [])
        logger.info(f"Convirtiendo columnas a datetime: {date_cols}")
        for col in date_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')

    def _drop_unnecessary_columns(self):
        cols_to_drop = self.config.get('cols_to_drop', [])
        cols_that_exist = [col for col in cols_to_drop if col in self.df.columns]
        if cols_that_exist:
            logger.info(f"Eliminando {len(cols_that_exist)} columnas innecesarias.")
            self.df.drop(columns=cols_that_exist, inplace=True)

    def handle_null_values(self):
        """
        CAMBIO: Se elimina el uso de `inplace=True` para seguir las 
        mejores prácticas de pandas y eliminar los FutureWarnings.
        """
        logger.info("Iniciando manejo de valores nulos basado en configuración.")
        null_config = self.config.get("NULL_HANDLING_CONFIG", {})
        if not null_config:
            logger.warning("No se encontró 'NULL_HANDLING_CONFIG'. Saltando el manejo de nulos.")
            return

        # Estrategia 1: Eliminar filas
        for col in null_config.get("drop_rows_if_null", []):
            if col in self.df.columns:
                # ANTES: self.df.dropna(subset=[col], inplace=True)
                self.df = self.df.dropna(subset=[col])

        # Estrategia 2: Imputar como categoría
        for col, fill_value in null_config.get("impute_as_category", {}).items():
            if col in self.df.columns:
                # ANTES: self.df[col].fillna(fill_value, inplace=True)
                self.df[col] = self.df[col].fillna(fill_value)

        # Estrategia 3: Imputar con la mediana
        for col in null_config.get("impute_with_median", []):
            if col in self.df.columns:
                median_value = self.df[col].median()
                # ANTES: self.df[col].fillna(median_value, inplace=True)
                self.df[col] = self.df[col].fillna(median_value)

        # Estrategia 4: Imputar con la moda
        for col in null_config.get("impute_with_mode", []):
            if col in self.df.columns and self.df[col].isnull().any():
                mode_value = self.df[col].mode()[0]
                # ANTES: self.df[col].fillna(mode_value, inplace=True)
                self.df[col] = self.df[col].fillna(mode_value)
                
        logger.info("Manejo de valores nulos completado.")

    def run_cleaning_pipeline(self) -> pd.DataFrame:
        """
        CAMBIO: Orquesta la limpieza usando la configuración interna (`self.config`).
        No necesita recibir ningún argumento.
        """
        logger.info("Iniciando pipeline de limpieza de datos.")
        initial_rows = len(self.df)
        self.report['initial_rows'] = initial_rows

        # El orden es importante:
        # 1. Duplicados primero para reducir datos.
        self._remove_duplicates() 
        # 2. Estandarizar nombres para que los pasos siguientes usen nombres consistentes.
        #    ¡OJO! Si tu config usa los nombres originales, este paso debe ir DESPUÉS.
        #    Lo dejaré al final para mayor seguridad.
        
        # 3. Conversión de tipos y eliminación de columnas.
        self._convert_to_datetime()
        self._drop_unnecessary_columns()

        # 4. Manejar nulos con las columnas y tipos correctos.
        self.handle_null_values()

        # 5. Estandarizar nombres al final.
        #    Esto asegura que la configuración (que usa nombres originales) funcione.
        self._standardize_column_names()

        final_rows = len(self.df)
        self.report['final_rows'] = final_rows
        self.report['rows_dropped'] = initial_rows - final_rows


        logger.info("Pipeline de limpieza completado exitosamente.")
        return self.df
    
def get_report(self):
        return self.report