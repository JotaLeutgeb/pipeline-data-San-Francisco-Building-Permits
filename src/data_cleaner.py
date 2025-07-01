# src/data_cleaner.py

import logging
import pandas as pd
from typing import List, Dict
from src.data_processing import DataProcessor

logger = logging.getLogger(__name__)

class DataCleaner:
    def __init__(self, df: pd.DataFrame):
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Se esperaba un DataFrame de pandas.")
        self.df = df.copy()
        self.processor = DataProcessor()

    def _standardize_column_names(self):
        """Estandariza los nombres de columna en self.df."""
        logger.info("Estandarizando nombres de columnas.")
        original_cols = self.df.columns
        cleaned_cols = [''.join(filter(str.isalnum, col.lower().replace(' ', '_'))) for col in original_cols]
        
        counts = {name: cleaned_cols.count(name) for name in cleaned_cols}
        
        final_cols = []
        suffix_counts: Dict[str, int] = {}
        for col_name in cleaned_cols:
            if counts[col_name] > 1:
                current_suffix = suffix_counts.get(col_name, 0)
                final_cols.append(f"{col_name}_{current_suffix}")
                suffix_counts[col_name] = current_suffix + 1
            else:
                final_cols.append(col_name)
        
        self.df.columns = final_cols
        logger.info(f"Nombres de columnas estandarizados: {self.df.columns.tolist()}")

    def _remove_duplicates(self):
        """Elimina duplicados usando el DataProcessor en self.df."""
        logger.info("Invocando el paso de eliminación de duplicados.")
        self.df = self.processor.remove_duplicates(self.df)

    def _impute_missing_values(self):
        """Imputa valores faltantes en self.df."""
        logger.info("Imputando valores faltantes.")
        # Imputar 'Fire Only Permit'
        if 'fireonlypermit' in self.df.columns:
            self.df['fireonlypermit'] = self.df['fireonlypermit'].fillna('N')
        
        # Imputar 'Street Suffix'
        if 'streetsuffix' in self.df.columns and self.df['streetsuffix'].isnull().any():
            mode_suffix = self.df['streetsuffix'].mode()[0]
            self.df['streetsuffix'] = self.df['streetsuffix'].fillna(mode_suffix)

    def _drop_unnecessary_columns(self, config: dict):
        """Elimina columnas innecesarias de self.df."""
        cols_to_drop = config.get('cols_to_drop', [])
        # Asegurarse de que las columnas a eliminar existan en el DataFrame
        cols_that_exist = [col for col in cols_to_drop if col in self.df.columns]
        if cols_that_exist:
            logger.info(f"Eliminando {len(cols_that_exist)} columnas innecesarias.")
            self.df = self.df.drop(columns=cols_that_exist)

    def _convert_to_datetime(self, config: dict):
        """Convierte columnas de fecha en self.df."""
        date_cols = config.get('date_cols', [])
        logger.info(f"Convirtiendo columnas a datetime: {date_cols}")
        for col in date_cols:
            if col in self.df.columns:
                # El manejo de fechas ya se realiza en los datos de prueba
                self.df[col] = pd.to_datetime(self.df[col])


    def run_cleaning_pipeline(self, config: dict) -> pd.DataFrame:
        """
        Ejecuta el pipeline de limpieza completo en el orden correcto.
        """
        logger.info("Iniciando pipeline de limpieza de datos.")
        
        # Secuencia de limpieza:
        self._standardize_column_names()
        self._remove_duplicates()
        self._impute_missing_values()
        self._convert_to_datetime(config)
        self._drop_unnecessary_columns(config) # Dropping at the end
        
        logger.info("Pipeline de limpieza completado exitosamente.")
        return self.df
