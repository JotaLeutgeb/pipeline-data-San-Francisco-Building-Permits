# src/data_cleaner.py

import logging
import pandas as pd
from typing import Dict
from collections import defaultdict
from src.data_processing import DataProcessor

logger = logging.getLogger(__name__)

class DataCleaner:
    def __init__(self, df: pd.DataFrame):
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Se esperaba un DataFrame de pandas.")
        self.df = df.copy()
        self.processor = DataProcessor()

    def _standardize_column_names(self):
        """
        Estandariza los nombres de las columnas de forma determinista.

        La estrategia es inmune al orden de las columnas de entrada:
        1.  Limpia todos los nombres de columna (minúsculas, sin espacios, etc.).
        2.  Agrupa los nombres de columna originales por su versión limpia.
        3.  Para los grupos con múltiples columnas, asigna sufijos (_0, _1)
            basándose en el orden alfabético de los nombres originales.
        4.  Finalmente, reordena las columnas del DataFrame según el orden
            alfabético de los nombres originales para garantizar una salida canónica.
        """
        logger.info("Estandarizando nombres de columnas de forma determinista.")
        
        original_cols = self.df.columns.tolist()
        
        # Mapa de nombre limpio a lista de nombres originales que lo producen
        cleaned_to_originals = defaultdict(list)
        for col in original_cols:
            cleaned_name = ''.join(filter(str.isalnum, col.lower().replace(' ', '_')))
            cleaned_to_originals[cleaned_name].append(col)

        # Crear el mapa de renombrado final
        renaming_map: Dict[str, str] = {}
        for cleaned_name, original_group in cleaned_to_originals.items():
            if len(original_group) == 1:
                renaming_map[original_group[0]] = cleaned_name
            else:
                # Ordenar alfabéticamente para un sufijo determinista
                original_group.sort()
                for i, original_col in enumerate(original_group):
                    renaming_map[original_col] = f"{cleaned_name}_{i}"
        
        # Aplicar el renombrado
        self.df = self.df.rename(columns=renaming_map)
        
        # Ordenar las columnas del DataFrame de forma canónica
        final_ordered_cols = sorted(self.df.columns)
        self.df = self.df[final_ordered_cols]
        
        logger.info(f"Nombres de columnas estandarizados y ordenados: {self.df.columns.tolist()}")

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
        Ejecuta el pipeline de limpieza completo en un orden definido y justificado.

        Procedimiento del Pipeline:
        1.  Estandarizar Nombres (_standardize_column_names):
            - Justificación: Garantiza un acceso fiable y predecible a las
              columnas en todos los pasos posteriores. Es un prerrequisito.
        2.  Eliminar Duplicados (_remove_duplicates):
            - Justificación: Evita procesar datos redundantes, mejorando el
              rendimiento y la precisión de los análisis futuros.
        3.  Imputar Valores Faltantes (_impute_missing_values):
            - Justificación: Asegura que los modelos o cálculos posteriores no
              fallen debido a valores nulos.
        4.  Convertir a Datetime (_convert_to_datetime):
            - Justificación: Permite realizar operaciones de series temporales
              y cálculos basados en fechas.
        5.  Eliminar Columnas Innecesarias (_drop_unnecessary_columns):
            - Justificación: Reduce el uso de memoria y simplifica el conjunto
              de datos final, eliminando ruido. Se hace al final para poder
              usar cualquier columna si fuera necesario en pasos intermedios.
        """
        logger.info("Iniciando pipeline de limpieza de datos.")
        
        self._standardize_column_names()
        self._remove_duplicates()
        self._impute_missing_values()
        self._convert_to_datetime(config)
        self._drop_unnecessary_columns(config)
        
        logger.info("Pipeline de limpieza completado exitosamente.")
        return self.df

