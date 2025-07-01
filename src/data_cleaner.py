import logging
import pandas as pd
from typing import Dict
from collections import defaultdict
from src.data_processing import DataProcessor

logger = logging.getLogger(__name__)

class DataCleaner:
    # __init__ and other methods remain unchanged
    def __init__(self, df: pd.DataFrame):
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Se esperaba un DataFrame de pandas.")
        self.df = df.copy()
        self.processor = DataProcessor()

    def _standardize_column_names(self):
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
                original_group.sort()
                for i, original_col in enumerate(original_group):
                    renaming_map[original_col] = f"{cleaned_name}_{i}"
        self.df = self.df.rename(columns=renaming_map)
        final_ordered_cols = sorted(self.df.columns)
        self.df = self.df[final_ordered_cols]
        logger.info(f"Nombres de columnas estandarizados y ordenados: {self.df.columns.tolist()}")

    def _remove_duplicates(self):
        logger.info("Invocando el paso de eliminación de duplicados.")
        self.df = self.processor.remove_duplicates(self.df)

    def _impute_missing_values(self, config: dict):
        """
        Imputa valores faltantes de forma configurable y segura, evitando 'inplace=True'.
        """
        logger.info("Iniciando imputación configurable de valores faltantes.")
        imputation_rules = config.get('imputation_rules', {})

        for column, rule in imputation_rules.items():
            if column not in self.df.columns:
                logger.warning(f"La columna de imputación '{column}' no se encuentra. Omitiendo.")
                continue

            strategy = rule.get('strategy')
            logger.info(f"Aplicando estrategia '{strategy}' a la columna '{column}'.")

            if strategy == 'constant':
                value = rule.get('value')
                # FIX: Use direct assignment instead of inplace=True
                self.df[column] = self.df[column].fillna(value)
            elif strategy == 'mode':
                mode_value = self.df[column].mode()[0]
                # FIX: Use direct assignment instead of inplace=True
                self.df[column] = self.df[column].fillna(mode_value)
            elif strategy == 'mean':
                mean_value = self.df[column].mean()
                # FIX: Use direct assignment instead of inplace=True
                self.df[column] = self.df[column].fillna(mean_value)
            elif strategy == 'median':
                median_value = self.df[column].median()
                # FIX: Use direct assignment instead of inplace=True
                self.df[column] = self.df[column].fillna(median_value)
            else:
                logger.warning(f"Estrategia de imputación '{strategy}' no reconocida. Omitiendo.")

    def _convert_to_datetime(self, config: dict):
        date_cols = config.get('date_cols', [])
        logger.info(f"Convirtiendo columnas a datetime: {date_cols}")
        for col in date_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')

    def _drop_unnecessary_columns(self, config: dict):
        cols_to_drop = config.get('cols_to_drop', [])
        cols_that_exist = [col for col in cols_to_drop if col in self.df.columns]
        if cols_that_exist:
            logger.info(f"Eliminando {len(cols_that_exist)} columnas innecesarias.")
            self.df = self.df.drop(columns=cols_that_exist)

    def run_cleaning_pipeline(self, config: dict) -> pd.DataFrame:
        """
        Ejecuta el pipeline de limpieza completo en un orden definido y justificado.
        """
        logger.info("Iniciando pipeline de limpieza de datos.")

        self._standardize_column_names()
        self._remove_duplicates()
        self._impute_missing_values(config)
        self._convert_to_datetime(config)
        self._drop_unnecessary_columns(config)

        logger.info("Pipeline de limpieza completado exitosamente.")
        return self.df
