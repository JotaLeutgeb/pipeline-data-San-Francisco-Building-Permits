# src/data_cleaner.py

import logging
from typing import Dict, Any, List, Union
from collections import defaultdict
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType

logger = logging.getLogger(__name__)

class DataCleaner:
    def __init__(self, config: Dict[str, Any]):
        if not isinstance(config, dict):
            raise TypeError("La configuración debe ser un diccionario.")
        self.config = config
        self.report = {}

    def _create_name_map(self, original_cols: List[str]) -> Dict[str, str]:
        """
        NEW: Crea un mapa de nombres de columna originales a estandarizados, manejando colisiones.
        Ejemplo: { "Permit Type": "permittype", "Permit type": "permittype_1" }
        """
        renaming_map = {}
        cleaned_to_originals = defaultdict(list)
        
        for col in original_cols:
            cleaned_name = ''.join(filter(str.isalnum, col.lower().replace(' ', '_')))
            cleaned_to_originals[cleaned_name].append(col)

        for cleaned_name, original_group in cleaned_to_originals.items():
            if len(original_group) == 1:
                renaming_map[original_group[0]] = cleaned_name
            else:
                original_group.sort()
                for i, original_col in enumerate(original_group):
                    renaming_map[original_col] = f"{cleaned_name}_{i}"
        return renaming_map

    def _translate_config(self, config_item: Union[Dict, List, Any], name_map: Dict[str, str]) -> Union[Dict, List, Any]:
        """
        NEW: Recorre recursivamente la configuración del usuario y la traduce usando el name_map.
        Reemplaza tanto claves como valores que correspondan a nombres de columnas.
        """
        if isinstance(config_item, dict):
            new_dict = {}
            for key, value in config_item.items():
                translated_key = name_map.get(key, key)
                translated_value = self._translate_config(value, name_map)
                new_dict[translated_key] = translated_value
            return new_dict
        elif isinstance(config_item, list):
            return [self._translate_config(item, name_map) for item in config_item]
        elif isinstance(config_item, str):
            return name_map.get(config_item, config_item)
        else:
            return config_item

    def _drop_unnecessary_columns(self, df: DataFrame, internal_config: Dict) -> DataFrame:
        """REFACTORED: Ahora usa la configuración interna con nombres limpios."""
        cols_to_drop = internal_config.get('cols_to_drop', [])
        if cols_to_drop:
            df = df.drop(*cols_to_drop)
        return df

    def _convert_to_datetime(self, df: DataFrame, internal_config: Dict) -> DataFrame:
        """REFACTORED: Ahora usa la configuración interna con nombres limpios."""
        date_cols = internal_config.get('date_cols', [])
        for col in date_cols:
            if col in df.columns:
                df = df.withColumn(col, F.to_timestamp(F.col(col)))
        return df

    def handle_null_values(self, df: DataFrame, internal_config: Dict) -> DataFrame:
        """REFACTORED: Ahora usa la configuración interna con nombres limpios."""
        null_config = internal_config.get("null_handling_config", {})
        if not null_config:
            return df

        df = df.dropna(subset=null_config.get("drop_rows_if_null", []))

        imputations = {}
        imputations.update(null_config.get("impute_as_category", {}))

        for col_name in null_config.get("impute_with_median", []):
            if col_name in df.columns:
                median_val = df.approxQuantile(col_name, [0.5], 0.01)[0]
                if median_val is not None:
                    imputations[col_name] = median_val
        
        for col_name in null_config.get("impute_with_mode", []):
            if col_name in df.columns:
                mode_row = df.groupBy(col_name).count().orderBy(F.col("count").desc()).first()
                if mode_row:
                    imputations[col_name] = mode_row[0]
        
        if imputations:
            df = df.fillna(imputations)
        return df

    def _remove_duplicates(self, df: DataFrame) -> DataFrame:
        """Elimina filas duplicadas (sin cambios, opera sobre filas enteras)."""
        return df.distinct()

    def run_cleaning_pipeline(self, df: DataFrame) -> DataFrame:
        """
        REFACTORED: Implementa la nueva arquitectura robusta.
        """
        logger.info("Iniciando pipeline de limpieza con arquitectura robusta.")
        initial_rows = df.count()
        self.report['initial_rows'] = initial_rows

        # 1. Generar el mapa de nombres
        name_map = self._create_name_map(df.columns)

        # 2. Traducir la configuración del usuario a una configuración interna y estandarizada
        internal_config = self._translate_config(self.config, name_map)

        # 3. Estandarizar el DataFrame para que use la nomenclatura interna
        select_exprs = [F.col(f"`{k}`").alias(v) for k, v in name_map.items()]
        df_cleaned = df.select(select_exprs)
        
        # 4. Ejecutar el resto de los pasos usando el DataFrame y la configuración estandarizados
        df_cleaned = self._remove_duplicates(df_cleaned)
        df_cleaned = self._drop_unnecessary_columns(df_cleaned, internal_config)
        df_cleaned = self._convert_to_datetime(df_cleaned, internal_config)
        df_cleaned = self.handle_null_values(df_cleaned, internal_config)
        
        # 5. Ordenar columnas para un output consistente
        df_cleaned = df_cleaned.select(sorted(df_cleaned.columns))

        final_rows = df_cleaned.count()
        self.report['final_rows'] = final_rows
        self.report['rows_dropped'] = initial_rows - final_rows

        logger.info("Pipeline de limpieza en PySpark completado.")
        return df_cleaned

    def get_report(self) -> Dict[str, Any]:
        return self.report