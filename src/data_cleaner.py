# src/data_cleaner.py

import logging
from typing import Dict, List, Any, Tuple
from collections import defaultdict
from pathlib import Path
import yaml
import re

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DateType
from datetime import datetime as py_datetime

from pydantic import ValidationError
from .config_schema import MainConfig
from .cleaning_report import CleaningReport

logger = logging.getLogger(__name__)


# Función auxiliar para parsear fechas de forma segura.
def _safe_parse_date(date_str: str) -> py_datetime.date:
    """
    Función auxiliar que intenta parsear un string de fecha con varios formatos.
    Devuelve un objeto de tipo date o None si la conversión falla.
    """
    if not isinstance(date_str, str):
        return None
    
    # Lista de formatos de fecha a intentar en orden de prioridad.
    formats_to_try = [
        "%Y-%m-%d",        # "2023-01-30"
        "%Y-%m-%dT%H:%M:%S",# "2023-01-30T10:00:00"
        "%m/%d/%Y",        # "01/30/2023"
        "%m/%d/%y",        # "01/30/23"
    ]
    
    for date_format in formats_to_try:
        try:
            # Intenta convertir el string usando el formato actual.
            return py_datetime.strptime(date_str, date_format).date()
        except (ValueError, TypeError):
            # Si falla, continúa con el siguiente formato.
            continue
            
    # Si ningún formato funcionó, devuelve None.
    return None

# Registrar la función de Python como una UDF de Spark.
safe_parse_date_udf = F.udf(_safe_parse_date, DateType())

class DataCleaner:
    """
    ETL profesional para limpieza de datos de permisos de construcción.
    Utiliza PySpark para escalabilidad y Pydantic para validación de configuración.
    """
    
    def __init__(self, config_path: str, spark_session: SparkSession = None):
        """
        Construye y valida la 'máquina de limpieza'.

        Args:
            config_path (str): Ruta al archivo de configuración YAML.
            spark_session (SparkSession, optional): Sesión de Spark. Si no se proporciona, se crea una nueva.
        
        Raises:
            ValidationError: Si la configuración no cumple con el esquema.
            FileNotFoundError: Si la ruta al config no existe.
        """
        self.config_path = Path(config_path)
        self.spark = spark_session or self._create_spark_session()
        self.config = self._load_and_validate_config()
        self.report = CleaningReport()
        
        logger.info(f"DataCleaner configurado exitosamente desde {config_path}")
    
    def _create_spark_session(self) -> SparkSession:
        """Crea una sesión de Spark con configuración optimizada."""
        return SparkSession.builder \
            .appName("SF_Building_Permits_ETL") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    
    def _load_and_validate_config(self) -> MainConfig:
        """Carga y valida la configuración usando Pydantic."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                raw_config = yaml.safe_load(f)
            
            # Validación estricta con Pydantic
            config = MainConfig.parse_obj(raw_config)
            logger.info("Configuración validada exitosamente")
            return config
            
        except FileNotFoundError:
            logger.error(f"Archivo de configuración no encontrado: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error al parsear YAML: {e}")
            raise
        except ValidationError as e:
            logger.error(f"Error de validación en configuración:")
            for error in e.errors():
                logger.error(f"  • {error['loc']}: {error['msg']}")
            raise
    
    def _validate_config_against_dataframe(self, df: DataFrame) -> None:
        """Valida que las columnas especificadas en config existan en el DataFrame."""
        logger.info("Validando configuración contra estructura del DataFrame...")
        
        df_columns = set(df.columns)
        config_columns = set()
        
        # Recolectar todas las columnas mencionadas en la configuración
        config_columns.update(self.config.cleaning_config.cols_to_drop)
        config_columns.update(self.config.cleaning_config.date_cols)
        config_columns.update(self.config.cleaning_config.null_handling_config.drop_rows_if_null)
        config_columns.update(self.config.cleaning_config.null_handling_config.impute_with_category.keys())
        config_columns.update(self.config.cleaning_config.null_handling_config.impute_with_median)
        config_columns.update(self.config.cleaning_config.null_handling_config.impute_with_mode)
        
        # Verificar columnas faltantes
        missing_columns = config_columns - df_columns
        if missing_columns:
            error_msg = f"Columnas especificadas en config pero no encontradas en DataFrame: {missing_columns}"
            logger.error(f"{error_msg}")
            raise ValueError(error_msg)
        
        logger.info("✅ Validación de configuración completada")
    
    def _create_column_name_map(self, original_cols: List[str]) -> Dict[str, str]:
        """
        Crea un mapa de nombres de columna originales a estandarizados.
        Maneja colisiones añadiendo sufijos numéricos.
        """
        logger.info("Creando mapa de estandarización de nombres de columna...")
        
        name_map = {}
        standardized_to_originals = defaultdict(list)
        
        for col in original_cols:
            normalized_name = col.lower()
            normalized_name = re.sub(r'[^\w]+', '_', normalized_name, flags=re.UNICODE)
            normalized_name = normalized_name.strip('_')

            # Si después de limpiar queda una cadena vacía (ej: "!!!"), usa un nombre por defecto
            if not normalized_name:
                normalized_name = "invalid_col_name"
            
            standardized_to_originals[normalized_name].append(col)
        
        # Resolver colisiones
        for standardized, originals in standardized_to_originals.items():
            if len(originals) == 1:
                name_map[originals[0]] = standardized
            else:
                # Ordenar para consistencia y agregar sufijos
                originals.sort()
                for i, original in enumerate(originals):
                    name_map[original] = f"{standardized}_{i}"
        
        logger.info(f"Mapa de nombres creado para {len(name_map)} columnas")
        return name_map
    
    def _standardize_column_names(self, df: DataFrame) -> Tuple[DataFrame, Dict[str, str]]:
        """Estandariza nombres de columna y retorna el DataFrame modificado y el mapa."""
        name_map = self._create_column_name_map(df.columns)
        
        # Aplicar renombrado
        for old_name, new_name in name_map.items():
            df = df.withColumnRenamed(old_name, new_name)
        
        return df, name_map
    
    def _translate_config_to_standardized_names(self, name_map: Dict[str, str]) -> Dict[str, Any]:
        """
        Traduce la configuración para usar nombres de columna estandarizados.
        """
        logger.info("🔄 Traduciendo configuración a nombres estandarizados...")
        
        def translate_item(item):
            if isinstance(item, dict):
                return {name_map.get(k, k): translate_item(v) for k, v in item.items()}
            elif isinstance(item, list):
                return [name_map.get(i, i) for i in item]
            elif isinstance(item, str):
                return name_map.get(item, item)
            else:
                return item
        
        # Convertir config Pydantic a dict y traducir
        config_dict = self.config.dict()
        translated_config = translate_item(config_dict)
        
        logger.info("✅ Configuración traducida")
        return translated_config
    
    def _drop_columns(self, df: DataFrame, cols_to_drop: List[str]) -> DataFrame:
        """Elimina columnas especificadas del DataFrame."""
        if not cols_to_drop:
            return df
        
        logger.info(f"🗑️  Eliminando {len(cols_to_drop)} columnas...")
        
        # Filtrar columnas que realmente existen
        existing_cols_to_drop = [col for col in cols_to_drop if col in df.columns]
        
        if existing_cols_to_drop:
            df = df.drop(*existing_cols_to_drop)
            self.report.column_drop_report.columns_dropped = existing_cols_to_drop
            logger.info(f"✅ Columnas eliminadas: {existing_cols_to_drop}")
        
        return df
    
    def _convert_date_columns(self, df: DataFrame, date_cols: List[str]) -> DataFrame:
        """Convierte columnas especificadas a tipo timestamp."""
        if not date_cols:
            return df
        
        logger.info(f"Convirtiendo {len(date_cols)} columnas de fecha...")
        
        for col in date_cols:
            if col in df.columns:
                initial_non_nulls = df.filter(F.col(col).isNotNull()).count()
                
                # CORRECCIÓN FINAL: Aplicamos nuestra UDF segura.
                df = df.withColumn(col, safe_parse_date_udf(F.col(col)))

                final_non_nulls = df.filter(F.col(col).isNotNull()).count()
                
                errors = initial_non_nulls - final_non_nulls
                successes = final_non_nulls

                self.report.date_conversion_report.add_conversion(col, successes=successes, errors=errors)
                logger.info(f"Columna '{col}' convertida a timestamp (Éxitos: {successes}, Errores: {errors})")
        
        return df
    
    def _handle_null_values(self, df: DataFrame, null_config: Dict[str, Any]) -> DataFrame:
        """Maneja valores nulos según la configuración especificada."""
        logger.info("🔧 Procesando valores nulos...")
        
        # 1. Eliminar filas con nulos en columnas críticas
        drop_cols = null_config.get('drop_rows_if_null', [])
        for col in drop_cols:
            if col in df.columns:
                initial_count = df.count()
                df = df.filter(F.col(col).isNotNull())
                final_count = df.count()
                rows_dropped = initial_count - final_count
                
                if rows_dropped > 0:
                    self.report.null_handling_report.add_null_drop(col, rows_dropped)
                    logger.info(f"✅ Eliminadas {rows_dropped} filas por nulos en '{col}'")
        
        # 2. Imputar con categorías fijas
        category_imputations = null_config.get('impute_with_category', {})
        for col, category in category_imputations.items():
            if col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                if null_count > 0:
                    df = df.fillna({col: category})
                    self.report.null_handling_report.add_imputation(col, 'category', category, null_count)
                    logger.info(f"✅ Imputados {null_count} nulos en '{col}' con '{category}'")
        
        # 3. Imputar con mediana
        median_cols = null_config.get('impute_with_median', [])
        for col in median_cols:
            if col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                if null_count > 0:
                    median_value = df.approxQuantile(col, [0.5], 0.01)[0]
                    if median_value is not None:
                        df = df.fillna({col: median_value})
                        self.report.null_handling_report.add_imputation(col, 'median', median_value, null_count)
                        logger.info(f"✅ Imputados {null_count} nulos en '{col}' con mediana {median_value}")
        
        # 4. Imputar con moda
        mode_cols = null_config.get('impute_with_mode', [])
        for col in mode_cols:
            if col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                if null_count > 0:
                    mode_row = df.groupBy(col).count().orderBy(F.col('count').desc()).first()
                    if mode_row and mode_row[0] is not None:
                        mode_value = mode_row[0]
                        df = df.fillna({col: mode_value})
                        self.report.null_handling_report.add_imputation(col, 'mode', mode_value, null_count)
                        logger.info(f"✅ Imputados {null_count} nulos en '{col}' con moda '{mode_value}'")
        
        return df
    
    def _remove_duplicates(self, df: DataFrame) -> DataFrame:
        """Elimina filas duplicadas del DataFrame."""
        logger.info("Eliminando duplicados...")
        
        initial_count = df.count()
        df_deduplicated = df.distinct()
        final_count = df_deduplicated.count()
        
        duplicates_removed = initial_count - final_count
        
        # CORRECCIÓN: Se actualizan los atributos correctos en el reporte.
        self.report.duplicate_report.duplicates_removed = duplicates_removed
        self.report.duplicate_report.unique_rows_kept = final_count
        
        if duplicates_removed > 0:
            logger.info(f"Eliminados {duplicates_removed} duplicados")
        else:
            logger.info("No se encontraron duplicados")
        
        return df_deduplicated
    
    def run_cleaning_pipeline(self, df: DataFrame) -> DataFrame:
        """
        Ejecuta el pipeline completo de limpieza de datos.
        
        Args:
            df (DataFrame): DataFrame de PySpark a limpiar
            
        Returns:
            DataFrame: DataFrame limpio
        """
        logger.info("Iniciando pipeline de limpieza profesional...")
        
        # Inicializar métricas
        self.report.initial_rows = df.count()
        self.report.initial_columns = len(df.columns)
        
        logger.info(f"Dataset inicial: {self.report.initial_rows} filas, {self.report.initial_columns} columnas")
        
        # Validar configuración contra DataFrame
        self._validate_config_against_dataframe(df)
        df, name_map = self._standardize_column_names(df)
        translated_config = self._translate_config_to_standardized_names(name_map)
        
        cleaning_config = translated_config['cleaning_config']
        
        #Transformación
        df = self._remove_duplicates(df)
        df = self._drop_columns(df, cleaning_config['cols_to_drop'])
        df = self._convert_date_columns(df, cleaning_config['date_cols'])
        df = self._handle_null_values(df, cleaning_config['null_handling_config'])
        df = df.select(sorted(df.columns))

        self.report.final_rows = df.count()
        self.report.final_columns = len(df.columns)

        logger.info(f"Pipeline completado: {self.report.final_rows} filas, {self.report.final_columns} columnas")
        logger.info(f"Tasa de retención: {self.report.data_retention_rate:.2f}%")
        
        return df
    
    def get_detailed_report(self) -> CleaningReport:
        """Retorna el reporte detallado de limpieza."""
        return self.report
    
    def save_report(self, report_path: str) -> None:
        """Guarda el reporte de limpieza en un archivo."""
        self.report.save_to_file(report_path)
        logger.info(f"Reporte guardado en: {report_path}")
    
    def print_summary(self) -> None:
        """Imprime un resumen ejecutivo del proceso de limpieza."""
        self.report.print_executive_summary()