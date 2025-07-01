import pandas as pd
import numpy as np
import yaml
import os
import logging
from collections import Counter
from unidecode import unidecode
import re

# Configuración del Logging: La mejor práctica para la trazabilidad
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# (La función 'standardize_column_names' no cambia, la incluimos por completitud)
def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    def clean_name(col_name: str) -> str:
        s = unidecode(str(col_name)).lower()
        s = re.sub(r'[^a-z0-9]+', '_', s).strip('_')
        return s if s else 'unnamed_column'
    original_columns, cleaned_names = df.columns.tolist(), [clean_name(col) for col in df.columns.tolist()]
    name_counts, final_columns, suffix_tracker = Counter(cleaned_names), [], Counter()
    for name in cleaned_names:
        if name_counts[name] == 1:
            final_columns.append(name)
            continue
        suffix_tracker.update([name])
        current_occurrence = suffix_tracker[name]
        if current_occurrence == 1: final_columns.append(name)
        else: final_columns.append(f"{name}_{current_occurrence}")
    df.columns = final_columns
    return df


def load_config(config_path: str) -> dict:
    """Carga de forma segura la configuración YAML."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Archivo de configuración no encontrado en {config_path}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error al parsear el archivo YAML: {e}")
        raise

def handle_missing_values(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Maneja los valores nulos basándose en las reglas de la configuración."""
    logging.info("Iniciando manejo de valores nulos...")
    df = df.copy()
    
    # Eliminar columnas por umbral de nulos
    if 'null_threshold_drop' in config:
        threshold = config['null_threshold_drop']
        null_pct = df.isnull().sum() / len(df)
        cols_to_drop = null_pct[null_pct > threshold].index
        df.drop(columns=cols_to_drop, inplace=True)
        logging.info(f"Columnas eliminadas por superar umbral de {threshold*100}% nulos: {list(cols_to_drop)}")

    # Imputación desde otra columna
    if 'impute_from_other_column' in config.get('null_handling', {}):
        for rule in config['null_handling']['impute_from_other_column']:
            target, source = rule['target_column'], rule['source_column']
            if target in df.columns and source in df.columns:
                df[target].fillna(df[source], inplace=True)
                logging.info(f"Nulos en '{target}' imputados desde '{source}'.")
    
    # Llenado con un valor específico
    if 'fill_with_value' in config.get('null_handling', {}):
        for rule in config['null_handling']['fill_with_value']:
            value = rule['value']
            for col in rule['columns']:
                if col in df.columns:
                    df[col].fillna(value, inplace=True)
                    logging.info(f"Nulos en '{col}' llenados con el valor '{value}'.")
    
    # Eliminación manual
    if 'manual_drop' in config:
        cols_to_drop = [col for col in config['manual_drop'] if col in df.columns]
        df.drop(columns=cols_to_drop, inplace=True)
        logging.info(f"Columnas eliminadas manualmente: {cols_to_drop}")

    return df

def correct_data_types(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Convierte los tipos de datos basándose en la configuración."""
    logging.info("Iniciando corrección de tipos de datos...")
    df = df.copy()
    
    if 'type_conversion' not in config:
        return df

    type_cfg = config['type_conversion']
    
    # Conversión a datetime
    for rule in type_cfg.get('to_datetime', []):
        if 'ends_with' in rule:
            for col in df.columns:
                if col.endswith(rule['ends_with']):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    logging.info(f"Columna '{col}' convertida a datetime.")

    # Conversión a numérico
    for rule in type_cfg.get('to_numeric', []):
        if 'contains' in rule:
            for col in df.columns:
                if rule['contains'] in col:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    logging.info(f"Columna '{col}' convertida a numérica.")
    
    return df

def run_cleaning_pipeline(raw_file_path: str, processed_file_path: str, config_path: str):
    """Pipeline principal que orquesta la limpieza basada en la configuración."""
    try:
        logging.info("🚀 Iniciando pipeline de limpieza de datos...")
        config = load_config(config_path)
        
        df = pd.read_csv(raw_file_path, low_memory=False)
        logging.info(f"Datos cargados desde '{raw_file_path}'. Shape: {df.shape}")

        df = standardize_column_names(df)
        df = handle_missing_values(df, config)
        df = correct_data_types(df, config)

        os.makedirs(os.path.dirname(processed_file_path), exist_ok=True)
        df.to_csv(processed_file_path, index=False)
        logging.info(f"✅ ¡Éxito! Datos limpios guardados en '{processed_file_path}'. Shape final: {df.shape}")

    except FileNotFoundError:
        logging.error(f"❌ Error Crítico: El archivo de datos no fue encontrado en {raw_file_path}")
    except Exception as e:
        logging.error(f"❌ Error Crítico: Ocurrió un error inesperado en el pipeline.", exc_info=True)


if __name__ == '__main__':
    RAW_DATA_FILE = 'data/raw/Building_Permits_20250630.csv'
    PROCESSED_DATA_FILE = 'data/processed/Building_Permits_cleaned.csv'
    CONFIG_FILE = 'configs/cleaning_config.yaml'
    
    run_cleaning_pipeline(RAW_DATA_FILE, PROCESSED_DATA_FILE, CONFIG_FILE)