# main.py

import logging
import pandas as pd
import yaml
import sys
import os
import traceback

# Añadir el directorio raíz al path para que las importaciones funcionen
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PROJECT_ROOT)

from configs.config import PIPELINE_CONFIG
from src.data_cleaner import DataCleaner

# Configuración del logger (sin cambios)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log", mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_config(path="configs/config.yml") -> dict:
    logger.info(f"Cargando configuración desde {path}...")
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def main():
    """Orquesta la ejecución completa del pipeline ETL."""
    logger.info("Iniciando pipeline de permisos de construcción.")
    
    try:
        # 1. EXTRACCIÓN
        config = load_config()
        data_path = config['data_path']
        data_path = PIPELINE_CONFIG['data_path']
        logger.info(f"Extrayendo datos de: {data_path}")
        raw_df = pd.read_csv(data_path)
        
        # 2. TRANSFORMACIÓN
        cleaner = DataCleaner(raw_df, config)
        logger.info("Iniciando la fase de transformación...")
        cleaner = DataCleaner(raw_df, PIPELINE_CONFIG)
        cleaned_df = cleaner.run_cleaning_pipeline()
        
        #2.5 REPORTING 
        report = cleaner.get_report()
        logger.info("--- Resumen de Limpieza ---")
        logger.info(f"Filas iniciales: {report['initial_rows']}")
        logger.info(f"Filas finales: {report['final_rows']}")
        logger.info(f"Total de filas eliminadas: {report['rows_dropped']}")
        logger.info("---------------------------")

        # 3. CARGA
        output_path = PIPELINE_CONFIG['PROCESSED_DATA_PATH'] / "permisos_limpios.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True) # Asegura que el directorio exista
        logger.info(f"Cargando datos limpios en: {output_path}")
        cleaned_df.to_csv(output_path, index=False)
        
        logger.info("Pipeline completado exitosamente.")
        
    except FileNotFoundError as e:
        logging.error("Error: Archivo de entrada no encontrado.")
    except Exception as e:
        logging.error(f"Ocurrió un error inesperado: {e}")

if __name__ == "__main__":
    try:
        logging.info("INICIO: Pipeline de procesamiento de permisos de construcción")
        main()
        logging.info("FIN: Pipeline ejecutado exitosamente.")

    except FileNotFoundError as e:
        logging.error(f"Error de Extracción: No se pudo encontrar el archivo de entrada. Revisa la ruta en .env. Detalles: {e}")
    except pd.errors.EmptyDataError as e: # Existe archivo, no tiene columnas
        logging.error(f"Error de Extracción: El archivo de entrada está vacío. Detalles: {e}")
    except KeyError as e: # Se intenta buscar una columna que no existe
        logging.error(f"Error de Transformación: Falta una columna esperada en los datos. Columna: {e}")
    except ValueError as e: # Argumento de tipo correcto perovaloor inapropiado
        logging.error(f"Error de Transformación: Error en el valor de un dato, posiblemente un tipo incorrecto. Detalles: {e}")
    except Exception as e: # Aplica a cualquier otro error no especificado
        logging.error(f"Ocurrió un error no manejado en el pipeline: {e}")
        logging.error(f"Traceback completo:\n{traceback.format_exc()}")