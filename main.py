# main.py

import logging
import pandas as pd
import sys
import os

# Añadir el directorio raíz al path para que las importaciones funcionen
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PROJECT_ROOT)

# CAMBIO: Importamos el diccionario de configuración, no el módulo.
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

def run_pipeline():
    """Orquesta la ejecución completa del pipeline ETL."""
    logger.info("Iniciando pipeline de permisos de construcción.")
    
    try:
        # 1. EXTRACCIÓN
        data_path = PIPELINE_CONFIG['data_path']
        logger.info(f"Extrayendo datos de: {data_path}")
        raw_df = pd.read_csv(data_path)
        
        # 2. TRANSFORMACIÓN
        logger.info("Iniciando la fase de transformación...")
        # CAMBIO: Instanciamos DataCleaner pasándole el DataFrame y la configuración.
        cleaner = DataCleaner(raw_df, PIPELINE_CONFIG)
        # CAMBIO: run_cleaning_pipeline ya no necesita argumentos.
        cleaned_df = cleaner.run_cleaning_pipeline()
        
        # 3. CARGA
        output_path = PIPELINE_CONFIG['PROCESSED_DATA_PATH'] / "permisos_limpios.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True) # Asegura que el directorio exista
        logger.info(f"Cargando datos limpios en: {output_path}")
        cleaned_df.to_csv(output_path, index=False)
        
        logger.info("✅ Pipeline completado exitosamente.")
        
    except FileNotFoundError:
        logger.error(f"❌ ERROR: El archivo de datos no fue encontrado en '{data_path}'.")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"❌ El pipeline falló de forma inesperada. Error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    run_pipeline()