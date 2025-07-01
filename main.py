import logging
import pandas as pd
import sys
import os

# Añade el directorio raíz del proyecto a sys.path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PROJECT_ROOT)

from configs import config
from src.data_cleaner import DataCleaner


# Configuración del logger para registrar cada paso del pipeline
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"), # Guarda los logs en un archivo
        logging.StreamHandler() # Muestra los logs en la consola
    ]
)

logger = logging.getLogger(__name__)

def extract_data(path: str) -> pd.DataFrame:
    """
    Paso 1: Extracción de Datos.
    Carga los datos desde un archivo CSV.

    Args:
        path (str): Ruta al archivo CSV.

    Returns:
        pd.DataFrame: DataFrame con los datos cargados.
    """
    try:
        logger.info(f"Iniciando el paso de EXTRACCIÓN desde {path}...")
        df = pd.read_csv(path)
        logger.info("Extracción completada exitosamente.")
        return df
    except FileNotFoundError:
        logger.error(f"Error en EXTRACCIÓN: El archivo no fue encontrado en {path}.")
        raise
    except Exception as e:
        logger.error(f"Error inesperado durante la EXTRACCIÓN: {e}")
        raise

def transform_data(df: pd.DataFrame, cleaning_config: dict) -> pd.DataFrame:
    """
    Paso 2: Transformación de Datos.
    Limpia y procesa el DataFrame.

    Args:
        df (pd.DataFrame): DataFrame con los datos crudos.
        cleaning_config (dict): Configuración para la limpieza.

    Returns:
        pd.DataFrame: DataFrame transformado y limpio.
    """
    try:
        logger.info("Iniciando el paso de TRANSFORMACIÓN...")
        cleaner = DataCleaner(df)
        cleaned_df = cleaner.clean_data(cleaning_config)
        logger.info("Transformación completada exitosamente.")
        return cleaned_df
    except Exception as e:
        logger.error(f"Error inesperado durante la TRANSFORMACIÓN: {e}")
        raise

def load_data(df: pd.DataFrame, path: str):
    """
    Paso 3: Carga de Datos.
    Guarda el DataFrame procesado en un archivo CSV.

    Args:
        df (pd.DataFrame): DataFrame limpio.
        path (str): Ruta del archivo de destino.
    """
    try:
        logger.info(f"Iniciando el paso de CARGA hacia {path}...")
        df.to_csv(path, index=False)
        logger.info("Carga completada exitosamente. Los datos limpios están listos.")
    except Exception as e:
        logger.error(f"Error inesperado durante la CARGA: {e}")
        raise

def run_pipeline():
    """Orquesta la ejecución completa del pipeline ETL."""
    logger.info("🚀 Iniciando pipeline de permisos de construcción.")
    
    try:
        # 1. Cargar datos
        raw_df = pd.read_csv(config.DATA_PATH)
        
        # 2. Cargar configuración (ya está cargada en config.py)
        cleaning_config = config.CLEANING_CONFIG
        logger.info("Configuración de limpieza cargada.")
        
        # 3. Instanciar y ejecutar el pipeline de limpieza
        cleaner = DataCleaner(raw_df)
        cleaned_df = cleaner.run_cleaning_pipeline(cleaning_config)
        
        # 4. Guardar resultado
        load_data(cleaned_df, config.PROCESSED_DATA_DIR)
        logger.info("✅ Pipeline completado exitosamente.")
    except Exception as e:
        logger.critical(f"❌ El pipeline falló. Error: {e}", exc_info=True)

if __name__ == '__main__':
    run_pipeline()