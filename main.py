import logging
import pandas as pd
from configs import config
from src.data_cleaner import DataCleaner

# Configuración básica del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_pipeline():
    """
    Función principal que ejecuta el pipeline de limpieza de datos.

    Carga los datos, los limpia usando la clase DataCleaner y los guarda
    en un nuevo archivo CSV.
    """
    logging.info("Iniciando el pipeline de datos.")

    try:
        # Cargar los datos
        logging.info(f"Cargando datos desde: {config.DATA_PATH}")
        df = pd.read_csv(config.DATA_PATH)
        logging.info("Datos cargados exitosamente.")

        # Limpiar los datos
        cleaner = DataCleaner(df)
        cleaned_df = cleaner.clean_data(config.CLEANING_CONFIG)

        # Guardar los datos limpios
        logging.info(f"Guardando datos limpios en: {config.CLEANED_DATA_PATH}")
        cleaned_df.to_csv(config.CLEANED_DATA_PATH, index=False)
        logging.info("Pipeline completado exitosamente.")

    except FileNotFoundError:
        logging.error(f"Error: No se encontró el archivo de datos en {config.DATA_PATH}")
    except Exception as e:
        logging.error(f"Ha ocurrido un error inesperado durante la ejecución del pipeline: {e}")

if __name__ == '__main__':
    run_pipeline()