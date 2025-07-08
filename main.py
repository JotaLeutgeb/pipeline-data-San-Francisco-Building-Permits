# main.py
"""
Script principal para orquestar el pipeline ETL de limpieza de datos de
permisos de construcción de San Francisco.

Fases del pipeline:
1.  **Configuración e Inicialización**: Crea la sesión de Spark y carga la configuración.
2.  **Extracción**: Carga los datos crudos desde un archivo CSV.
3.  **Transformación**: Aplica todas las reglas de limpieza definidas en DataCleaner.
4.  **Reporte**: Genera y guarda un reporte detallado del proceso de limpieza.
5.  **Carga**: Guarda el DataFrame limpio en el formato especificado (CSV o Parquet).
"""
import logging
import sys
import os
import traceback
from pathlib import Path

# Configurar el entorno para que PySpark funcione correctamente.
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

# --- 1. IMPORTACIÓN DE CONFIGURACIÓN CENTRALIZADA ---
# Se importa el módulo de configuración de rutas. Usamos un alias 'paths' para claridad.
# Ya no se definen rutas manualmente en este archivo.
from configs import config as paths
from src.data_cleaner import DataCleaner


# --- 2. CONFIGURACIÓN DEL LOGGER USANDO RUTAS CENTRALIZADAS ---
# Asegurarse de que el directorio de logs exista antes de configurarlo.
paths.LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        # La ruta del archivo de log ahora viene de nuestra configuración.
        logging.FileHandler(paths.LOG_FILE_PATH, mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Crea y configura una sesión de Spark optimizada."""
    logger.info("Creando sesión de Spark...")
    try:
        spark = SparkSession.builder \
            .appName("SF_Building_Permits_ETL") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        logger.info("Sesión de Spark creada exitosamente.")
        return spark
    except Exception as e:
        logger.error(f"Error fatal al crear la sesión de Spark: {e}")
        raise


def load_data(spark: SparkSession, data_path: str) -> SparkSession:
    """Carga los datos desde un archivo CSV, validando su existencia."""
    logger.info(f"Cargando datos desde: {data_path}")
    if not Path(data_path).exists():
        raise FileNotFoundError(f"El archivo de datos no se encontró en la ruta: {data_path}")

    try:
        df = spark.read.option("header", "true") \
                       .option("inferSchema", "true") \
                       .option("multiline", "true") \
                       .option("escape", '"') \
                       .csv(data_path)
        logger.info(f"Datos cargados exitosamente: {df.count():,} filas y {len(df.columns)} columnas.")
        return df
    except Exception as e:
        logger.error(f"Error al leer el archivo CSV: {e}")
        raise


def save_data(df: SparkSession, output_path_str: str):
    """Guarda el DataFrame limpio en formato Parquet o CSV."""
    output_path = Path(output_path_str)
    logger.info(f"Guardando datos limpios en: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if output_path.suffix.lower() == '.parquet':
            df.write.mode("overwrite").parquet(str(output_path))
        else:
            temp_output_dir = output_path.parent / f"temp_{output_path.stem}"
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(temp_output_dir))
            
            csv_files = list(temp_output_dir.glob("*.csv"))
            if csv_files:
                csv_files[0].rename(output_path)
                for file in temp_output_dir.iterdir():
                    file.unlink()
                temp_output_dir.rmdir()
            else:
                logger.warning("No se encontró ningún archivo CSV en el directorio temporal para renombrar.")

        logger.info(f"✅ Datos guardados exitosamente.")
    except Exception as e:
        logger.error(f"Error al guardar los datos: {e}")
        raise


def main():
    """Orquesta la ejecución completa del pipeline ETL."""
    logger.info("INICIANDO PIPELINE ETL DE PERMISOS DE CONSTRUCCIÓN")
    spark = None
    try:
        # 3. INICIALIZACIÓN USANDO RUTAS CENTRALIZADAS
        # La ruta al archivo .yml también se construye a partir de la configuración.
        config_yml_path = paths.ROOT_PATH / "configs" / "config.yml"
        
        spark = create_spark_session()
        cleaner = DataCleaner(str(config_yml_path), spark)

        # Las rutas de datos de entrada/salida se leen desde el .yml,
        # lo cual ya es una buena práctica.
        # cleaner.config.data_path
        # cleaner.config.processed_data_path
        
        # EXTRACCIÓN (E)
        logger.info("Iniciando Fase de Extracción...")
        raw_df = load_data(spark, cleaner.config.data_path)

        # TRANSFORMACIÓN (T)
        logger.info("Iniciando Fase de Transformación...")
        cleaned_df = cleaner.run_cleaning_pipeline(raw_df)

        # REPORTE
        logger.info("Generando y guardando reporte de limpieza...")
        report = cleaner.get_detailed_report()
        
        # La ruta del reporte también viene de la configuración.
        report_path = paths.REPORTS_PATH / "cleaning_report.json"
        paths.REPORTS_PATH.mkdir(parents=True, exist_ok=True)
        cleaner.save_report(str(report_path))
        cleaner.print_summary()

        # CARGA (L)
        logger.info("Iniciando Fase de Carga...")
        save_data(cleaned_df, cleaner.config.processed_data_path)

        logger.info("PIPELINE COMPLETADO EXITOSAMENTE")

    except (FileNotFoundError, ValueError) as e:
        logger.error(f"ERROR DE CONFIGURACIÓN O DATOS: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"ERROR INESPERADO EN EL PIPELINE: {e}")
        logger.error(f"Traceback completo:\n{traceback.format_exc()}")
        sys.exit(1)
    finally:
        if spark:
            logger.info("Cerrando sesión de Spark...")
            spark.stop()
            logger.info("Sesión de Spark cerrada.")

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Pipeline interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error crítico no manejado: {e}")
        sys.exit(1)
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