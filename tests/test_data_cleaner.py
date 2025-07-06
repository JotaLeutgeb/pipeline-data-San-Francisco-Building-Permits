# tests/test_data_cleaner.py

import unittest
import sys
import os

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

from src.data_cleaner import DataCleaner

class TestDataCleaner(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("PySparkCleanerTests").master("local[2]").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        """Prepara los datos y la configuración para cada test."""
        test_data = [
            (1, 'A', '2023-01-01', 100.0, '2023-01-01', 'eliminar_1'),
            (2, 'B', None, 200.0, '2023-01-02', 'eliminar_2'),
            (3, 'A', '2023-02-01', 600.0, '2023-01-03', 'eliminar_3'),
            (4, None, '2023-03-01', None, '2023-01-04', 'eliminar_4'),
            (None, 'A', '2023-04-01', 300.0, '2023-01-05', 'eliminar_5'),
            (1, 'A', '2023-01-01', 100.0, '2023-01-01', 'eliminar_1'),
        ]
        
        # El esquema usa los nombres de columna originales y "sucios"
        schema = StructType([
            StructField("Record ID", IntegerType(), True),
            StructField("Permit Type", StringType(), True),
            StructField("Completed Date", StringType(), True),
            StructField("Estimated Cost", DoubleType(), True),
            StructField("Permit Creation Date", StringType(), True),
            StructField("col a eliminar", StringType(), True), # Nombre con espacios
        ])

        self.df = self.spark.createDataFrame(data=test_data, schema=schema)

        # La configuración del test sigue usando los nombres originales
        self.test_config = {
            "cols_to_drop": ["col a eliminar"],
            "date_cols": ["Permit Creation Date"],
            "null_handling_config": {
                "drop_rows_if_null": ["Record ID"],
                "impute_as_category": {"Completed Date": "Ongoing"},
                "impute_with_median": ["Estimated Cost"],
                "impute_with_mode": ["Permit Type"]
            }
        }
        self.cleaner = DataCleaner(self.test_config)
    
    def test_unit_name_map_creation_with_collision(self):
        """REFACTORED: Verifica que la creación del mapa de nombres maneje colisiones."""
        original_cols = ["Test Name", "test_name", "Another Col"]
        cleaner = DataCleaner({}) # No necesita config para este test
        name_map = cleaner._create_name_map(original_cols)
        
        expected_map = {
            "Another Col": "anothercol",
            "Test Name": "testname_0", # Ordenado alfabéticamente
            "test_name": "testname_1"
        }
        self.assertEqual(name_map, expected_map)

    def test_unit_median_imputation_is_correctly_applied(self):
        """NEW: Verifica de forma aislada que la imputación de la mediana funciona."""
        schema = StructType([StructField("id", IntegerType()), StructField("costo", DoubleType())])
        data = [(1, 10.0), (2, 80.0), (3, None), (4, 30.0)]
        df_minimal = self.spark.createDataFrame(data, schema)
        
        # Configuración mínima para el test
        config_minimal = {"null_handling_config": {"impute_with_median": ["costo"]}}
        cleaner_isolated = DataCleaner(config_minimal)
        
        result_df = cleaner_isolated.run_cleaning_pipeline(df_minimal)
        
        imputed_value = result_df.filter("id = 3").select("costo").first()[0]
        self.assertEqual(imputed_value, 30.0)

    def test_integration_full_pipeline(self):
        """REFACTORED: Verifica el pipeline completo con la nueva arquitectura."""
        cleaned_df = self.cleaner.run_cleaning_pipeline(self.df)
        
        # Las aserciones ahora usan los nombres de columna EST