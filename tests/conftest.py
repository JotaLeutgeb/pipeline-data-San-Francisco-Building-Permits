# tests/conftest.py
# tests/conftest.py
import pytest
import tempfile
import shutil
from pathlib import Path
import yaml
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

@pytest.fixture(scope="session")
def spark():
    """Fixture de sesión para Spark - compartida entre todos los tests."""
    spark = SparkSession.builder \
        .appName("ETL_Test_Session") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .config("spark.sql.adaptive.enabled", "false") \
        .master("local[2]") \
        .getOrCreate()
    
    yield spark
    spark.stop()

@pytest.fixture
def temp_config_dir():
    """Fixture para crear directorio temporal de configuración."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)

@pytest.fixture
def valid_config():
    """Fixture con configuración válida básica."""
    return {
        "data_path": "test_data.csv",
        "processed_data_path": "processed_test.csv",
        "cleaning_config": {
            "cols_to_drop": ["col_to_drop", "another_col"],
            "date_cols": ["date_col"],
            "null_handling_config": {
                "drop_rows_if_null": ["critical_col"],
                "impute_with_category": {"category_col": "Unknown"},
                "impute_with_median": ["numeric_col"],
                "impute_with_mode": ["categorical_col"]
            }
        }
    }

@pytest.fixture
def sample_dataframe(spark):
    """Fixture para crear DataFrame de prueba con datos realistas."""
    schema = StructType([
        StructField("Record ID", StringType(), True),
        StructField("col_to_drop", StringType(), True),
        StructField("another_col", StringType(), True),
        StructField("date_col", StringType(), True),
        StructField("critical_col", StringType(), True),
        StructField("category_col", StringType(), True),
        StructField("numeric_col", DoubleType(), True),
        StructField("categorical_col", StringType(), True),
        StructField("Weird Column Name!!!", StringType(), True)
    ])
    
    data = [
        ("1", "drop_me", "drop_me_too", "2023-01-01", "important", "cat1", 10.5, "mode1", "test"),
        ("2", "drop_me", "drop_me_too", "2023-01-02", "important", None, 20.5, "mode1", "test"),
        ("3", "drop_me", "drop_me_too", "2023-01-03", "important", "cat2", None, "mode2", "test"),
        ("4", "drop_me", "drop_me_too", "invalid-date", None, "cat1", 30.5, "mode1", "test"),  # Fila que debería eliminarse
        ("1", "drop_me", "drop_me_too", "2023-01-01", "important", "cat1", 10.5, "mode1", "test"),  # Duplicado
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture
def empty_dataframe(spark):
    """Fixture para DataFrame vacío con esquema."""
    schema = StructType([
        StructField("Record ID", StringType(), True),
        StructField("col_to_drop", StringType(), True),
        StructField("date_col", StringType(), True),
        StructField("critical_col", StringType(), True),
        StructField("category_col", StringType(), True),
        StructField("numeric_col", DoubleType(), True),
        StructField("categorical_col", StringType(), True)
    ])
    
    return spark.createDataFrame([], schema)

@pytest.fixture
def config_with_yaml_file(temp_config_dir, valid_config):
    """Fixture que crea un archivo YAML temporal con configuración válida."""
    config_path = temp_config_dir / "config.yml"
    with open(config_path, 'w') as f:
        yaml.dump(valid_config, f)
    return str(config_path)

@pytest.fixture
def dataframe_with_nulls(spark):
    """Fixture para DataFrame con varios tipos de valores nulos."""
    return spark.createDataFrame([
        ("1", "cat1", 10.0, "mode1"),
        ("2", None, 20.0, "mode1"),      # Null en category
        ("3", "cat2", None, "mode2"),    # Null en numeric
        ("4", "cat1", 30.0, None),       # Null en categorical
        ("5", None, None, None)          # Múltiples nulls
    ], ["id", "category_col", "numeric_col", "categorical_col"])

@pytest.fixture
def dataframe_with_duplicates(spark):
    """Fixture para DataFrame con duplicados."""
    return spark.createDataFrame([
        ("1", "value1"),
        ("2", "value2"),
        ("1", "value1"),  # Duplicado exacto
        ("3", "value3"),
        ("2", "value2"),  # Otro duplicado
        ("4", "value4")
    ], ["id", "value"])

@pytest.fixture
def dataframe_with_weird_columns(spark):
    """Fixture para DataFrame con nombres de columnas problemáticos."""
    schema = StructType([
        StructField("Weird Column Name!!!", StringType(), True),
        StructField("  Spaces  ", StringType(), True),
        StructField("Duplicate_Name", StringType(), True),
        StructField("duplicate name", StringType(), True),
        StructField("Special-Chars@#$", StringType(), True),
        StructField("123_starts_with_number", StringType(), True)
    ])
    
    return spark.createDataFrame([
        ("test1", "test2", "test3", "test4", "test5", "test6")
    ], schema)

# Configuración de pytest
def pytest_configure(config):
    """Configuración global de pytest."""
    config.addinivalue_line("markers", "slow: marks tests as slow")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "unit: marks tests as unit tests")
    config.addinivalue_line("markers", "edge_case: marks tests for edge cases")