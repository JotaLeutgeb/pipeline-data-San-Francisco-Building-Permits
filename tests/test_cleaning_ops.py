# tests/test_cleaning_operations.py
import pytest
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from unittest.mock import patch, MagicMock

from src.data_cleaner import DataCleaner


class TestDropColumns:
    """Tests para la funcionalidad de eliminación de columnas."""
    
    def test_drop_existing_columns(self, spark, config_with_yaml_file):
        """Test eliminación de columnas existentes."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        # Crear DataFrame con columnas a eliminar
        df = spark.createDataFrame([
            ("keep1", "drop1", "keep2", "drop2")
        ], ["keep_col1", "drop_col1", "keep_col2", "drop_col2"])
        
        result_df = cleaner._drop_columns(df, ["drop_col1", "drop_col2"])
        
        # Verificar que se mantienen las columnas correctas
        assert set(result_df.columns) == {"keep_col1", "keep_col2"}
        assert result_df.count() == 1
    
    def test_drop_single_column(self, spark, config_with_yaml_file):
        """Test eliminación de una sola columna."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([("keep", "drop")], ["keep_col", "drop_col"])
        result_df = cleaner._drop_columns(df, ["drop_col"])
        
        assert result_df.columns == ["keep_col"]
    
    def test_drop_nonexistent_columns(self, spark, config_with_yaml_file):
        """Test que maneje correctamente columnas inexistentes."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([("keep",)], ["keep_col"])
        
        # No debería fallar con columnas inexistentes
        result_df = cleaner._drop_columns(df, ["nonexistent_col"])
        
        assert result_df.columns == ["keep_col"]
        assert result_df.count() == 1
    
    def test_drop_all_columns_except_one(self, spark, config_with_yaml_file):
        """Test eliminación de todas las columnas excepto una."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("keep", "drop1", "drop2", "drop3")
        ], ["keep_col", "drop_col1", "drop_col2", "drop_col3"])
        
        result_df = cleaner._drop_columns(df, ["drop_col1", "drop_col2", "drop_col3"])
        
        assert result_df.columns == ["keep_col"]
        assert result_df.count() == 1
    
    def test_drop_empty_list(self, spark, config_with_yaml_file):
        """Test con lista vacía de columnas a eliminar."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([("col1", "col2")], ["keep_col1", "keep_col2"])
        result_df = cleaner._drop_columns(df, [])
        
        # DataFrame debería permanecer igual
        assert set(result_df.columns) == {"keep_col1", "keep_col2"}
        assert result_df.count() == 1
    
    def test_drop_columns_updates_report(self, spark, config_with_yaml_file):
        """Test que la eliminación de columnas actualice el reporte."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([("keep", "drop1", "drop2")], ["keep_col", "drop_col1", "drop_col2"])
        
        # Verificar estado inicial del reporte
        assert len(cleaner.report.column_drop_report.columns_dropped) == 0
        
        cleaner._drop_columns(df, ["drop_col1", "drop_col2"])
        
        # Verificar que el reporte se actualice
        assert len(cleaner.report.column_drop_report.columns_dropped) == 2
        assert "drop_col1" in cleaner.report.column_drop_report.columns_dropped
        assert "drop_col2" in cleaner.report.column_drop_report.columns_dropped


class TestRemoveDuplicates:
    """Tests para la funcionalidad de eliminación de duplicados."""
    
    def test_remove_exact_duplicates(self, spark, config_with_yaml_file):
        """Test eliminación de duplicados exactos."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        # Crear DataFrame con duplicados exactos
        df = spark.createDataFrame([
            ("1", "value1"),
            ("2", "value2"),
            ("1", "value1"),  # Duplicado exacto
            ("3", "value3"),
            ("2", "value2"),  # Otro duplicado
        ], ["id", "value"])
        
        result_df = cleaner._remove_duplicates(df)
        
        # Verificar que se eliminen los duplicados
        assert result_df.count() == 3
        
        # Verificar que se mantengan los valores únicos
        collected_data = result_df.collect()
        ids = [row["id"] for row in collected_data]
        assert "1" in ids
        assert "2" in ids
        assert "3" in ids
    
    def test_remove_duplicates_no_duplicates(self, spark, config_with_yaml_file):
        """Test con DataFrame sin duplicados."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("1", "value1"),
            ("2", "value2"),
            ("3", "value3")
        ], ["id", "value"])
        
        result_df = cleaner._remove_duplicates(df)
        
        # El DataFrame debería permanecer igual
        assert result_df.count() == 3
        assert cleaner.report.duplicate_report.duplicates_removed == 0
        assert cleaner.report.duplicate_report.unique_rows_kept == 3
    
    def test_remove_duplicates_all_duplicates(self, spark, config_with_yaml_file):
        """Test con DataFrame donde todas las filas son duplicados."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("1", "value1"),
            ("1", "value1"),
            ("1", "value1"),
            ("1", "value1")
        ], ["id", "value"])
        
        result_df = cleaner._remove_duplicates(df)
        
        # Debería quedar solo una fila
        assert result_df.count() == 1
        assert cleaner.report.duplicate_report.duplicates_removed == 3
        assert cleaner.report.duplicate_report.unique_rows_kept == 1
    
    def test_remove_duplicates_empty_dataframe(self, spark, config_with_yaml_file):
        """Test con DataFrame vacío."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([], StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ]))
        
        result_df = cleaner._remove_duplicates(df)
        
        assert result_df.count() == 0
        assert cleaner.report.duplicate_report.duplicates_removed == 0
        assert cleaner.report.duplicate_report.unique_rows_kept == 0
    
    def test_remove_duplicates_updates_report(self, spark, config_with_yaml_file):
        """Test que la eliminación de duplicados actualice el reporte correctamente."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("1", "value1"),
            ("1", "value1"),  # Duplicado
            ("2", "value2")
        ], ["id", "value"])
        
        # Estado inicial del reporte
        assert cleaner.report.duplicate_report.duplicates_removed == 0
        assert cleaner.report.duplicate_report.unique_rows_kept == 0
        
        result_df = cleaner._remove_duplicates(df)
        
        # Verificar actualización del reporte
        assert result_df.count() == 2
        assert cleaner.report.duplicate_report.duplicates_removed == 1
        assert cleaner.report.duplicate_report.unique_rows_kept == 2


class TestNullHandling:
    """Tests para el manejo de valores nulos."""
    
    def test_drop_rows_with_nulls(self, spark, config_with_yaml_file):
        """Test eliminación de filas con valores nulos."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("1", "important_value", "other"),
            ("2", None, "other"),           # Debería eliminarse
            ("3", "important_value", None), # No debería eliminarse (null en otra columna)
            ("4", None, None)               # Debería eliminarse
        ], ["id", "critical_col", "other_col"])
        
        null_config = {"drop_rows_if_null": ["critical_col"]}
        result_df = cleaner._handle_null_values(df, null_config)
        
        # Verificar que se eliminen las filas correctas
        assert result_df.count() == 2
        
        # Verificar que las filas restantes no tengan nulls en critical_col
        critical_col_nulls = result_df.filter(F.col("critical_col").isNull()).count()
        assert critical_col_nulls == 0
    
    def test_drop_rows_multiple_columns(self, spark, config_with_yaml_file):
        """Test eliminación de filas con nulls en múltiples columnas."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("1", "val1", "val2"),
            ("2", None, "val2"),    # Null en col1
            ("3", "val1", None),    # Null en col2
            ("4", None, None),      # Null en ambas
            ("5", "val1", "val2")
        ], ["id", "col1", "col2"])
        
        null_config = {"drop_rows_if_null": ["col1", "col2"]}
        result_df = cleaner._handle_null_values(df, null_config)
        
        # Solo deberían quedar las filas sin nulls en ninguna de las columnas especificadas
        assert result_df.count() == 2
        
        # Verificar que no hay nulls en las columnas críticas
        for col in ["col1", "col2"]:
            null_count = result_df.filter(F.col(col).isNull()).count()
            assert null_count == 0
    
    def test_impute_with_category(self, spark, config_with_yaml_file):
        """Test imputación como categoría."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("1", "cat1"),
            ("2", None),
            ("3", "cat2"),
            ("4", None)
        ], ["id", "category_col"])
        
        null_config = {"impute_with_category": {"category_col": "Unknown"}}
        result_df = cleaner._handle_null_values(df, null_config)
        
        # Verificar que no hay nulls
        null_count = result_df.filter(F.col("category_col").isNull()).count()
        assert null_count == 0
        
        # Verificar que los nulls se reemplazaron con "Unknown"
        unknown_count = result_df.filter(F.col("category_col") == "Unknown").count()
        assert unknown_count == 2
    
    def test_impute_with_median(self, spark, config_with_yaml_file):
        """Test imputación con mediana."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("1", 10.0),
            ("2", 20.0),
            ("3", None),
            ("4", 30.0),
            ("5", None)
        ], ["id", "numeric_col"])
        
        null_config = {"impute_with_median": ["numeric_col"]}
        result_df = cleaner._handle_null_values(df, null_config)
        
        # Verificar que no hay nulls
        null_count = result_df.filter(F.col("numeric_col").isNull()).count()
        assert null_count == 0
        
        # Verificar que los nulls se reemplazaron con la mediana (20.0)
        median_count = result_df.filter(F.col("numeric_col") == 20.0).count()
        assert median_count == 3  # 1 original + 2 imputados
    
    def test_impute_with_mode(self, spark, config_with_yaml_file):
        """Test imputación con moda."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("1", "mode1"),
            ("2", "mode1"),
            ("3", "mode2"),
            ("4", None),
            ("5", None)
        ], ["id", "categorical_col"])
        
        null_config = {"impute_with_mode": ["categorical_col"]}
        result_df = cleaner._handle_null_values(df, null_config)
        
        # Verificar que no hay nulls
        null_count = result_df.filter(F.col("categorical_col").isNull()).count()
        assert null_count == 0
        
        # Verificar que los nulls se reemplazaron con la moda ("mode1")
        mode_count = result_df.filter(F.col("categorical_col") == "mode1").count()
        assert mode_count == 4  # 2 originales + 2 imputados
    
    def test_combined_null_handling(self, spark, config_with_yaml_file):
        """Test manejo combinado de nulls (drop + impute)."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("1", "important", "cat1", 10.0),
            ("2", None, "cat2", 20.0),          # Debería eliminarse (null en critical)
            ("3", "important", None, 30.0),     # category_col debería imputarse
            ("4", "important", "cat1", None),   # numeric_col debería imputarse
            ("5", None, None, None)             # Debería eliminarse (null en critical)
        ], ["id", "critical_col", "category_col", "numeric_col"])
        
        null_config = {
            "drop_rows_if_null": ["critical_col"],
            "impute_with_category": {"category_col": "Unknown"},
            "impute_with_median": ["numeric_col"]
        }
        
        result_df = cleaner._handle_null_values(df, null_config)
        
        # Verificar que se eliminaron las filas correctas
        assert result_df.count() == 3
        
        # Verificar que no hay nulls en ninguna columna
        for col in ["critical_col", "category_col", "numeric_col"]:
            null_count = result_df.filter(F.col(col).isNull()).count()
            assert null_count == 0
    
    def test_null_handling_empty_dataframe(self, spark, config_with_yaml_file):
        """Test manejo de nulls con DataFrame vacío."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([], StructType([
            StructField("id", StringType(), True),
            StructField("col1", StringType(), True),
            StructField("col2", DoubleType(), True)
        ]))
        
        null_config = {
            "drop_rows_if_null": ["col1"],
            "impute_with_median": ["col2"]
        }
        
        result_df = cleaner._handle_null_values(df, null_config)
        
        # Debería permanecer vacío
        assert result_df.count() == 0
    
    def test_null_handling_updates_report(self, spark, config_with_yaml_file):
        """Test que el manejo de nulls actualice el reporte."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("1", "important", "cat1"),
            ("2", None, "cat2"),     # Debería eliminarse
            ("3", "important", None) # category_col debería imputarse
        ], ["id", "critical_col", "category_col"])
        
        null_config = {
            "drop_rows_if_null": ["critical_col"],
            "impute_with_category": {"category_col": "Unknown"}
        }
        
        result_df = cleaner._handle_null_values(df, null_config)
        
        # Verificar actualización del reporte
        assert cleaner.report.null_handling_report.rows_dropped_by_null.get("critical_col") == 1
        assert "category_col" in cleaner.report.null_handling_report.imputation_summary
        assert cleaner.report.null_handling_report.imputation_summary["category_col"]["nulls_found"] == 1
    
    def test_impute_all_null_column(self, spark, config_with_yaml_file):
        """Test imputación en columna completamente nula."""

        cleaner = DataCleaner(config_with_yaml_file, spark)

        schema = StructType([
            StructField("id", StringType(), True),
            StructField("all_null_col", StringType(), True) 
        ])
        
        df = spark.createDataFrame([
            ("1", None),
            ("2", None),
            ("3", None)
        ], schema)
        
        null_config = {"impute_with_category": {"all_null_col": "Default"}}
        result_df = cleaner._handle_null_values(df, null_config)
        
        # Verificar que todos los nulls se reemplazaron
        null_count = result_df.filter(F.col("all_null_col").isNull()).count()
        assert null_count == 0
        
        default_count = result_df.filter(F.col("all_null_col") == "Default").count()
        assert default_count == 3
    
    def test_impute_mode_with_tie(self, spark, config_with_yaml_file):
        """Test imputación con moda cuando hay empate."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("1", "A"),
            ("2", "B"),
            ("3", "A"),
            ("4", "B"),
            ("5", None)  # Empate entre A y B
        ], ["id", "tie_col"])
        
        null_config = {"impute_with_mode": ["tie_col"]}
        result_df = cleaner._handle_null_values(df, null_config)
        
        # Verificar que no hay nulls
        null_count = result_df.filter(F.col("tie_col").isNull()).count()
        assert null_count == 0
        
        # Verificar que se eligió una de las modas (A o B)
        collected = result_df.filter(F.col("id") == "5").collect()
        assert len(collected) == 1
        assert collected[0]["tie_col"] in ["A", "B"]


class TestDateHandling:
    """Tests para el manejo de fechas."""
    
    def test_convert_date_columns(self, spark, config_with_yaml_file):
        """Test estandarización de columnas de fecha."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("1", "2023-01-01"),
            ("2", "2023-12-31"),
            ("3", "2023-06-15")
        ], ["id", "date_col"])
        
        result_df = cleaner._convert_date_columns(df, ["date_col"])
        
        # Verificar que las fechas válidas se mantienen
        assert result_df.count() == 3
        
        # Verificar que se pueden convertir a timestamp
        # 1. Se extrae el tipo de dato para la aserción.
        # 2. Se usa el DataFrame real (result_df) para la aserción del filtro.
        col_type = result_df.schema["date_col"].dataType.simpleString()
        assert col_type == "date"

        # Verificar que no se generaron nulos en la conversión
        assert result_df.filter(F.col("date_col").isNull()).count() == 0
    
    def test_handle_invalid_dates(self, spark, config_with_yaml_file):
        """Test manejo de fechas inválidas."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("1", "2023-01-01"),      # Fecha válida
            ("2", "invalid-date"),    # Fecha inválida
            ("3", "2023-13-45"),      # Fecha inválida (mes/día imposible)
            ("4", "2023-06-15")       # Fecha válida
        ], ["id", "date_col"])
        
        result_df = cleaner._convert_date_columns(df, ["date_col"])
        
        # Las fechas inválidas deberían convertirse a null
        valid_dates = result_df.filter(F.col("date_col").isNotNull()).count()
        assert valid_dates == 2
        
        null_dates = result_df.filter(F.col("date_col").isNull()).count()
        assert null_dates == 2
    
    def test_multiple_date_columns(self, spark, config_with_yaml_file):
        """Test estandarización de múltiples columnas de fecha."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("1", "2023-01-01", "2023-01-15"),
            ("2", "invalid", "2023-02-20"),
            ("3", "2023-03-10", "invalid")
        ], ["id", "date_col1", "date_col2"])
        
        result_df = cleaner._convert_date_columns(df, ["date_col1", "date_col2"])
        
        # Verificar que cada columna se procesó independientemente
        assert result_df.count() == 3
        
        # Verificar fechas válidas en cada columna
        valid_col1 = result_df.filter(F.col("date_col1").isNotNull()).count()
        valid_col2 = result_df.filter(F.col("date_col2").isNotNull()).count()
        
        assert valid_col1 == 2  # Filas 1 y 3
        assert valid_col2 == 2  # Filas 1 y 2
    
    def test_empty_date_columns_list(self, spark, config_with_yaml_file):
        """Test con lista vacía de columnas de fecha."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        df = spark.createDataFrame([
            ("1", "2023-01-01"),
            ("2", "invalid-date")
        ], ["id", "date_col"])
        
        result_df = cleaner._convert_date_columns(df, [])
        
        # DataFrame debería permanecer sin cambios
        assert result_df.count() == 2
        assert result_df.columns == ["id", "date_col"]
        
        # Los valores deberían permanecer como string
        collected = result_df.collect()
        assert collected[0]["date_col"] == "2023-01-01"
        assert collected[1]["date_col"] == "invalid-date"