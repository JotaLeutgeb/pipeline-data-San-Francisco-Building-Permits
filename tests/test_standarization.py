# tests/test_standardization.py
import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)
from pyspark.sql.functions import col

from src.data_cleaner import DataCleaner


class TestColumnNameStandardization:
    """Tests para la lógica de estandarización de nombres de columnas (ya existentes)."""

    # ... (todos tus tests desde test_basic_column_name_standardization hasta
    # test_dataframe_column_renaming se mantienen aquí sin cambios) ...
    def test_basic_column_name_standardization(self, spark, config_with_yaml_file):
        """Test estandarización básica de nombres de columnas."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        df = spark.createDataFrame([("test1", "test2", "test3")], ["Weird Column Name!!!", "  Spaces  ", "Normal_Name"])
        name_map = cleaner._create_column_name_map(df.columns)
        assert name_map["Weird Column Name!!!"] == "weird_column_name"
        assert name_map["  Spaces  "] == "spaces"
        assert name_map["Normal_Name"] == "normal_name"

    def test_column_name_collision_handling(self, spark, config_with_yaml_file):
        """Test manejo de colisiones en nombres de columnas."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        columns = ["Test Column", "Test  Column", "test column", "TEST_COLUMN"]
        name_map = cleaner._create_column_name_map(columns)
        standardized_names = list(name_map.values())
        assert len(standardized_names) == len(set(standardized_names))
        standardized_values = sorted(name_map.values())
        assert any("_0" in name for name in standardized_values)
        assert any("_1" in name for name in standardized_values)

    def test_special_characters_handling(self, spark, config_with_yaml_file):
        """Test manejo de caracteres especiales en nombres de columnas."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        columns = ["Column@#$%^&*()", "Column-With-Hyphens", "Column.With.Dots", "Column/With/Slashes", "Column With Multiple    Spaces"]
        name_map = cleaner._create_column_name_map(columns)
        for original, standardized in name_map.items():
            assert standardized.replace("_", "").replace("0", "").replace("1", "").replace("2", "").replace("3", "").replace("4", "").isalnum()
            assert not standardized.startswith("_")
            assert not standardized.endswith("_")

    def test_numbers_in_column_names(self, spark, config_with_yaml_file):
        """Prueba que los números y espacios se manejan correctamente."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        columns = ["123StartWithNumber", "Column123", "Column 123 Middle", "123 456 789"]
        name_map = cleaner._create_column_name_map(columns)
        
        # Aserciones CORRECTAS para la nueva lógica
        assert name_map["123StartWithNumber"] == "123startwithnumber"
        assert name_map["Column123"] == "column123"
        assert name_map["Column 123 Middle"] == "column_123_middle"
        assert name_map["123 456 789"] == "123_456_789"

    def test_unicode_characters_handling(self, spark, config_with_yaml_file):
        """Prueba que los caracteres Unicode se PRESERVAN."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        columns = ["Columna_Español", "Colonne_Français", "Колонка_Русский", "列_中文"]
        name_map = cleaner._create_column_name_map(columns)

        # Aserciones CORRECTAS: los nombres se limpian pero conservan sus caracteres.
        assert name_map["Columna_Español"] == "columna_español"
        assert name_map["Colonne_Français"] == "colonne_français"
        assert name_map["Колонка_Русский"] == "колонка_русский"
        assert name_map["列_中文"] == "列_中文"
        

    def test_empty_and_whitespace_columns(self, spark, config_with_yaml_file):
        """Test manejo de columnas vacías o solo con espacios."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        columns = ["", "   ", "\t\n", "Valid_Column"]
        name_map = cleaner._create_column_name_map(columns)
        for original, standardized in name_map.items():
            assert len(standardized) > 0
            assert not standardized.isspace()
            assert standardized.strip() == standardized

    def test_very_long_column_names(self, spark, config_with_yaml_file):
        """Test manejo de nombres de columnas muy largos."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        long_name = "This_Is_A_Very_Long_Column_Name_That_Should_Be_Handled_Properly_" * 5
        columns = [long_name, "Short"]
        name_map = cleaner._create_column_name_map(columns)
        for standardized in name_map.values():
            assert len(standardized) > 0
            assert isinstance(standardized, str)

    def test_reserved_keywords_handling(self, spark, config_with_yaml_file):
        """Test manejo de palabras reservadas potenciales."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        columns = ["select", "from", "where", "join", "group", "order"]
        name_map = cleaner._create_column_name_map(columns)
        for original, standardized in name_map.items():
            assert len(standardized) > 0
            assert standardized.isidentifier()

    def test_massive_collision_scenario(self, spark, config_with_yaml_file):
        """Test escenario con muchas colisiones potenciales."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        columns = ["Test Column", "Test_Column", "test column", "TEST COLUMN", "Test-Column", "Test.Column", "Test@Column", "Test#Column", "Test$Column", "Test%Column"]
        name_map = cleaner._create_column_name_map(columns)
        standardized_names = list(name_map.values())
        assert len(standardized_names) == len(set(standardized_names))
        assert len(standardized_names) == len(columns)
        suffixed_names = [name for name in standardized_names if "_" in name and name.split("_")[-1].isdigit()]
        assert len(suffixed_names) >= len(columns) - 1

    def test_column_mapping_preservation(self, spark, config_with_yaml_file):
        """Test que el mapeo de columnas se preserve correctamente."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        original_columns = ["Original Column 1", "Original Column 2", "Original Column 3"]
        name_map = cleaner._create_column_name_map(original_columns)
        assert len(name_map) == len(original_columns)
        for original in original_columns:
            assert original in name_map
            assert isinstance(name_map[original], str)
            assert len(name_map[original]) > 0
            
    def test_dataframe_column_renaming(self, spark, config_with_yaml_file):
        """Test renombrado de columnas en un DataFrame."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        df = spark.createDataFrame([("test1", "test2", "test3")], ["Weird Column Name!!!", "   Spaces  ", "Normal_Name"])
        
        # SOLUCIÓN: Desempaquetar la tupla
        renamed_df, _ = cleaner._standardize_column_names(df)
        
        # El nombre de columna con espacios en los extremos se limpia a "spaces"
        expected_columns = ["weird_column_name", "spaces", "normal_name"]

        assert renamed_df.columns == expected_columns

class TestFullStandardizationProcess:
    """Tests para el proceso completo de estandarización, verificando
    integridad de datos y comportamiento general."""

    @pytest.fixture
    def sample_df(self, spark):
        """Crea un DataFrame de ejemplo con nombres problemáticos y tipos de datos mixtos."""
        schema = StructType([
            StructField("First Name", StringType(), True),
            StructField("Age_in_Years", IntegerType(), True),
            StructField(" is_customer? ", BooleanType(), True)
        ])
        data = [("John", 30, True), ("Mary", 25, False)]
        return spark.createDataFrame(data, schema)

    def test_data_and_schema_integrity(self, spark, config_with_yaml_file, sample_df):
        """
        **Test Crítico**: Verifica que solo los nombres de las columnas cambien,
        pero los datos y sus tipos se mantengan intactos.
        """
        cleaner = DataCleaner(config_with_yaml_file, spark)

        # Asumimos que existe un método público que realiza la operación
        standardized_df, _ = cleaner._standardize_column_names(sample_df)

        # 1. Verificar nuevos nombres de columnas
        expected_columns = ["first_name", "age_in_years", "is_customer"]
        assert standardized_df.columns == expected_columns

        # 2. Verificar que los tipos de datos (schema) no cambiaron
        original_types = [field.dataType for field in sample_df.schema.fields]
        standardized_types = [field.dataType for field in standardized_df.schema.fields]
        assert original_types == standardized_types

        # 3. Verificar que los datos no se corrompieron
        # Se reordenan las columnas del DF original para que coincidan con el nuevo orden
        # y así poder comparar los datos directamente.
        original_data = sample_df.select(
            col("First Name").alias("first_name"),
            col("Age_in_Years").alias("age_in_years"),
            col(" is_customer? ").alias("is_customer")
        ).collect()
        
        standardized_data = standardized_df.collect()
        assert original_data == standardized_data

    def test_standardization_is_idempotent(self, spark, config_with_yaml_file):
        """
        Verifica que aplicar la estandarización a un DataFrame ya estandarizado
        no produzca ningún cambio.
        """
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        # DataFrame con nombres ya limpios
        df_clean = spark.createDataFrame([(1, "a"), (2, "b")], ["user_id", "user_category"])

        # Aplicar dos veces la estandarización
        df_re_standardized, _ = cleaner._standardize_column_names(df_clean)
        df_re_standardized_2, _ = cleaner._standardize_column_names(df_re_standardized)

        # Verificar que las columnas y los datos no han cambiado
        assert df_re_standardized.columns == df_re_standardized_2.columns
        assert df_re_standardized.collect() == df_re_standardized_2.collect()

    def test_empty_dataframe_handling(self, spark, config_with_yaml_file):
        """
        Verifica que el proceso no falle con un DataFrame vacío (sin filas).
        """
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        # DataFrame vacío con nombres de columna problemáticos
        schema = StructType([
            StructField("Column With Space", StringType(), True),
            StructField("Another-Column", IntegerType(), True)
        ])
        empty_df = spark.createDataFrame([], schema)

        standardized_df, _ = cleaner._standardize_column_names(empty_df)

        # Verificar que los nombres de columna se estandarizaron
        assert standardized_df.columns == ["column_with_space", "another_column"]
        # Verificar que el DataFrame sigue vacío
        assert standardized_df.count() == 0
    
    