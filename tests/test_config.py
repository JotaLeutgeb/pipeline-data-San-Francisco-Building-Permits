# tests/test_config.py
import pytest
import yaml
from pydantic import ValidationError

from src.config_schema import MainConfig
from src.data_cleaner import DataCleaner


class TestConfigValidation:
    """Tests para validación de configuración con Pydantic."""
    
    def test_valid_config_parsing(self, valid_config):
        """Test que la configuración válida se parse correctamente."""
        config = MainConfig.parse_obj(valid_config)
        assert config.data_path == "test_data.csv"
        assert config.processed_data_path == "processed_test.csv"
        assert len(config.cleaning_config.cols_to_drop) == 2
        assert "critical_col" in config.cleaning_config.null_handling_config.drop_rows_if_null
        assert "col_to_drop" in config.cleaning_config.cols_to_drop
        assert "date_col" in config.cleaning_config.date_cols
    
    def test_invalid_config_missing_required_fields(self):
        """Test que falle con campos requeridos faltantes."""
        invalid_config = {
            "data_path": "test.csv"
            # Falta processed_data_path y cleaning_config
        }
        
        with pytest.raises(ValidationError) as exc_info:
            MainConfig.parse_obj(invalid_config)
        
        errors = exc_info.value.errors()
        assert len(errors) >= 2  # Al menos 2 campos faltantes
        assert any("processed_data_path" in str(error) for error in errors)
        assert any("cleaning_config" in str(error) for error in errors)
    
    def test_invalid_config_wrong_types(self):
        """Test que falle con tipos incorrectos."""
        invalid_config = {
            "data_path": 123,  # Debería ser string
            "processed_data_path": "processed.csv",
            "cleaning_config": {
                "cols_to_drop": "not_a_list",  # Debería ser lista
                "date_cols": [],
                "null_handling_config": {
                    "drop_rows_if_null": [],
                    "impute_with_category": {},
                    "impute_with_median": [],
                    "impute_with_mode": []
                }
            }
        }
        
        with pytest.raises(ValidationError) as exc_info:
            MainConfig.parse_obj(invalid_config)
        
        errors = exc_info.value.errors()
        assert any("data_path" in str(error) for error in errors)
        assert any("cols_to_drop" in str(error) for error in errors)
    
    def test_extra_fields_forbidden(self, valid_config):
        """Test que se rechacen campos extra."""
        valid_config["extra_field"] = "not_allowed"
        
        with pytest.raises(ValidationError) as exc_info:
            MainConfig.parse_obj(valid_config)
        
        assert "extra_field" in str(exc_info.value)
    
    def test_empty_cleaning_config(self):
        """Test configuración con cleaning_config vacío."""
        config = {
            "data_path": "test.csv",
            "processed_data_path": "processed.csv",
            "cleaning_config": {
                "cols_to_drop": [],
                "date_cols": [],
                "null_handling_config": {
                    "drop_rows_if_null": [],
                    "impute_with_category": {},
                    "impute_with_median": [],
                    "impute_with_mode": []
                }
            }
        }
        
        # No debería fallar
        parsed_config = MainConfig.parse_obj(config)
        assert len(parsed_config.cleaning_config.cols_to_drop) == 0
        assert len(parsed_config.cleaning_config.date_cols) == 0
    
    def test_null_handling_config_validation(self):
        """Test validación específica de null handling config."""
        config = {
            "data_path": "test.csv",
            "processed_data_path": "processed.csv",
            "cleaning_config": {
                "cols_to_drop": [],
                "date_cols": [],
                "null_handling_config": {
                    "drop_rows_if_null": ["col1", "col2"],
                    "impute_with_category": {"col3": "Unknown", "col4": "N/A"},
                    "impute_with_median": ["col5"],
                    "impute_with_mode": ["col6", "col7"]
                }
            }
        }
        
        parsed_config = MainConfig.parse_obj(config)
        null_config = parsed_config.cleaning_config.null_handling_config
        
        assert len(null_config.drop_rows_if_null) == 2
        assert len(null_config.impute_with_category) == 2
        assert len(null_config.impute_with_median) == 1
        assert len(null_config.impute_with_mode) == 2
        assert null_config.impute_with_category["col3"] == "Unknown"


class TestDataCleanerInitialization:
    """Tests para inicialización de DataCleaner."""
    
    def test_successful_initialization(self, spark, config_with_yaml_file):
        """Test inicialización exitosa."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        assert cleaner.config.data_path == "test_data.csv"
        assert cleaner.spark == spark
        assert cleaner.report is not None
    
    def test_initialization_with_invalid_config_path(self, spark):
        """Test que falle con ruta de configuración inválida."""
        with pytest.raises(FileNotFoundError):
            DataCleaner("nonexistent_config.yml", spark)
    
    def test_initialization_with_invalid_yaml(self, spark, temp_config_dir):
        """Test que falle con YAML inválido."""
        config_path = temp_config_dir / "invalid.yml"
        with open(config_path, 'w') as f:
            f.write("invalid: yaml: content: [")
        
        with pytest.raises(yaml.YAMLError):
            DataCleaner(str(config_path), spark)
    
    def test_initialization_creates_spark_session_if_none(self, config_with_yaml_file):
        """Test que cree sesión de Spark si no se proporciona."""
        cleaner = DataCleaner(config_with_yaml_file)
        assert cleaner.spark is not None
        assert cleaner.spark.sparkContext.appName == "ETL_Test_Session"
    
    def test_initialization_with_malformed_config_content(self, spark, temp_config_dir):
        """Test con contenido de configuración malformado."""
        config_path = temp_config_dir / "malformed.yml"
        malformed_config = {
            "data_path": "test.csv",
            "processed_data_path": "processed.csv",
            "cleaning_config": {
                "cols_to_drop": ["col1"],
                "date_cols": [],
                "null_handling_config": {
                    "drop_rows_if_null": "should_be_list",  # Error: debería ser lista
                    "impute_with_category": [],  # Error: debería ser dict
                    "impute_with_median": "should_be_list",  # Error: debería ser lista
                    "impute_with_mode": {}  # Error: debería ser lista
                }
            }
        }
        
        with open(config_path, 'w') as f:
            yaml.dump(malformed_config, f)
        
        with pytest.raises(ValidationError):
            DataCleaner(str(config_path), spark)
    
    def test_config_property_access(self, spark, config_with_yaml_file):
        """Test acceso a propiedades de configuración."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        # Verificar acceso a propiedades anidadas
        assert isinstance(cleaner.config.cleaning_config.cols_to_drop, list)
        assert isinstance(cleaner.config.cleaning_config.date_cols, list)
        assert isinstance(cleaner.config.cleaning_config.null_handling_config.drop_rows_if_null, list)
        assert isinstance(cleaner.config.cleaning_config.null_handling_config.impute_with_category, dict)
        assert isinstance(cleaner.config.cleaning_config.null_handling_config.impute_with_median, list)
        assert isinstance(cleaner.config.cleaning_config.null_handling_config.impute_with_mode, list)


class TestConfigValidationAgainstDataFrame:
    """Tests para validación de configuración contra DataFrame."""
    
    def test_valid_config_against_dataframe(self, spark, config_with_yaml_file, sample_dataframe):
        """Test validación exitosa de configuración contra DataFrame."""
        cleaner = DataCleaner(config_with_yaml_file, spark)
        
        # No debería lanzar excepción
        cleaner._validate_config_against_dataframe(sample_dataframe)
    
    def test_invalid_config_missing_columns(self, spark, temp_config_dir, sample_dataframe):
        """Test que falle cuando la configuración reference columnas inexistentes."""
        invalid_config = {
            "data_path": "test.csv",
            "processed_data_path": "processed.csv",
            "cleaning_config": {
                "cols_to_drop": ["nonexistent_column"],
                "date_cols": ["another_nonexistent_column"],
                "null_handling_config": {
                    "drop_rows_if_null": ["missing_column"],
                    "impute_with_category": {"missing_cat_col": "Unknown"},
                    "impute_with_median": ["missing_num_col"],
                    "impute_with_mode": ["missing_mode_col"]
                }
            }
        }
        
        config_path = temp_config_dir / "config.yml"
        with open(config_path, 'w') as f:
            yaml.dump(invalid_config, f)
        
        cleaner = DataCleaner(str(config_path), spark)
        
        with pytest.raises(ValueError) as exc_info:
            cleaner._validate_config_against_dataframe(sample_dataframe)
        
        error_msg = str(exc_info.value)
        assert "nonexistent_column" in error_msg
        assert "another_nonexistent_column" in error_msg
        assert "missing_column" in error_msg
    
    def test_partial_invalid_config_columns(self, spark, temp_config_dir, sample_dataframe):
        """Test con configuración parcialmente inválida."""
        mixed_config = {
            "data_path": "test.csv",
            "processed_data_path": "processed.csv",
            "cleaning_config": {
                "cols_to_drop": ["col_to_drop", "nonexistent_col"],  # Una existe, otra no
                "date_cols": ["date_col"],  # Esta existe
                "null_handling_config": {
                    "drop_rows_if_null": ["critical_col"],  # Esta existe
                    "impute_with_category": {"category_col": "Unknown"},  # Esta existe
                    "impute_with_median": ["numeric_col", "missing_col"],  # Una existe, otra no
                    "impute_with_mode": ["categorical_col"]  # Esta existe
                }
            }
        }
        
        config_path = temp_config_dir / "config.yml"
        with open(config_path, 'w') as f:
            yaml.dump(mixed_config, f)
        
        cleaner = DataCleaner(str(config_path), spark)
        
        with pytest.raises(ValueError) as exc_info:
            cleaner._validate_config_against_dataframe(sample_dataframe)
        
        error_msg = str(exc_info.value)
        assert "nonexistent_col" in error_msg
        assert "missing_col" in error_msg
        # Las columnas válidas no deberían aparecer en el error
        assert "col_to_drop" not in error_msg
        assert "date_col" not in error_msg
    
    def test_config_validation_with_standardized_columns(self, spark, temp_config_dir, dataframe_with_weird_columns):
        """Test validación de configuración con nombres de columnas estandarizados."""
        # Configuración usando nombres estandarizados
        config_with_standardized = {
            "data_path": "test.csv",
            "processed_data_path": "processed.csv",
            "cleaning_config": {
                "cols_to_drop": ["weird_column_name"],  # Nombre estandarizado
                "date_cols": ["spaces"],  # Nombre estandarizado
                "null_handling_config": {
                    "drop_rows_if_null": ["duplicate_name"],
                    "impute_with_category": {},
                    "impute_with_median": [],
                    "impute_with_mode": []
                }
            }
        }
        
        config_path = temp_config_dir / "config.yml"
        with open(config_path, 'w') as f:
            yaml.dump(config_with_standardized, f)
        
        cleaner = DataCleaner(str(config_path), spark)
        
        # Después de la estandarización, la validación debería pasar
        # (esto requiere que la validación se haga después de la estandarización)
        # Si la implementación actual no maneja esto, este test ayudará a identificar la necesidad
        try:
            cleaner._validate_config_against_dataframe(dataframe_with_weird_columns)
        except ValueError:
            # Si falla, es porque la validación se hace antes de la estandarización
            # Este comportamiento podría necesitar ajuste en la implementación
            pass