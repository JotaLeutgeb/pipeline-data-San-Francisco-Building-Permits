import pytest
import pandas as pd
import numpy as np
import os
from src.data_cleaner import DataCleaner  # Make sure this import works based on your project structure

@pytest.fixture
def test_config_path(tmp_path) -> str:
    """
    A pytest fixture that creates a temporary test config YAML file and returns its path.
    This ensures tests are isolated and don't depend on the main config file.
    """
    config_content = """
null_threshold_drop: 0.5
null_handling:
  fill_with_value:
    - columns: ['estimated_cost', 'revised_cost', 'units']
      value: 0
    - columns: ['description']
      value: 'N/A'
  impute_from_other_column:
    - target_column: 'revised_cost'
      source_column: 'estimated_cost'
type_conversion:
  to_datetime:
    - ends_with: '_date'
  to_numeric:
    - contains: 'cost'
    - contains: 'units'
    """
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(config_content)
    return str(config_file)

# --- 1. Tests for Column Name Standardization ---

# Define test cases for parameterization
column_test_cases = [
    # input_columns, expected_columns, test_id
    (['First Name', 'Last-Name'], ['first_name', 'last_name'], 'basic_cleaning'),
    (['Año', 'País (ISO)'], ['ano', 'pais_iso'], 'special_chars'),
    (['col A', 'col_A', 'COL-A'], ['col_a', 'col_a_2', 'col_a_3'], 'duplicates'),
    (['__', '-'], ['unnamed_column', 'unnamed_column_2'], 'empty_names'),
]

@pytest.mark.parametrize("input_cols, expected_cols, test_id", column_test_cases, ids=[case[2] for case in column_test_cases])
def test_standardize_column_names(input_cols, expected_cols, test_id, test_config_path):
    """Tests the standardization of column names with various scenarios."""
    # Setup
    df_input = pd.DataFrame(columns=input_cols)
    cleaner = DataCleaner(df_input, test_config_path)

    # Action
    cleaner.standardize_column_names()

    # Assert
    assert cleaner.df.columns.tolist() == expected_cols

# --- 2. Tests for Handling Missing Values ---

def test_handle_missing_values_imputation_and_filling(test_config_path):
    """Tests if nulls are correctly imputed from another column and filled with specified values."""
    # Setup
    data = {
        'estimated_cost': [1000, 2000, np.nan],
        'revised_cost': [np.nan, 2500, 3000],
        'units': [1, np.nan, 3]
    }
    df_input = pd.DataFrame(data)
    cleaner = DataCleaner(df_input, test_config_path)

    # Action
    cleaner.handle_missing_values()

    # Assert
    expected_revised = pd.Series([1000.0, 2500.0, 3000.0], name='revised_cost')
    expected_units = pd.Series([1.0, 0.0, 3.0], name='units')
    
    pd.testing.assert_series_equal(cleaner.df['revised_cost'], expected_revised)
    pd.testing.assert_series_equal(cleaner.df['units'], expected_units)

def test_handle_missing_values_dropping_by_threshold(test_config_path):
    """Tests if columns with a high percentage of nulls are dropped correctly."""
    # Setup
    data = {
        'col_to_keep': [1, 2, 3, np.nan, 5],       # 20% nulls
        'col_to_drop': [1, np.nan, np.nan, np.nan, 5]  # 60% nulls (threshold is 50%)
    }
    df_input = pd.DataFrame(data)
    cleaner = DataCleaner(df_input, test_config_path)

    # Action
    cleaner.handle_missing_values()

    # Assert
    assert 'col_to_keep' in cleaner.df.columns
    assert 'col_to_drop' not in cleaner.df.columns

# --- 3. Test for Data Type Correction ---

def test_correct_data_types(test_config_path):
    """Tests if data types are correctly converted based on the config."""
    # Setup
    data = {
        'permit_creation_date': ['2025-01-01', '2025-02-10'],
        'estimated_cost': ['100.50', '200'],
        'some_string': ['A', 'B']
    }
    df_input = pd.DataFrame(data)
    cleaner = DataCleaner(df_input, test_config_path)

    # Action
    cleaner.correct_data_types()

    # Assert
    assert pd.api.types.is_datetime64_any_dtype(cleaner.df['permit_creation_date'])
    assert pd.api.types.is_numeric_dtype(cleaner.df['estimated_cost'])
    assert pd.api.types.is_string_dtype(cleaner.df['some_string'])

# --- 4. Integration Test for the Full Pipeline ---

def test_run_pipeline_end_to_end(test_config_path):
    """An integration test to ensure the full pipeline runs correctly."""
    # Setup
    dirty_data = {
        'Permit Type': [1, 2],
        'estimated_cost': ['5000', np.nan],
        'Permit Creation Date': ['2025-01-30', 'invalid-date'],
        'Very Null Col': [np.nan, np.nan] # Should be dropped
    }
    df_input = pd.DataFrame(dirty_data)
    cleaner = DataCleaner(df_input, test_config_path)

    # Action
    clean_df = cleaner.run_pipeline()

    # Assertions
    # Column names are clean?
    assert 'permit_type' in clean_df.columns
    assert 'very_null_col' not in clean_df.columns
    
    # Data types are correct?
    assert pd.api.types.is_datetime64_any_dtype(clean_df['permit_creation_date'])
    
    # Nulls handled?
    assert clean_df['estimated_cost'].isnull().sum() == 0
    
    # Values are correct? (e.g., coercion to NaT for invalid dates)
    assert pd.isna(clean_df.loc[1, 'permit_creation_date'])