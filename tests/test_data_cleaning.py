import pytest
import pandas as pd
from src.data_cleaning import DataFrameStandardizer
from src.data_pipeline import DataCleaner

# ===================================================================
# Unit Tests for the DataFrameStandardizer Component
# ===================================================================

@pytest.fixture
def standardizer() -> DataFrameStandardizer:
    """Provides a reusable instance of the DataFrameStandardizer."""
    return DataFrameStandardizer()

# Define test cases for parameterization to test the component thoroughly
column_test_cases = [
    # input_columns, expected_columns, test_id
    (['First Name', 'Last-Name'], ['first_name', 'last_name'], 'basic_cleaning'),
    (['Año', 'País (ISO)'], ['ano', 'pais_iso'], 'special_chars'),
    (['col A', 'col_A', 'COL-A'], ['col_a', 'col_a_2', 'col_a_3'], 'deterministic_duplicates'),
    (['__', '-'], ['unnamed_column', 'unnamed_column_2'], 'empty_names'),
    (['A', 'B', 'a'], ['a', 'b', 'a_2'], 'order_insensitivity_1'),
    (['a', 'B', 'A'], ['a', 'b', 'a_2'], 'order_insensitivity_2'), # Should produce identical output
]

@pytest.mark.parametrize("input_cols, expected_cols, test_id", column_test_cases, ids=[case[2] for case in column_test_cases])
def test_standardizer_scenarios(standardizer, input_cols, expected_cols, test_id):
    """
    Unit test for the DataFrameStandardizer component, covering multiple scenarios.
    """
    # Setup
    df_input = pd.DataFrame(columns=input_cols)

    # Action
    df_clean = standardizer.standardize(df_input)

    # Assert
    assert df_clean.columns.tolist() == expected_cols

# ===================================================================
# Integration Test for the DataCleaner Pipeline Orchestrator
# ===================================================================

def test_datacleaner_pipeline_orchestration():
    """
    Integration test to ensure the DataCleaner correctly uses its components
    and that the pipeline runs end-to-end.
    """
    # Setup: Create a sample DataFrame with dirty column names
    dirty_data = {
        'First Name': ['john', 'jane'],
        'Last-Name': ['doe', 'doe'],
        'First Name': ['peter', 'sue'] # Deliberate duplicate column name
    }
    # Pandas automatically renames the second 'First Name' to 'First Name.1' on creation
    # Our standardizer should handle this gracefully. The input will be ['First Name', 'Last-Name', 'First Name.1']
    df_input = pd.DataFrame(dirty_data)
    
    # Expected columns after robust standardization
    expected_cols = ['first_name', 'last_name', 'first_name_1']

    # Action
    # Instantiate the pipeline orchestrator
    pipeline = DataCleaner(df_input)
    # Run the pipeline
    df_output = pipeline.run_pipeline()

    # Assert
    # Check if the final columns match the expected output after standardization
    assert df_output.columns.tolist() == expected_cols
    # Check if the data itself is preserved
    assert df_output.shape == (2, 3)
    assert df_output.iloc[0]['first_name'] == 'john'