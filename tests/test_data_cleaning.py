# 1. Importaciones de librerías estándar y de terceros
import pandas as pd
import pytest


from src.data_cleaning import standardize_column_names


# --- El resto de tu código de test (sin cambios) ---

def test_standardize_column_names():
    """
    Test para verificar que los nombres de columna se estandarizan correctamente.
    """
    # Create a sample DataFrame
    data = {
        'Column One': [1, 2],
        'COLUMN_TWO': [3, 4],
        'column-three!': [5, 6],
    }
    df = pd.DataFrame(data)

    # Apply the standardization function
    standardized_df = standardize_column_names(df) # Asumo que tu función se llama así ahora

    # Check if the column names are standardized correctly
    expected_columns = ['column_one', 'column_two', 'column_three']
    assert list(standardized_df.columns) == expected_columns, "Los nombres no se estandarizaron correctamente"