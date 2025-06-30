# 1. Importaciones de librerías estándar y de terceros
import pytest
import pandas as pd
from collections import Counter

# Asumimos que la función está en este archivo, o la importamos desde src.data_cleaning
from src.data_cleaning import standardize_column_names

# --- 1. Definimos nuestros "sets de ingredientes" (los casos de prueba) ---
# Cada tupla contiene: (lista_de_columnas_originales, lista_esperada_después_de_limpiar, id_del_test)
test_cases = [
    (
        ['Nombre Completo', 'FECHA-NAC', 'País'],
        ['nombre_completo', 'fecha_nac', 'pais'],
        "basic_cleaning" # ID para que el reporte de pytest sea más claro
    ),
    (
        ['Año de Nacimiento', 'Teléfono Móvil'],
        ['ano_de_nacimiento', 'telefono_movil'],
        "special_chars_and_accents"
    ),
    (
        # 'contact@email' y 'contact_email' se convierten en 'contact_email'
        ['contact@email', 'contact_email', 'contact@email'],
        ['contact_email', 'contact_email_2', 'contact_email_3'],
        "handle_duplicates"
    ),
    (
        # '---' y '_' se limpian y quedan vacíos
        ['A', '---', 'B', '_'],
        ['a', 'unnamed_column', 'b', 'unnamed_column_2'],
        "handle_empty_after_clean"
    ),
    (
        # Columnas numéricas y columnas que no cambian
        ['column_a', 123, 'column_b'],
        ['column_a', '123', 'column_b'],
        "handle_numeric_columns"
    ),
    (
        [],
        [],
        "empty_dataframe"
    ),
    (
        ['A', 'B', 'a'],
        ['a', 'b', 'a_2'],
        "case_sensitive_handling"
    ),
    (   ['a', 'B', 'A'],
        ['a', 'b', 'a_2'],  
        "case_sensitive_reversed_order"
    ),
    (
        ['a', 'A', 'contact@email', 'a', 'contact_email'],
        ['a', 'a_2', 'contact_email', 'a_3', 'contact_email_2'],
        "multiple_collisions"
    )
]

# --- 2. Usamos el decorador @pytest.mark.parametrize para crear el test ---
@pytest.mark.parametrize("input_columns, expected_columns, test_id", test_cases, ids=[case[2] for case in test_cases])
def test_standardize_column_names(input_columns, expected_columns, test_id):
    """
    Test que verifica la estandarización de columnas para múltiples casos de prueba.
    """
    # a. Creamos un DataFrame de prueba con las columnas de entrada
    # El contenido de los datos no importa, solo los nombres de las columnas
    df_input = pd.DataFrame(columns=input_columns)
    
    # b. Aplicamos la función que queremos probar
    df_processed = standardize_column_names(df_input)
    
    # c. Verificamos que el resultado sea el esperado
    actual_columns = df_processed.columns.tolist()
    
    assert actual_columns == expected_columns, f"Test case '{test_id}' failed!"
