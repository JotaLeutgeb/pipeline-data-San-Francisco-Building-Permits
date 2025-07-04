import unittest
import pandas as pd
import numpy as np
import sys
import os

# Añadir el directorio raíz al path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

from src.data_cleaner import DataCleaner

class TestDataCleaner(unittest.TestCase):
    """
    Suite de pruebas unificada para la clase DataCleaner.
    """

    def setUp(self):
        """
        Este método se ejecuta ANTES de cada 'test_*' y prepara los datos
        y la configuración de prueba.
        """
        # Un DataFrame de prueba que contiene todos los casos que queremos probar.
        self.test_data = {
            'Record ID': [1, 2, 3, 4, np.nan],                           # --> Fila con NaN será eliminada
            'Permit Type': ['A', 'B', 'A', np.nan, 'A'],                 # --> Nulo se imputará con moda 'A'
            'Completed Date': ['2023-01-01', np.nan, '2023-02-01', '2023-03-01', '2023-04-01'], # --> Nulo se imputará con 'Ongoing'
            'Estimated Cost': [100.0, 200.0, 600.0, np.nan, 300.0],       # --> Nulo se imputará con la mediana
            'PERMIT CREATION DATE': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'], # --> Para estandarización
            'col_a_eliminar': [1, 2, 3, 4, 5]
        }
        self.df = pd.DataFrame(self.test_data)
        
        # Una configuración de prueba que refleja la estructura de la configuración principal.
        self.test_config = {
            "cols_to_drop": ["col_a_eliminar"],
            "NULL_HANDLING_CONFIG": {
                "drop_rows_if_null": ["Record ID"],
                "impute_as_category": {"Completed Date": "Ongoing"},
                "impute_with_median": ["Estimated Cost"],
                "impute_with_mode": ["Permit Type"]
            }
        }

    def test_drop_rows_on_null(self):
        """Prueba que las filas se eliminan correctamente."""
        cleaner = DataCleaner(self.df, self.test_config)
        cleaner.handle_null_values()
        # El DataFrame original tiene 5 filas, una con 'Record ID' nulo. Deberían quedar 4.
        self.assertEqual(len(cleaner.df), 4)
        self.assertFalse(cleaner.df['Record ID'].isnull().any())

    def test_impute_as_category(self):
        """Prueba la imputación de nulos con un valor categórico constante."""
        cleaner = DataCleaner(self.df, self.test_config)
        cleaner.handle_null_values()
        # La fila con índice 1 tenía un NaN en 'Completed Date'
        # Después de eliminar la fila 4, la fila con ID 2 (índice 1) debería tener 'Ongoing'.
        self.assertEqual(cleaner.df[cleaner.df['Record ID'] == 2]['Completed Date'].iloc[0], 'Ongoing')
        
    def test_impute_with_median(self):
        """Prueba la imputación de nulos con la mediana."""
        # Mediana de [100.0, 200.0, 600.0, 300.0] es (200+300)/2 = 250.0
        cleaner = DataCleaner(self.df, self.test_config)
        cleaner.handle_null_values()
        expected_median = 200.0
        self.assertEqual(cleaner.df[cleaner.df['Record ID'] == 4]['Estimated Cost'].iloc[0], expected_median)

    def test_impute_with_mode(self):
        """Prueba la imputación de nulos con la moda."""
        # La moda de ['A', 'B', 'A', 'A'] (sin contar el NaN) es 'A'
        cleaner = DataCleaner(self.df, self.test_config)
        cleaner.handle_null_values()
        expected_mode = 'A'
        # La fila con ID 4 (índice 3) tenía el NaN en 'Permit Type'
        self.assertEqual(cleaner.df[cleaner.df['Record ID'] == 4]['Permit Type'].iloc[0], expected_mode)
        
    def test_standardize_column_names(self):
        """Prueba la estandarización de nombres de columnas."""
        cleaner = DataCleaner(self.df, self.test_config)
        cleaner._standardize_column_names()
        self.assertIn('permitcreationdate', cleaner.df.columns)
        self.assertNotIn('PERMIT CREATION DATE', cleaner.df.columns)

    def test_full_cleaning_pipeline(self):
        """Prueba de integración: el pipeline completo debe ejecutarse sin errores."""
        cleaner = DataCleaner(self.df, self.test_config)
        cleaned_df = cleaner.run_cleaning_pipeline()
        
        # 1. Verificar que la fila con ID nulo fue eliminada.
        self.assertEqual(len(cleaned_df), 4)
        self.assertNotIn(np.nan, cleaned_df['recordid'])
        
        # 2. Verificar que la columna fue eliminada.
        self.assertNotIn('col_a_eliminar', cleaned_df.columns)
        
        # 3. Verificar que los nombres de columnas están estandarizados y en minúsculas.
        expected_cols_subset = ['recordid', 'permittype', 'completeddate', 'estimatedcost', 'permitcreationdate']
        for col in expected_cols_subset:
            self.assertIn(col, cleaned_df.columns)
            
        # 4. Verificar que no quedan nulos en las columnas tratadas.
        self.assertFalse(cleaned_df[['permittype', 'completeddate', 'estimatedcost']].isnull().any().any())

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
