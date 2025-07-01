import unittest
import pandas as pd
import numpy as np
from src.data_cleaner import DataCleaner

class TestDataCleaner(unittest.TestCase):
    """
    Suite de pruebas para la clase DataCleaner.
    """

    def setUp(self):
        """
        Configura los datos de prueba antes de cada test.
        """
        # CORRECCIÓN: Se añade un 'St' extra para que sea la moda inequívoca.
        data = {
            'Permit Number': [1, 2, 3, 4],
            'Permit Type': [1, 2, 3, 1],
            'Street Suffix': ['St', 'Av', 'St', np.nan], # <-- 'St' es ahora la moda
            'Fire Only Permit': ['Y', np.nan, 'N', 'Y'],
            'Permit Creation Date': ['20230101', '20230102', '20230103', '20230104'],
            'Existing Use': ['office', 'retail', 'residential', 'office'],
            'Zipcode': [94102, 94103, 94102, 94102],
            'useless_col': [0, 0, 0, 0]
        }
        self.df = pd.DataFrame(data)
        self.config = {
            'cols_to_drop': ['useless_col'],
            'date_cols': ['Permit Creation Date']
        }

    def test_initialization(self):
        """
        Prueba que la clase se inicializa correctamente con un DataFrame.
        """
        cleaner = DataCleaner(self.df)
        self.assertIsInstance(cleaner.df, pd.DataFrame)
        # Prueba que se lanza un error si no se pasa un DataFrame
        with self.assertRaises(TypeError):
            DataCleaner("no es un dataframe")

    def test_drop_unnecessary_columns(self):
        """
        Prueba la eliminación de columnas.
        """
        cleaner = DataCleaner(self.df)
        cleaned_df = cleaner.clean_data(self.config)
        self.assertNotIn('useless_col', cleaned_df.columns)
        self.assertIn('Permit Number', cleaned_df.columns)

    def test_impute_missing_values(self):
        """
        Prueba la imputación de valores faltantes.
        """
        cleaner = DataCleaner(self.df)
        cleaned_df = cleaner.clean_data(self.config)

        # 'Fire Only Permit' NaN debe ser 'N'
        self.assertEqual(cleaned_df.loc[1, 'Fire Only Permit'], 'N')
        # 'Street Suffix' NaN debe ser imputado con la moda ('St')
        # Con los datos corregidos, esta aserción ahora siempre será verdadera.
        self.assertEqual(cleaned_df.loc[3, 'Street Suffix'], 'St')

    def test_convert_to_datetime(self):
        """
        Prueba la conversión de columnas a formato datetime.
        """
        cleaner = DataCleaner(self.df)
        cleaned_df = cleaner.clean_data(self.config)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(cleaned_df['Permit Creation Date']))

    def test_clean_data_pipeline(self):
        """
        Prueba el pipeline de limpieza de datos completo.
        """
        cleaner = DataCleaner(self.df)
        cleaned_df = cleaner.clean_data(self.config)

        # Verificar que se ejecutaron todos los pasos
        self.assertNotIn('useless_col', cleaned_df.columns)
        self.assertFalse(cleaned_df['Fire Only Permit'].isnull().any())
        self.assertFalse(cleaned_df['Street Suffix'].isnull().any())
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(cleaned_df['Permit Creation Date']))
        # El DataFrame de salida debe tener el mismo número de filas que el original
        self.assertEqual(len(cleaned_df), len(self.df))

if __name__ == '__main__':
    unittest.main()