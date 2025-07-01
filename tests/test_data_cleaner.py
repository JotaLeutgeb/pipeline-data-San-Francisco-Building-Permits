import unittest
import pandas as pd
import numpy as np
import sys
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

from src.data_cleaner import DataCleaner

class TestDataCleaner(unittest.TestCase):
    """
    Suite de pruebas para la clase DataCleaner y su pipeline de limpieza.
    """

    def setUp(self):
        """Configura datos de prueba válidos para cada test."""
        data = {
            'Permit Number':      [1, 2, 3, 4, 1],
            'Permit Type':        [1, 2, 3, 1, 1],
            'Permit type':        ['A', 'B', 'A', 'C', 'A'],
            'Street Suffix':      ['St', 'Av', 'St', np.nan, 'St'],
            'Fire Only Permit':   ['Y', np.nan, 'N', 'Y', 'Y'],
            'Permit Creation Date': ['20230101', '20230102', '20230103', '20230104', '20230101'],
            'useless_col':        [0, 0, 0, 0, 0]
        }
        self.df = pd.DataFrame(data)
        
        self.config = {
            'cols_to_drop': ['uselesscol'],
            'date_cols': ['permitcreationdate'],
            'imputation_rules': {
                'fireonlypermit': {'strategy': 'constant', 'value': 'N'},
                'streetsuffix':   {'strategy': 'mode'}
            }
        }

    def test_standardize_is_order_independent(self):
        """Verifica que la estandarización de columnas es determinista."""
        data = {'Permit Type': [1], 'permit type': [2], 'Permit Status': [3]}
        df1 = pd.DataFrame(data, columns=['Permit Type', 'permit type', 'Permit Status'])
        df2 = pd.DataFrame(data, columns=['permit type', 'Permit Status', 'Permit Type'])

        cleaner1 = DataCleaner(df1)
        cleaner1._standardize_column_names()
        cleaner2 = DataCleaner(df2)
        cleaner2._standardize_column_names()

        expected_columns = ['permitstatus', 'permittype_0', 'permittype_1']
        self.assertListEqual(list(cleaner1.df.columns), expected_columns)
        self.assertListEqual(list(cleaner2.df.columns), expected_columns)

    def test_impute_missing_values(self):
        """Prueba la imputación de valores faltantes después de la estandarización."""
        cleaner = DataCleaner(self.df)
        cleaner._standardize_column_names()
        cleaner._impute_missing_values(self.config)

        self.assertEqual(cleaner.df.loc[1, 'fireonlypermit'], 'N')
        self.assertEqual(cleaner.df.loc[3, 'streetsuffix'], 'St')

    def test_configurable_imputation(self):
        """Verifica que la imputación configurable funciona para diferentes estrategias."""
        # FIX: Todas las listas ahora tienen la misma longitud (4 elementos).
        imputation_data = {
            'col_constante': ['A', 'B', 'A', np.nan],
            'col_moda':      [10, 20, 10, np.nan],
            'col_media':     [10.0, 20.0, 30.0, np.nan]
        }
        test_df = pd.DataFrame(imputation_data)
        test_config = {
            "imputation_rules": {
                "col_constante": { "strategy": "constant", "value": "C" },
                "col_moda":      { "strategy": "mode" },
                "col_media":     { "strategy": "mean" }
            }
        }
        cleaner = DataCleaner(test_df)
        cleaner._impute_missing_values(test_config)
        result_df = cleaner.df
        self.assertEqual(result_df['col_constante'].iloc[3], 'C')
        self.assertEqual(result_df['col_moda'].iloc[3], 10)
        self.assertAlmostEqual(result_df['col_media'].iloc[3], 20.0)

    def test_full_cleaning_pipeline(self):
        """Prueba el pipeline completo con el método 'run_cleaning_pipeline'."""
        cleaner = DataCleaner(self.df)
        cleaned_df = cleaner.run_cleaning_pipeline(self.config)

        self.assertEqual(len(cleaned_df), 4)
        expected_cols = ['fireonlypermit', 'permitcreationdate', 'permitnumber', 'permittype_0', 'permittype_1', 'streetsuffix']
        self.assertListEqual(sorted(list(cleaned_df.columns)), expected_cols)
        self.assertFalse(cleaned_df['fireonlypermit'].isnull().any())
        self.assertFalse(cleaned_df['streetsuffix'].isnull().any())
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(cleaned_df['permitcreationdate']))

if __name__ == '__main__':
    unittest.main()