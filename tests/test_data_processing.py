#tests/test_data_processing.py

import unittest
import pandas as pd
from src.data_processing import DataProcessor

class TestDataProcessor(unittest.TestCase):

    def setUp(self):
        self.processor = DataProcessor()
        self.data_with_duplicates = pd.DataFrame({
            'col1': ['A', 'B', 'A'],
            'col2': [1, 2, 1]
        })
        self.data_without_duplicates = pd.DataFrame({
            'col1': ['A', 'B', 'C'],
            'col2': [1, 2, 3]
        })

    def test_remove_duplicates(self):
        """
        Verifica que las filas duplicadas se eliminan correctamente.
        """
        result_df = self.processor.remove_duplicates(self.data_with_duplicates)
        self.assertEqual(len(result_df), 2)

    def test_remove_duplicates_no_duplicates(self):
        """
        Verifica que no se eliminan filas si no hay duplicados.
        """
        result_df = self.processor.remove_duplicates(self.data_without_duplicates)
        self.assertEqual(len(result_df), 3)

if __name__ == '__main__':
    unittest.main()