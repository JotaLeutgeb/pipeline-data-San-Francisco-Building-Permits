# tests/test_config_translator.py

import unittest
from src.data_cleaner import DataCleaner

class TestConfigTranslator(unittest.TestCase):
    """
    Suite de tests unitarios para el método _translate_config de DataCleaner.
    Prueba la lógica de traducción de diccionarios en aislamiento, sin Spark.
    """
    def setUp(self):
        """Prepara un DataCleaner y un mapa de nombres estándar para los tests."""
        self.cleaner = DataCleaner(config={})
        self.name_map = {
            "Record ID": "recordid",
            "Permit Type": "permittype",
            "Estimated Cost": "estimatedcost",
            "Existing Construction Type": "existingconstructiontype",
            "Proposed Construction Type": "proposedconstructiontype",
            "cols_to_drop": "colstodrop" # Un caso donde la clave es también un nombre de columna
        }

    def test_successful_translation(self):
        """
        Prueba que una configuración estándar se traduce correctamente.
        """
        original_config = {
            "cols_to_drop": ["Permit Type", "Estimated Cost"],
            "null_handling_config": {
                "impute_with_median": ["Estimated Cost"],
                "impute_as_category": {"Permit Type": "Ongoing"}
            }
        }
        
        # a 'colstodrop' según las reglas del name_map.
        expected_config = {
            "colstodrop": ["permittype", "estimatedcost"],
            "null_handling_config": {
                "impute_with_median": ["estimatedcost"],
                "impute_as_category": {"permittype": "Ongoing"}
            }
        }

        translated_config = self.cleaner._translate_config(original_config, self.name_map)
        self.assertEqual(translated_config, expected_config)


    def test_unmapped_key_in_list_raises_error(self):
        """
        Prueba que el sistema falla rápido (lanza KeyError) si una columna en una lista
        de la configuración no existe en el mapa de nombres.
        """
        faulty_config = {
            "cols_to_drop": ["Permit Type", "Non Existent Column"]
        }
        
        with self.assertRaisesRegex(KeyError, "Non Existent Column"):
            self.cleaner._translate_config(faulty_config, self.name_map)

    def test_key_and_value_are_translated(self):
        """
        Prueba el caso límite donde tanto la clave como el valor de un par
        en la configuración son nombres de columna que deben ser traducidos.
        """
        edge_case_config = {
            "remap_values": {
                "Existing Construction Type": "Proposed Construction Type"
            }
        }

        expected_config = {
            "remap_values": {
                "existingconstructiontype": "proposedconstructiontype"
            }
        }

        translated_config = self.cleaner._translate_config(edge_case_config, self.name_map)
        self.assertEqual(translated_config, expected_config)
        
    def test_config_key_is_also_a_column_name(self):
        """
        Prueba el caso donde una clave de alto nivel en la config
        (ej: 'cols_to_drop') también podría ser un nombre de columna.
        """
        config = {
            "cols_to_drop": ["Permit Type"] # La clave 'cols_to_drop' está en el name_map
        }

        # La clave de alto nivel 'cols_to_drop' debe traducirse a 'colstodrop'
        expected_config = {
            "colstodrop": ["permittype"]
        }

        translated_config = self.cleaner._translate_config(config, self.name_map)
        self.assertEqual(translated_config, expected_config)


if __name__ == '__main__':
    unittest.main()