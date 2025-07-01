# src/data_cleaner.py

import pandas as pd
import yaml
import logging
import re
from collections import Counter
from unidecode import unidecode

class DataCleaner:
    """
    A class to encapsulate the data cleaning pipeline for a given DataFrame.
    It is driven by a YAML configuration file that specifies all cleaning steps.
    """
    def __init__(self, df: pd.DataFrame, config_path: str):
        """
        Initializes the DataCleaner with a DataFrame and a path to a config file.
        
        Args:
            df (pd.DataFrame): The raw DataFrame to be cleaned.
            config_path (str): The file path for the cleaning_config.yaml.
        """
        self.df = df.copy()
        self.config = self._load_config(config_path)
        logging.info("DataCleaner initialized.")

    def _load_config(self, config_path: str) -> dict:
        """Safely loads and returns the YAML configuration."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logging.error(f"Configuration file not found at {config_path}")
            raise
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML file: {e}")
            raise

    def standardize_column_names(self):
        """Applies deterministic column name standardization."""
        logging.info("Standardizing column names...")
        df_copy = self.df.copy()
        
        def clean_name(col_name: str) -> str:
            s = unidecode(str(col_name)).lower()
            s = re.sub(r'[^a-z0-9]+', '_', s).strip('_')
            return s if s else 'unnamed_column'
            
        original_cols = df_copy.columns.tolist()
        cleaned_names = [clean_name(col) for col in original_cols]
        name_counts = Counter(cleaned_names)
        
        final_columns = []
        suffix_tracker = Counter()
        for name in cleaned_names:
            if name_counts[name] == 1:
                final_columns.append(name)
                continue
            suffix_tracker.update([name])
            current_occurrence = suffix_tracker[name]
            if current_occurrence == 1:
                final_columns.append(name)
            else:
                final_columns.append(f"{name}_{current_occurrence}")
                
        df_copy.columns = final_columns
        self.df = df_copy
        return self

    def handle_missing_values(self):
        """Handles missing values based on the rules in the config file."""
        logging.info("Handling missing values...")
        if 'null_threshold_drop' in self.config:
            threshold = self.config['null_threshold_drop']
            null_pct = self.df.isnull().sum() / len(self.df)
            cols_to_drop = null_pct[null_pct > threshold].index
            self.df.drop(columns=cols_to_drop, inplace=True)
            logging.info(f"Dropped columns by threshold: {list(cols_to_drop)}")

        rules = self.config.get('null_handling', {})
        
        # --- FIX APPLIED HERE ---
        if 'impute_from_other_column' in rules:
            for rule in rules['impute_from_other_column']:
                target, source = rule['target_column'], rule['source_column']
                if target in self.df.columns and source in self.df.columns:
                    # Instead of inplace=True, we reassign the column
                    self.df[target] = self.df[target].fillna(self.df[source])

        # --- AND FIX APPLIED HERE ---
        if 'fill_with_value' in rules:
            for rule in rules['fill_with_value']:
                value = rule['value']
                for col in rule['columns']:
                    if col in self.df.columns:
                        # Reassign the column here as well
                        self.df[col] = self.df[col].fillna(value)
        return self

    def correct_data_types(self):
        """Corrects data types based on the rules in the config file."""
        logging.info("Correcting data types...")
        rules = self.config.get('type_conversion', {})
        
        for rule in rules.get('to_datetime', []):
            if 'ends_with' in rule:
                suffix = rule['ends_with']
                for col in self.df.columns:
                    if col.endswith(suffix):
                        self.df[col] = pd.to_datetime(self.df[col], errors='coerce')

        for rule in rules.get('to_numeric', []):
            if 'contains' in rule:
                substring = rule['contains']
                for col in self.df.columns:
                    if substring in col:
                        self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
        return self

    def run_pipeline(self) -> pd.DataFrame:
        """Runs the full cleaning pipeline by chaining all cleaning methods."""
        logging.info("Data cleaning pipeline started.")
        self.standardize_column_names()
        self.handle_missing_values()
        self.correct_data_types()
        logging.info("Data cleaning pipeline finished successfully.")
        return self.df