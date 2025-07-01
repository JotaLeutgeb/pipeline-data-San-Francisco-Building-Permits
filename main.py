# main.py

import pandas as pd
import logging
import os
from src.data_cleaner import DataCleaner
from configs.config import RAW_DATA_DIR, PROCESSED_DATA_DIR, CONFIGS_DIR

# --- Configure Logging ---
# A best practice to see the pipeline's progress and debug issues.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

def main():
    """
    Main function to orchestrate the data loading, cleaning, and saving process.
    """
    logging.info("Starting the main application.")
    
    # --- 1. Define Paths ---
    # Using the centralized config for robustness.
    try:
        raw_data_path = os.path.join(RAW_DATA_DIR, 'building-permits.csv')
        processed_data_path = os.path.join(PROCESSED_DATA_DIR, 'building-permits-cleaned.csv')
        config_path = os.path.join(CONFIGS_DIR, 'cleaning_config.yaml')
        
        # --- 2. Load Data ---
        logging.info(f"Loading raw data from: {raw_data_path}")
        df_raw = pd.read_csv(raw_data_path, low_memory=False)
        
        # --- 3. Clean Data ---
        # Instantiate the cleaner and run the full pipeline.
        cleaner = DataCleaner(df=df_raw, config_path=config_path)
        df_clean = cleaner.run_pipeline()
        
        # --- 4. Save Data ---
        logging.info(f"Saving cleaned data to: {processed_data_path}")
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True) # Ensure the directory exists
        df_clean.to_csv(processed_data_path, index=False)
        
        logging.info("✅ Application finished successfully.")
        
    except FileNotFoundError as e:
        logging.error(f"Critical Error: A file was not found. Please check paths. Details: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred in the main pipeline.", exc_info=True)


if __name__ == '__main__':
    main()