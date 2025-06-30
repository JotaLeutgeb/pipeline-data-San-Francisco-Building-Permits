import pandas as pd
import glob
import os

print("Starting the script to unify all CSV data...")

# Defines the path where the original CSVs are located
path_source = 'data/source/'

# Find all files ending in .csv in the source path (should be 2 files = accepted and rejected)
files_csv = glob.glob(os.path.join(path_source, "*.csv"))

# We create a list to store each read DataFrame
df_list = []

print(f"Found {len(files_csv)} CSV files in the folder '{path_source}'")

# Loop through each file found, read it, and add it to the list
for file in files_csv:
    print(f"Reading the file: {file}")
    # low_memory=False helps prevent errors with mixed data types in large files
    df_temporal = pd.read_csv(file, low_memory=False)
    df_list.append(df_temporal)

# Concatenate all DataFrames from the list into a single one
print("\nConcatenating all files... This may take a moment.")
df_final = pd.concat(df_list, ignore_index=True)
print(f"Concatenation complete! The final DataFrame has {len(df_final)} rows.")

# Define the destination folder and the final file name
path_destino = 'data/raw/'
nombre_archivo_final = 'unified_data.csv.gz'  # Added .gz for compression
ruta_completa_destino = os.path.join(path_destino, nombre_archivo_final)

# Create the destination folder ('data/raw/') if it does not exist
os.makedirs(path_destino, exist_ok=True)

# Save the final DataFrame
print(f"Saving the final compressed file in: {ruta_completa_destino}")
# index=False is important to not save the pandas index as a column
# compression='gzip' creates a .gz file, saving a lot of disk space
df_final.to_csv(ruta_completa_destino, index=False, compression='gzip')

print("\nProcess finished successfully!")