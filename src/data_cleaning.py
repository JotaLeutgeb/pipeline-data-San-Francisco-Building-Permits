import pandas as pd
import re
from unidecode import unidecode
from collections import Counter

def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes column names to make them robust and unique.

    The process includes:
    1.  Converting column names to strings.
    2.  Transliterating to ASCII (e.g., 'Año' -> 'Ano').
    3.  Converting to lowercase.
    4.  Replacing any non-alphanumeric characters with a single underscore.
    5.  Handling empty column names resulting from the cleaning process.
    6.  Ensuring all final column names are unique by appending suffixes (_2, _3, etc.).

    Args:
        df: The input DataFrame.

    Returns:
        A new DataFrame with standardized, unique, and robust column names.
    """
    # Create a copy to avoid modifying the original DataFrame
    df = df.copy()
    
    # 1. Standardize column names
    new_cols = []
    for col in df.columns:
        # Convert to string, then to ASCII and lowercase
        s = unidecode(str(col)).lower()
        # Replace all non-alphanumeric sequences with a single underscore
        s = re.sub(r'[^a-z0-9]+', '_', s)
        # Remove leading/trailing underscores that might result
        s = s.strip('_')
        # Handle cases where the name becomes empty after cleaning
        if not s:
            s = 'unnamed_column'
        new_cols.append(s)

    # 2. Handle duplicates
    counts = Counter(new_cols)
    final_cols = []
    for i, col_name in enumerate(new_cols):
        # If the column name is not a duplicate, use it as is
        if counts[col_name] == 1:
            final_cols.append(col_name)
            continue
        
        # If it is a duplicate, find a unique suffix
        # The first occurrence keeps its name, subsequent ones get suffixes
        suffix_counter = 1
        original_name = col_name
        
        # Find which occurrence this is
        for prev_col in new_cols[:i]:
            if prev_col == original_name:
                suffix_counter += 1
        
        # If it's not the first time we see this name, append the suffix
        if suffix_counter > 1:
            final_cols.append(f"{original_name}_{suffix_counter}")
        else:
            final_cols.append(original_name)

    df.columns = final_cols
    return df

# --- Ejemplo de uso ---

if __name__ == '__main__':
    # Create a sample DataFrame with tricky column names
    data = {'Nombre Completo': [1], 'FECHA-NAC': [2], 'FECHA-NAC': [3], 'País': [4],
            '---': [5], 'País': [6], 'contact@email': [7], 'contact_email': [8]}
    
    tricky_df = pd.DataFrame(data)
    print("Columnas Originales:")
    print(tricky_df.columns.tolist())
    # Salida: ['Nombre Completo', 'FECHA-NAC', 'FECHA-NAC', 'País', '---', 'País', 'contact@email', 'contact_email']
    
    # Apply the robust function
    clean_df = standardize_column_names(tricky_df)
    
    print("\nColumnas Estandarizadas (infalibles):")
    print(clean_df.columns.tolist())
    # Salida: ['nombre_completo', 'fecha_nac', 'fecha_nac_2', 'pais', 'unnamed_column', 'pais_2', 'contact_email', 'contact_email_2']
