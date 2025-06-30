import pandas as pd
import re
from unidecode import unidecode
from collections import Counter

import pandas as pd
import re
from unidecode import unidecode
from collections import Counter

def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes column names to be robust, unique, and deterministic,
    regardless of the input column order.

    The process is done in two passes:
    1.  **Census Pass**: Clean all column names and count the frequency of each
        resulting name to identify all potential duplicates beforehand.
    2.  **Assignment Pass**: Iterate again to build the final list of names.
        - Unique names are kept as is.
        - The first occurrence of a duplicated name is kept as is.
        - Subsequent occurrences of a duplicated name are given a suffix (_2, _3, ...).

    Args:
        df: The input DataFrame.

    Returns:
        A new DataFrame with standardized column names.
    """
    df = df.copy()
    
    # Helper function for cleaning a single column name
    def clean_name(col_name: str) -> str:
        s = unidecode(str(col_name)).lower()
        s = re.sub(r'[^a-z0-9]+', '_', s).strip('_')
        return s if s else 'unnamed_column'

    # --- Paso 1: Censo ---
    original_columns = df.columns.tolist()
    cleaned_names = [clean_name(col) for col in original_columns]
    name_counts = Counter(cleaned_names)

    # --- Paso 2: Asignación ---
    final_columns = []
    suffix_tracker = Counter() # Tracks usage of each name during this pass

    for name in cleaned_names:
        # If the name is not a duplicate in the entire set, just use it
        if name_counts[name] == 1:
            final_columns.append(name)
            continue

        # If it is a duplicate, we need to apply suffixes deterministically
        suffix_tracker.update([name])
        current_occurrence = suffix_tracker[name]
        
        if current_occurrence == 1:
            # First time we assign this name, it gets no suffix
            final_columns.append(name)
        else:
            # Subsequent times, it gets a suffix
            final_columns.append(f"{name}_{current_occurrence}")
    
    df.columns = final_columns
    return df

if __name__ == '__main__':
    # Caso 1: ['A', 'B', 'a']
    df1 = pd.DataFrame(columns=['A', 'B', 'a'])
    df1_clean = standardize_column_names(df1)
    print(f"Entrada: {df1.columns.tolist()}")
    print(f"Salida:  {df1_clean.columns.tolist()}")
    # Salida:  ['a', 'b', 'a_2']

    print("-" * 20)

    # Caso 2: ['a', 'B', 'A'] (Orden invertido)
    df2 = pd.DataFrame(columns=['a', 'B', 'A'])
    df2_clean = standardize_column_names(df2)
    print(f"Entrada: {df2.columns.tolist()}")
    print(f"Salida:  {df2_clean.columns.tolist()}")
    # Salida:  ['a', 'b', 'a_2'] -> ¡Exactamente el mismo resultado!
    
    print("-" * 20)

    # Caso 3: Múltiples colisiones
    df3 = pd.DataFrame(columns=['a', 'A', 'contact@email', 'a', 'contact_email'])
    df3_clean = standardize_column_names(df3)
    print(f"Entrada: {df3.columns.tolist()}")
    print(f"Salida:  {df3_clean.columns.tolist()}")
    # Salida:  ['a', 'a_2', 'contact_email', 'a_3', 'contact_email_2']