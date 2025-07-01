import logging
import pandas as pd
from typing import List, Dict

# Configuración del logger
logger = logging.getLogger(__name__)

class DataCleaner:
    """
    Clase para realizar la limpieza de datos de los permisos de construcción
    de San Francisco.
    """
    def __init__(self, df: pd.DataFrame):
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Se esperaba un DataFrame de pandas.")
        self.df = df.copy()

    def _standardize_column_names(self) -> pd.DataFrame:
        """
        Estandariza los nombres de las columnas para que sean deterministas
        e independientes del orden original.
        - Convierte a minúsculas.
        - Reemplaza caracteres no alfanuméricos con guiones bajos.
        - Maneja nombres de columna duplicados añadiendo un sufijo numérico.
        """
        logger.info("Estandarizando nombres de columnas.")
        original_cols = self.df.columns
        cleaned_cols = [
            ''.join(filter(str.isalnum, col.lower().replace(' ', '_')))
            for col in original_cols
        ]

        # Primera pasada: Contar ocurrencias de cada nombre limpio
        counts: Dict[str, int] = {}
        for col_name in cleaned_cols:
            counts[col_name] = counts.get(col_name, 0) + 1

        # Segunda pasada: Construir los nombres finales
        final_cols = []
        # Reiniciar contadores para aplicar sufijos
        suffix_counts: Dict[str, int] = {}
        for col_name in cleaned_cols:
            # Si el nombre no es único, añadir sufijo
            if counts[col_name] > 1:
                current_suffix = suffix_counts.get(col_name, 0)
                final_cols.append(f"{col_name}_{current_suffix}")
                suffix_counts[col_name] = current_suffix + 1
            else:
                final_cols.append(col_name)

        df_renamed = self.df.copy()
        df_renamed.columns = final_cols
        logger.info(f"Nombres de columnas estandarizados: {final_cols}")
        return df_renamed

    def _impute_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Imputa los valores faltantes en columnas específicas.

        Se imputan valores para 'Fire Only Permit' y 'Street Suffix'.

        Args:
            df (pd.DataFrame): DataFrame con valores faltantes.

        Returns:
            pd.DataFrame: DataFrame con valores imputados.
        """
        logger.info("Imputando valores faltantes.")
        # Imputar 'Fire Only Permit' con 'N' (No)
        df['Fire Only Permit'] = df['Fire Only Permit'].fillna('N')
        
        # Imputar 'Street Suffix' con el valor más frecuente (moda)
        if 'Street Suffix' in df.columns and df['Street Suffix'].isnull().any():
            mode_suffix = df['Street Suffix'].mode()[0]
            logger.info(f"Imputando 'Street Suffix' con la moda: {mode_suffix}")
            df['Street Suffix'] = df['Street Suffix'].fillna(mode_suffix)
        return df

    def _convert_to_datetime(self, df: pd.DataFrame, date_cols: List[str]) -> pd.DataFrame:
        """
        Convierte las columnas de fecha a formato datetime.

        Args:
            df (pd.DataFrame): DataFrame con columnas de fecha como strings.
            date_cols (List[str]): Lista de columnas para convertir a datetime.

        Returns:
            pd.DataFrame: DataFrame con columnas de fecha en formato datetime.
        """
        logger.info(f"Convirtiendo columnas a datetime: {date_cols}")
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df = df.dropna(subset=[col])
                df[col] = df[col].astype(int)
                df[col] = pd.to_datetime(df[col], format='%Y%m%d')
        return df

    def clean_data(self, config: dict) -> pd.DataFrame:
        """
        Ejecuta el pipeline completo de limpieza de datos.
        """
        logger.info("Iniciando el proceso de limpieza de datos.")
        
        # Paso 1: Estandarizar nombres de columnas (AHORA ES EL PRIMER PASO)
        processed_df = self._standardize_column_names()

        # ... los demás pasos se aplicarían a 'processed_df' ...
        # Por ahora, solo devolvemos el DataFrame con columnas renombradas.
        
        logger.info("Proceso de limpieza de datos completado exitosamente.")
        return processed_df
