import logging
import pandas as pd
from typing import List

# Configuración del logger
logger = logging.getLogger(__name__)

class DataCleaner:
    """
    Clase para realizar la limpieza de datos de los permisos de construcción
    de San Francisco.
    """

    def __init__(self, df: pd.DataFrame):
        """
        Inicializa el DataCleaner con un DataFrame.

        Args:
            df (pd.DataFrame): El DataFrame que se va a limpiar.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Se esperaba un DataFrame de pandas.")
        self.df = df.copy() # Hacemos una copia para evitar efectos secundarios

    def _drop_unnecessary_columns(self, cols_to_drop: List[str]) -> pd.DataFrame:
        """
        Elimina las columnas innecesarias del DataFrame.

        Args:
            cols_to_drop (List[str]): Una lista de nombres de columnas a eliminar.

        Returns:
            pd.DataFrame: DataFrame sin las columnas especificadas.
        """
        logger.info(f"Eliminando {len(cols_to_drop)} columnas innecesarias.")
        df = self.df.drop(columns=cols_to_drop)
        return df

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

        Utiliza una configuración para determinar qué columnas eliminar y
        cuáles convertir a formato de fecha.

        Args:
            config (dict): Un diccionario de configuración con 'cols_to_drop'
                           y 'date_cols'.

        Returns:
            pd.DataFrame: El DataFrame limpio y procesado.
        """
        logger.info("Iniciando el proceso de limpieza de datos.")
        
        # Paso 1: Eliminar columnas
        cols_to_drop = config.get('cols_to_drop', [])
        cleaned_df = self._drop_unnecessary_columns(cols_to_drop)
        
        # Paso 2: Imputar valores faltantes
        cleaned_df = self._impute_missing_values(cleaned_df)

        # Paso 3: Convertir a datetime
        date_cols = config.get('date_cols', [])
        cleaned_df = self._convert_to_datetime(cleaned_df, date_cols)
        
        logger.info("Proceso de limpieza de datos completado exitosamente.")
        return cleaned_df