import logging
import pandas as pd

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Contiene la lógica para procesamientos de datos más avanzados.
    """
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Elimina filas duplicadas del DataFrame.

        Args:
            df (pd.DataFrame): El DataFrame de entrada.

        Returns:
            pd.DataFrame: DataFrame sin filas duplicadas.
        """
        logger.info(f"DataFrame original tiene {len(df)} filas.")
        df_no_duplicates = df.drop_duplicates()
        logger.info(f"DataFrame sin duplicados tiene {len(df_no_duplicates)} filas.")
        logger.info(f"Se eliminaron {len(df) - len(df_no_duplicates)} filas duplicadas.")
        return df_no_duplicates
