# src/config_loader.py

import importlib.util
from typing import Dict, Any

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Carga dinámicamente un archivo de configuración de Python.

    Args:
        config_path: La ruta al archivo config.py.

    Returns:
        Un diccionario que contiene todas las variables en mayúsculas del archivo
        de configuración.
    """
    spec = importlib.util.spec_from_file_location("config", config_path)
    config_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_module)

    # Extraer todas las variables en mayúsculas y sin guiones bajos iniciales
    config_dict = {
        key: getattr(config_module, key)
        for key in dir(config_module)
        if key.isupper() and not key.startswith('_')
    }
    return config_dict