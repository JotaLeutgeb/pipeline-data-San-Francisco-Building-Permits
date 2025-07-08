# src/cleaning_report.py

from typing import Dict, List, Any
from dataclasses import dataclass, field
from datetime import datetime
import json

@dataclass
class ColumnDropReport:
    """Reporte detallado de columnas eliminadas."""
    columns_dropped: List[str] = field(default_factory=list)

    @property
    def total_columns_dropped(self) -> int:
        """Calcula el total de columnas eliminadas."""
        return len(self.columns_dropped)

    def add_dropped_column(self, column: str):
        """Agrega una columna a la lista de eliminadas, evitando duplicados."""
        if column and column not in self.columns_dropped:
            self.columns_dropped.append(column)

    def add_dropped_columns(self, columns: List[str]):
        """Agrega una lista de columnas eliminadas."""
        for col in columns:
            self.add_dropped_column(col)
            
    def get_summary(self) -> Dict[str, Any]:
        """Genera un resumen de este reporte."""
        return {
            "columns_dropped": self.columns_dropped,
            "total_columns_dropped": self.total_columns_dropped
        }

@dataclass
class NullHandlingReport:
    """Reporte detallado del manejo de valores nulos."""
    rows_dropped_by_null: Dict[str, int] = field(default_factory=dict)
    imputation_summary: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @property
    def total_rows_dropped(self) -> int:
        """Suma de todas las filas eliminadas por nulos en diferentes columnas."""
        return sum(self.rows_dropped_by_null.values())
        
    @property
    def total_imputations(self) -> int:
        """Suma de todas las celdas imputadas."""
        return sum(details.get("nulls_found", 0) for details in self.imputation_summary.values())

    def add_null_drop(self, column: str, rows_dropped: int):
        """Registra y acumula filas eliminadas para una columna. Ignora si es cero."""
        if rows_dropped > 0:
            self.rows_dropped_by_null[column] = self.rows_dropped_by_null.get(column, 0) + rows_dropped
    
    def add_imputation(self, column: str, method: str, value: Any, nulls_imputed: int):
        """Registra una imputación realizada, usando los nombres de campo que esperan los tests."""
        self.imputation_summary[column] = {
            'method': method,
            'imputation_value': value,
            'nulls_found': nulls_imputed
        }
        
    def get_summary(self) -> Dict[str, Any]:
        """Genera un resumen de este reporte."""
        return {
            "rows_dropped_by_null": self.rows_dropped_by_null,
            "imputation_summary": self.imputation_summary,
            "total_rows_dropped": self.total_rows_dropped,
            "total_imputations": self.total_imputations,
        }

@dataclass
class DuplicateReport:
    """Reporte de duplicados eliminados."""
    # Los nombres de los campos se ajustan a lo que esperan los tests
    duplicates_removed: int = 0
    unique_rows_kept: int = 0
    
    def set_duplicates_found(self, unique_duplicates: int, total_occurrences: int):
        """Establece las métricas de duplicados encontrados."""
        self.unique_rows_dropped = unique_duplicates
        self.total_rows_dropped = total_occurrences
        
    def get_summary(self) -> Dict[str, int]:
        """Genera un resumen de este reporte."""
        return {
            "duplicates_removed": self.duplicates_removed,
            "unique_rows_kept": self.unique_rows_kept,
        }

@dataclass
class DateConversionReport:
    """Reporte detallado de la conversión de columnas de fecha."""
    conversions: Dict[str, Dict[str, int]] = field(default_factory=dict)

    def add_conversion(self, column: str, successes: int = 0, errors: int = 0):
        """Registra conversiones exitosas y errores para una columna."""
        if column not in self.conversions:
            self.conversions[column] = {"successes": 0, "errors": 0}
        
        self.conversions[column]["successes"] += successes
        self.conversions[column]["errors"] += errors

    def get_summary(self) -> Dict[str, Any]:
        """Genera un resumen de este reporte."""
        return {
            "columns_converted": list(self.conversions.keys()),
            "details": self.conversions
        }

@dataclass
class CleaningReport:
    """
    Reporte completo y granular del proceso de limpieza.
    Diseñado para ser un contenedor que se llena durante el pipeline.
    """
    
    # Métricas básicas con valores por defecto. Esto es la clave.
    initial_rows: int = 0
    final_rows: int = 0
    initial_columns: int = 0
    final_columns: int = 0
    
    # Timestamp
    cleaning_timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    # Reportes detallados por operación
    column_drop_report: ColumnDropReport = field(default_factory=ColumnDropReport)
    null_handling_report: NullHandlingReport = field(default_factory=NullHandlingReport)
    duplicate_report: DuplicateReport = field(default_factory=DuplicateReport)
    # CORRECCIÓN: Se añade el reporte de conversión de fechas.
    date_conversion_report: DateConversionReport = field(default_factory=DateConversionReport)
    
    @property
    def total_rows_dropped(self) -> int:
        return self.initial_rows - self.final_rows
    
    @property
    def total_columns_dropped(self) -> int:
        return self.initial_columns - self.final_columns
    
    @property
    def data_retention_rate(self) -> float:
        if self.initial_rows == 0:
            return 0.0
        return (self.final_rows / self.initial_rows) * 100
    
    def get_summary(self) -> Dict[str, Any]:
        """Genera un resumen ejecutivo del proceso de limpieza, con la estructura que esperan los tests."""
        return {
            "data_dimensions": {
                "initial_rows": self.initial_rows,
                "final_rows": self.final_rows,
                "initial_columns": self.initial_columns,
                "final_columns": self.final_columns
            },
            "column_dropping": self.column_drop_report.get_summary(),
            "null_handling": self.null_handling_report.get_summary(),
            "duplicate_handling": self.duplicate_report.get_summary(),
            "date_conversion": self.date_conversion_report.get_summary(), # Se añade al resumen
        }
    
    
    def save_report(self, filepath: str):
        """Guarda el reporte completo en un archivo JSON."""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.get_summary(), f, indent=4, ensure_ascii=False)