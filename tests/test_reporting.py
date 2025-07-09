# tests/test_reporting.py
import json

from src.cleaning_report import CleaningReport, ColumnDropReport, NullHandlingReport, DuplicateReport

# Funciones Anteriores (las clases TestColumnDropReport, TestNullHandlingReport y TestDuplicateReport están correctas y se mantienen)

class TestCleaningReport:
    """Tests para el reporte de limpieza principal.
    CORRECCIÓN: Esta clase ha sido reescrita para que coincida con la implementación actual de CleaningReport.
    """

    def test_cleaning_report_initialization_and_properties(self):
        """
        Test para la inicialización y el estado inicial coherente del reporte de limpieza.
        """
        # Crear el objeto con los parámetros correctos del constructor
        report = CleaningReport(
            initial_rows=1000,
            initial_columns=10,
            final_rows=900,
            final_columns=8
        )
        
        # Verificación de la asignación directa y propiedades calculadas
        assert report.initial_rows == 1000
        assert report.initial_columns == 10
        assert report.final_rows == 900
        assert report.final_columns == 8
        assert report.total_rows_dropped == 100
        assert report.total_columns_dropped == 2
        assert report.data_retention_rate == 90.0

        # Verificación de la inicialización de sub-objetos
        assert isinstance(report.column_drop_report, ColumnDropReport)
        assert isinstance(report.null_handling_report, NullHandlingReport)
        assert isinstance(report.duplicate_report, DuplicateReport)
        
        # Nos aseguramos de que los reportes internos comiencen vacíos.
        assert report.column_drop_report.total_columns_dropped == 0
        assert report.null_handling_report.total_rows_dropped == 0
        assert report.duplicate_report.duplicates_removed == 0

    def test_generate_full_summary(self):
        """Test generación del resumen completo."""
        main_report = CleaningReport(initial_rows=1000, initial_columns=10, final_rows=865, final_columns=8)
        
        # Simular el llenado de los reportes
        main_report.column_drop_report.add_dropped_columns(["col_a", "col_b"])
        main_report.null_handling_report.add_null_drop("col_c", 10)
        main_report.null_handling_report.add_imputation("col_d", "mean", 55.5, 5)
        main_report.duplicate_report.duplicates_removed = 25
        main_report.duplicate_report.unique_rows_kept = 900 # Ejemplo

        summary = main_report.get_summary()

        assert "data_dimensions" in summary
        assert summary["data_dimensions"]["initial_rows"] == 1000
        assert summary["data_dimensions"]["final_columns"] == 8
        
        assert "column_dropping" in summary
        assert summary["column_dropping"]["total_columns_dropped"] == 2
        
        assert "null_handling" in summary
        assert summary["null_handling"]["total_rows_dropped"] == 10
        assert summary["null_handling"]["total_imputations"] == 5

        assert "duplicate_handling" in summary
        assert summary["duplicate_handling"]["duplicates_removed"] == 25
        
    def test_save_report_to_json(self, tmp_path):
        """Test para guardar el reporte en un archivo JSON."""
        # tmp_path es una fixture de pytest que crea un directorio temporal
        report_path = tmp_path / "test_report.json"
        
        main_report = CleaningReport(initial_rows=1000, initial_columns=10, final_rows=900, final_columns=8)
        main_report.column_drop_report.add_dropped_columns(["col1", "col2"])
        
        main_report.save_report(str(report_path))
        
        # Verificar que el archivo fue creado
        assert report_path.exists()
        
        # Leer el archivo y verificar su contenido
        with open(report_path, 'r') as f:
            data = json.load(f)
            
        assert data["data_dimensions"]["initial_rows"] == 1000
        assert data["column_dropping"]["total_columns_dropped"] == 2