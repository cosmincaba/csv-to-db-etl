"""
Pipeline metrics and reporting.

Tracks execution metrics, data quality and performance statistics.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from src.logger import setup_logger

logger = setup_logger(__name__)

class PipelineMetrics:
    """
    Collect and report pipeline execution metrics.

    Tracks:
    - Execution time and status
    - Row counts at each stage
    - Data quality metrics
    - File information
    """
    def __init__(self, pipeline_name: str, config_path: str):
        """
        Args:
            pipeline_name: Name of the pipeline
            config_path: Path to config file used
        """
        self.pipeline_name = pipeline_name
        self.config_path = config_path
        self.execution_id = self._generate_execution_id()
        self.start_time = datetime.now()
        self.end_time = None
        self.duration_seconds = None

        # Metrics
        self.exit_code = None
        self.status = None
        self.rows_extracted = 0
        self.rows_validated = 0
        self.rows_rejected = 0
        self.rows_transformed = 0
        self.rows_loaded = 0

        # Data quality
        self.null_errors = 0
        self.type_errors = 0
        self.duplicate_errors = 0
        self.validation_errors = {}

        # Files
        self.input_file = None
        self.input_file_size_bytes = None
        self.rejected_file = None
        self.clean_file = None

        logger.info(f"Metrics tracking started for execution: {self.execution_id}")

    def _generate_execution_id(self) -> str:
        """
        Generate unique execution ID.

        Returns:
            Execution ID in format: YYYYMMDD_HHMMSS_random
        """
        import random
        import string

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        return f"{timestamp}_{random_suffix}"

    def set_input_file(self, file_path: str):
        """Record input file information."""
        self.input_file = file_path

        try:
            path = Path(file_path)
            if path.exists():
                self.input_file_size_bytes = path.stat().st_size
        except Exception as e:
            logger.warning(f"Could not get file size: {e}")

    def set_extraction_metrics(self, rows_extracted: int):
        """Record extraction metrics."""
        self.rows_extracted = rows_extracted
        logger.debug(f"Extraction metrics: {rows_extracted} rows")

    def set_validation_metrics(self, rows_valid: int, rows_rejected: int):
        """Record validation metrics."""
        self.rows_validated = rows_valid
        self.rows_rejected = rows_rejected
        logger.debug(f"Transformation metrics: {rows_valid} rows")

    def set_transformation_metrics(self, rows_transformed: int):
        """Record transformation metrics."""
        self.rows_transformed = rows_transformed

    def set_load_metrics(self, rows_loaded: int):
        """Record load metrics."""
        self.rows_loaded = rows_loaded
        logger.debug(f"Load metrics: {rows_loaded} rows")

    def set_output_files(self, rejected_file: Optional[str] = None, clean_file: Optional[str] = None):
        self.rejected_file = rejected_file
        self.clean_file = clean_file

    def add_validation_error(self, error_type: str, count: int = 1):
        """Record validation error by type."""
        if error_type not in self.validation_errors:
            self.validation_errors[error_type] = 0
        self.validation_errors[error_type] += count

    def complete(self, exit_code: int, status: str):
        """Mark pipeline execution as complete."""
        self.end_time = datetime.now()
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()
        self.exit_code = exit_code
        self.status = status

        logger.info(f"Pipeline completed in {self.duration_seconds:.2f} seconds with status: {status}")

    def calculate_rejection_rate(self) -> float:
        """Calculate rejection rate as percentage."""
        if self.rows_extracted == 0:
            return 0.0
        return (self.rows_rejected / self.rows_extracted) * 100

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "execution_id": self.execution_id,
            "config_path": self.config_path,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": round(self.duration_seconds, 2) if self.duration_seconds else None,
            "exit_code": self.exit_code,
            "status": self.status,
            "metrics": {
                "rows_extracted": self.rows_extracted,
                "rows_validated": self.rows_validated,
                "rows_rejected": self.rows_rejected,
                "rows_transformed": self.rows_transformed,
                "rows_loaded": self.rows_loaded,
                "rejection_rate_percent": round(self.calculate_rejection_rate(), 2)
            },
            "data_quality": {
                "validation_errors": self.validation_errors,
                "total_errors": sum(self.validation_errors.values())
            },
            "files": {
                "input_file": self.input_file,
                "input_file_size_bytes": self.input_file_size_bytes,
                "rejected_file": self.rejected_file,
                "clean_file": self.clean_file
            }
        }

    def save_report(self, output_dir: str = "reports") -> str:
        """Save metrics report to JSON file."""
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        filename = f"{self.pipeline_name}_{self.execution_id}.json"
        filepath = Path(output_dir) / filename

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)

            logger.info(f"Metrics report saved to: {filepath}")
            return str(filepath)
        except Exception as e:
            logger.error(f"Failed to save metrics report: {e}")
            raise

    def print_summary(self):
        print("\n" + "=" * 60)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 60)
        print(f"Pipeline: {self.pipeline_name}")
        print(f"Execution ID: {self.execution_id}")
        print(f"Status: {self.status}")
        print(f"Duration: {self.duration_seconds:.2f} seconds")
        print(f"\nData Processed:")
        print(f"- Extracted: {self.rows_extracted:,} rows")
        print(f"- Validated: {self.rows_validated:,} rows")
        print(f"- Rejected: {self.rows_rejected:,} rows ({self.calculate_rejection_rate():.2f}%)")
        print(f"- Transformed: {self.rows_transformed:,} rows")
        print(f"- Loaded: {self.rows_loaded:,} rows")

        if self.validation_errors:
            print(f"\nValidation errors:")
            for error_type, count in sorted(self.validation_errors.items(), key=lambda x: x[1], reverse=True):
                print(f" - {error_type}: {count}")

        print("=" * 60 + "\n")


if __name__ == "__main__":
    # Test metrics
    logger.info("Testing PipelineMetrics...")

    metrics = PipelineMetrics("test_pipeline", "configs/test.yaml")

    # Simulate pipeline execution
    metrics.set_input_file("data/raw/customers.csv")
    metrics.set_extraction_metrics(100)
    metrics.set_validation_metrics(95, 5)
    metrics.set_transformation_metrics(95)
    metrics.set_load_metrics(95)
    metrics.set_output_files(
        rejected_file="data/processed/rejected_test.csv",
        clean_file="data/processed/clean_test.csv"
    )

    metrics.add_validation_error("null_values", 3)
    metrics.add_validation_error("invalid_integer", 2)

    metrics.complete(exit_code=0, status="SUCCESS")

    metrics.print_summary()

    # Save report
    report_path = metrics.save_report()
    print(f"\nReport saved to: {report_path}")

    # Load and display report
    with open(report_path, 'r') as f:
        report = json.load(f)

    print("\nReport contents:")
    print(json.dumps(report, indent=2))
