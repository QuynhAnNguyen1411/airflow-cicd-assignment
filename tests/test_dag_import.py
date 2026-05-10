"""Test DAG file - lightweight version (no full Airflow install needed)."""
import ast
import sys
from pathlib import Path

DAG_FILE = Path(__file__).parent.parent / "dags" / "example_etl_dag.py"


def test_dag_file_exists():
    """DAG file phải tồn tại."""
    assert DAG_FILE.exists(), f"DAG file not found: {DAG_FILE}"


def test_dag_syntax_valid():
    """DAG file phải có syntax Python hợp lệ."""
    source = DAG_FILE.read_text()
    tree = ast.parse(source)
    assert tree is not None


def test_dag_has_required_components():
    """DAG phải có các components cần thiết."""
    source = DAG_FILE.read_text()
    assert "healthcare_etl_pipeline" in source, "Missing DAG ID"
    assert "BigQueryInsertJobOperator" in source, "Missing BQ operator"
    assert "EmptyOperator" in source, "Missing start/end markers"
    assert "default_args" in source, "Missing default_args"


def test_dag_no_obvious_errors():
    """Không có common mistakes trong DAG."""
    source = DAG_FILE.read_text()
    assert "import" in source, "No imports found"
    assert "with DAG(" in source, "No DAG context manager found"
