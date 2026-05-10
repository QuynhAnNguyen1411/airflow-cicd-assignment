"""Test DAG file imports correctly và có structure đúng."""
import sys
from pathlib import Path

DAGS_DIR = Path(__file__).parent.parent / "dags"
sys.path.insert(0, str(DAGS_DIR))


def test_dag_import_no_errors():
    """DAG phải import được, không có error."""
    from airflow.models import DagBag
    dag_bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert len(dag_bag.import_errors) == 0, \
        f"Import errors: {dag_bag.import_errors}"


def test_dag_exists():
    """DAG ID 'healthcare_etl_pipeline' phải tồn tại."""
    from airflow.models import DagBag
    dag_bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert "healthcare_etl_pipeline" in dag_bag.dags


def test_dag_structure():
    """DAG phải có ít nhất 5 tasks và có start/end markers."""
    from airflow.models import DagBag
    dag_bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    dag = dag_bag.dags["healthcare_etl_pipeline"]
    
    assert len(dag.tasks) >= 5, f"Expected ≥5 tasks, got {len(dag.tasks)}"
    
    task_ids = [t.task_id for t in dag.tasks]
    assert "start" in task_ids
    assert "end" in task_ids