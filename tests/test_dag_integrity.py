import os
import pytest
from airflow.models import DagBag

def test_no_import_errors():
    """
    Test that the Airflow DAG parses without any import errors.
    This ensures that the DAG structure is valid before deployment.
    """
    # Assuming dags are located in the dags/ directory relative to project root
    dags_dir = os.path.join(os.path.dirname(__file__), '../dags')
    dag_bag = DagBag(dag_folder=dags_dir, include_examples=False)
    
    assert len(dag_bag.import_errors) == 0, \
        f"DAG import errors found: {dag_bag.import_errors}"
