"""Fixtures communes. Lancement : docker compose exec airflow-worker pytest tests/ -v"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = ROOT / "dags"
for path in (ROOT / "plugins", DAGS_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: nécessite le conteneur postgres-dwh")


@pytest.fixture(scope="session")
def dagbag():
    try:  # Airflow >= 3.1
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:
        from airflow.models import DagBag
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
