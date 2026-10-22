"""Constantes partagées par les DAGs."""
from datetime import timedelta

from airflow.sdk import Asset

MARKETPLACE_CONN_ID = "marketplace_api"
DWH_CONN_ID = "postgres_dwh"
S3_CONN_ID = "minio_s3"
DATA_LAKE_BUCKET = "data-lake"

COMMISSION_RATE = 0.12          # commission Maelys sur chaque vente
STAGING_RETENTION_DAYS = 30     # au-delà, staging.orders est purgé (le brut reste dans MinIO)
REVENUE_DROP_THRESHOLD = 0.70   # US-05 : alerte si CA du jour < 70% de la moyenne 7j

# Assets (data-aware scheduling) : l'ingestion publie FACT_ORDERS, ce qui déclenche
# le DAG analytics, qui publie DAILY_SUMMARY, ce qui déclenche la détection d'anomalies.
FACT_ORDERS_ASSET = Asset(name="dwh_fact_orders", uri="postgres://postgres-dwh:5432/dwh/dwh/fact_orders")
DAILY_SUMMARY_ASSET = Asset(name="analytics_daily_summary",
                            uri="postgres://postgres-dwh:5432/dwh/analytics/daily_summary")

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def raw_key(entity: str, ds: str) -> str:
    """Chemin du JSON brut dans le data lake : raw/<entity>/dt=YYYY-MM-DD/data.json"""
    return f"raw/{entity}/dt={ds}/data.json"
