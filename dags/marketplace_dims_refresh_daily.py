"""
### Refresh des dimensions

Pour chaque référentiel (vendeurs, produits, clients) :
`API` → **MinIO** `raw/<entité>/dt={{ ds }}/data.json` → `staging.<entité>` (remplacé)
→ upsert SCD1 dans `dwh.dim_<entité>`, le tout dans une transaction.

Les membres créés « à la volée » par le pipeline commandes (`is_inferred = TRUE`)
sont complétés ici.
"""
from __future__ import annotations

import json

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

from marketplace import sql
from marketplace.config import DATA_LAKE_BUCKET, DEFAULT_ARGS, MARKETPLACE_CONN_ID, S3_CONN_ID, raw_key
from marketplace.db import replace_rows
from marketplace.hooks import MarketplaceAPIHook

# entité -> colonnes de staging (dans l'ordre des champs JSON de l'API)
ENTITIES = {
    "sellers": ["seller_id", "name", "seller_type", "country", "city", "joined_date"],
    "products": ["product_id", "seller_id", "name", "category", "unit_price", "created_at"],
    "customers": ["customer_id", "country", "city", "segment", "signup_date"],
}


@dag(
    dag_id="marketplace_dims_refresh_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "ingestion", "dimensions"],
    doc_md=__doc__,
)
def marketplace_dims_refresh_daily():

    @task
    def extract_to_raw(entity: str, ds: str | None = None) -> str:
        hook = MarketplaceAPIHook(MARKETPLACE_CONN_ID)
        fetch = {"sellers": hook.get_sellers, "products": hook.get_products, "customers": hook.get_customers}
        rows = fetch[entity]()
        key = raw_key(entity, ds)
        S3Hook(aws_conn_id=S3_CONN_ID).load_string(
            json.dumps({"date": ds, "count": len(rows), entity: rows}, ensure_ascii=False),
            key=key,
            bucket_name=DATA_LAKE_BUCKET,
            replace=True,
        )
        print(f"{len(rows)} {entity} archivés dans s3://{DATA_LAKE_BUCKET}/{key}")
        return key

    @task
    def load_dimension(entity: str, key: str) -> int:
        body = S3Hook(aws_conn_id=S3_CONN_ID).read_key(key=key, bucket_name=DATA_LAKE_BUCKET)
        columns = ENTITIES[entity]
        rows = [tuple(item.get(c) for c in columns) for item in json.loads(body)[entity]]
        return replace_rows(
            delete_sql=f"DELETE FROM staging.{entity};",
            insert_sql=f"INSERT INTO staging.{entity} ({', '.join(columns)}) VALUES %s",
            rows=rows,
            then=[sql.DIM_UPSERT[entity]],
        )

    for entity in ENTITIES:
        key = extract_to_raw.override(task_id=f"extract_{entity}_to_raw")(entity)
        load_dimension.override(task_id=f"load_dim_{entity}")(entity, key)


marketplace_dims_refresh_daily()
