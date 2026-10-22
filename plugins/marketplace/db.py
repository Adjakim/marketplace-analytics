"""Accès au Data Warehouse : exécution transactionnelle des transformations."""
from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook

from marketplace.config import DWH_CONN_ID

log = logging.getLogger(__name__)


def run_in_transaction(
    statements: Sequence[str],
    parameters: dict[str, Any] | None = None,
    conn_id: str = DWH_CONN_ID,
) -> list[int]:
    """Exécute toutes les requêtes dans UNE transaction (BEGIN ... COMMIT).

    Si une requête échoue, ROLLBACK : la table n'est jamais laissée à moitié
    écrite (ex. partition supprimée mais pas réinsérée). Retourne le rowcount
    de chaque requête.
    """
    conn = PostgresHook(postgres_conn_id=conn_id).get_conn()
    rowcounts: list[int] = []
    try:
        with conn.cursor() as cur:
            for sql in statements:
                cur.execute(sql, parameters)
                rowcounts.append(cur.rowcount)
                log.info("%s ligne(s) <- %s", cur.rowcount, " ".join(sql.split())[:90])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return rowcounts


def replace_rows(
    delete_sql: str,
    insert_sql: str,
    rows: Iterable[tuple],
    parameters: dict[str, Any] | None = None,
    conn_id: str = DWH_CONN_ID,
    then: Sequence[str] = (),
) -> int:
    """DELETE puis INSERT en masse (execute_values), puis les requêtes ``then``,
    le tout dans une même transaction."""
    from psycopg2.extras import execute_values

    rows = list(rows)
    conn = PostgresHook(postgres_conn_id=conn_id).get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(delete_sql, parameters)
            log.info("%s ligne(s) supprimée(s)", cur.rowcount)
            if rows:
                execute_values(cur, insert_sql, rows, page_size=1000)
            for sql in then:
                cur.execute(sql, parameters)
                log.info("%s ligne(s) <- %s", cur.rowcount, " ".join(sql.split())[:90])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    log.info("%s ligne(s) insérée(s)", len(rows))
    return len(rows)
