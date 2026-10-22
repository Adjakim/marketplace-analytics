"""Opérateur de contrôle qualité configurable sur une table PostgreSQL."""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any

try:  # Airflow 3.x
    from airflow.sdk import BaseOperator
except ImportError:  # pragma: no cover - compatibilité Airflow 2
    from airflow.models import BaseOperator

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):
    """Vérifie 3 règles configurables sur une partition d'une table :

    1. ``check_not_empty``     : la partition contient au moins ``min_rows`` lignes ;
    2. ``not_null_columns``    : aucune valeur NULL dans ces colonnes ;
    3. ``no_future_date_column`` : aucune date/horodatage dans le futur.

    Retourne (XCom) ``{"passed": bool, "row_count": int, "failures": [...]}``.
    Par défaut l'opérateur NE lève PAS d'erreur quand une règle échoue : c'est la
    tâche de branchement en aval qui décide (load DWH ou alerte). Passer
    ``fail_on_error=True`` pour faire échouer la tâche directement.

    :param table: table qualifiée, ex. ``staging.orders``
    :param partition_filter: clause WHERE (templatée), ex. ``dt = '{{ ds }}'``
    :param results_table: si renseignée, chaque résultat de règle y est historisé
    """

    template_fields: Sequence[str] = ("table", "partition_filter")
    ui_color = "#fde68a"

    def __init__(
        self,
        *,
        table: str,
        partition_filter: str = "TRUE",
        postgres_conn_id: str = "postgres_dwh",
        check_not_empty: bool = True,
        min_rows: int = 1,
        not_null_columns: Sequence[str] = (),
        no_future_date_column: str | None = None,
        fail_on_error: bool = False,
        results_table: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.table = table
        self.partition_filter = partition_filter
        self.postgres_conn_id = postgres_conn_id
        self.check_not_empty = check_not_empty
        self.min_rows = min_rows
        self.not_null_columns = list(not_null_columns)
        self.no_future_date_column = no_future_date_column
        self.fail_on_error = fail_on_error
        self.results_table = results_table

    def _count(self, hook: PostgresHook, extra_condition: str | None = None) -> int:
        where = f"({self.partition_filter})"
        if extra_condition:
            where += f" AND ({extra_condition})"
        return int(hook.get_first(f"SELECT COUNT(*) FROM {self.table} WHERE {where}")[0])

    def execute(self, context: Any) -> dict[str, Any]:
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        results: list[tuple[str, bool, str]] = []

        row_count = self._count(hook)
        if self.check_not_empty:
            ok = row_count >= self.min_rows
            results.append(("not_empty", ok, f"{row_count} ligne(s), minimum attendu {self.min_rows}"))

        for column in self.not_null_columns:
            nulls = self._count(hook, f"{column} IS NULL")
            results.append((f"not_null:{column}", nulls == 0, f"{nulls} valeur(s) NULL"))

        if self.no_future_date_column:
            col = self.no_future_date_column
            future = self._count(hook, f"{col} > now()")
            results.append((f"no_future_date:{col}", future == 0, f"{future} ligne(s) dans le futur"))

        for name, ok, details in results:
            (self.log.info if ok else self.log.error)("[DQ] %s %s : %s", "OK" if ok else "KO", name, details)

        if self.results_table:
            self._store_results(hook, context, results)

        failures = [f"{name} ({details})" for name, ok, details in results if not ok]
        if failures and self.fail_on_error:
            raise AirflowException(f"Contrôles qualité en échec sur {self.table} : {failures}")
        return {"passed": not failures, "row_count": row_count, "failures": failures}

    def _store_results(self, hook: PostgresHook, context: Any, results: list[tuple[str, bool, str]]) -> None:
        dag_run = context.get("dag_run") if hasattr(context, "get") else None
        dag_id = getattr(dag_run, "dag_id", None)
        run_id = getattr(dag_run, "run_id", None)
        hook.insert_rows(
            table=self.results_table,
            rows=[(dag_id, run_id, self.table, self.partition_filter, name, ok, details)
                  for name, ok, details in results],
            target_fields=["dag_id", "run_id", "table_name", "partition", "check_name", "passed", "details"],
        )
