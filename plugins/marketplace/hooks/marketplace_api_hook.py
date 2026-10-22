"""Hook Airflow pour l'API REST du backoffice Maelys Marketplace."""
from __future__ import annotations

import time
from datetime import date, datetime
from typing import Any

import requests

try:  # Airflow 3.x
    from airflow.sdk import BaseHook
except ImportError:  # pragma: no cover - compatibilité Airflow 2
    from airflow.hooks.base import BaseHook

from airflow.exceptions import AirflowException

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class MarketplaceAPIHook(BaseHook):
    """Accès authentifié (Bearer) à l'API Marketplace.

    Lit la Connection Airflow ``marketplace_api`` :
      - ``host``     : URL de base, ex. ``http://api-marketplace:5000``
                       (ou juste le nom d'hôte + ``port`` / ``schema`` = http|https)
      - ``password`` : token Bearer

    Les erreurs transitoires (timeout, erreur réseau, HTTP 429/5xx) sont
    rejouées avec un backoff exponentiel ; les erreurs client (400, 401, 404...)
    échouent immédiatement car un nouvel essai ne changerait rien.
    """

    conn_name_attr = "marketplace_conn_id"
    default_conn_name = "marketplace_api"
    conn_type = "http"
    hook_name = "Marketplace API"

    def __init__(
        self,
        marketplace_conn_id: str = default_conn_name,
        timeout: float = 30.0,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ) -> None:
        super().__init__()
        self.marketplace_conn_id = marketplace_conn_id
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self._session: requests.Session | None = None
        self.base_url: str | None = None

    # ------------------------------------------------------------------ connexion
    def get_conn(self) -> requests.Session:
        """Session HTTP configurée avec le token de la Connection (créée une seule fois)."""
        if self._session is not None:
            return self._session

        conn = self.get_connection(self.marketplace_conn_id)
        if not conn.host:
            raise AirflowException(f"Connection '{self.marketplace_conn_id}' : champ host vide.")
        if not conn.password:
            raise AirflowException(f"Connection '{self.marketplace_conn_id}' : token (password) manquant.")

        host = conn.host.rstrip("/")
        if not host.startswith(("http://", "https://")):
            scheme = conn.schema or "http"
            host = f"{scheme}://{host}"
            if conn.port:
                host = f"{host}:{conn.port}"
        self.base_url = host

        session = requests.Session()
        session.headers.update({
            "Authorization": f"Bearer {conn.password}",
            "Accept": "application/json",
        })
        self._session = session
        return session

    def _request(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        session = self.get_conn()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        attempts = self.max_retries + 1

        for attempt in range(1, attempts + 1):
            try:
                response = session.get(url, params=params, timeout=self.timeout)
            except (requests.Timeout, requests.ConnectionError) as exc:
                error = f"{type(exc).__name__}: {exc}"
            else:
                if response.status_code < 400:
                    return response.json()
                if response.status_code not in RETRYABLE_STATUS:
                    raise AirflowException(
                        f"GET {url} -> HTTP {response.status_code} (non rejouable) : {response.text[:200]}"
                    )
                error = f"HTTP {response.status_code}"

            if attempt == attempts:
                raise AirflowException(f"GET {url} en échec après {attempts} tentatives ({error})")
            wait = self.backoff_factor ** (attempt - 1)
            self.log.warning("GET %s : %s - tentative %s/%s, nouvel essai dans %.1fs",
                             url, error, attempt, attempts, wait)
            time.sleep(wait)

        raise AssertionError("unreachable")  # pragma: no cover

    # ------------------------------------------------------------------ endpoints
    def health(self) -> dict[str, Any]:
        return self._request("/health")

    def get_orders(self, day: str | date | datetime) -> list[dict[str, Any]]:
        """Commandes d'une journée (``day`` au format YYYY-MM-DD ou objet date)."""
        if isinstance(day, datetime):
            day = day.date()
        if isinstance(day, date):
            day = day.isoformat()
        try:
            date.fromisoformat(day)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Date invalide '{day}', format attendu YYYY-MM-DD") from exc

        payload = self._request("/orders", params={"date": day})
        orders = payload["orders"]
        self.log.info("API /orders?date=%s : %s commandes", day, len(orders))
        return orders

    def get_sellers(self, limit: int | None = None) -> list[dict[str, Any]]:
        params = {"limit": limit} if limit else None
        return self._request("/sellers", params=params)["sellers"]

    def get_products(self) -> list[dict[str, Any]]:
        return self._request("/products")["products"]

    def get_customers(self, limit: int | None = None) -> list[dict[str, Any]]:
        params = {"limit": limit} if limit else None
        return self._request("/customers", params=params)["customers"]

    def test_connection(self) -> tuple[bool, str]:
        """Utilisé par le bouton 'Test' de l'UI Connections."""
        try:
            self._request("/sellers", params={"limit": 1})
            return True, "Connexion à l'API Marketplace OK"
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)
