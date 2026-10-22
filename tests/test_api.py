"""Tests de l'API simulée : authentification et déterminisme (même date = même réponse)."""
import importlib.util

import pytest

from conftest import ROOT

pytest.importorskip("flask")
APP_PATH = ROOT / "api" / "app.py"
if not APP_PATH.exists():
    pytest.skip("api/app.py introuvable", allow_module_level=True)

AUTH = {"Authorization": "Bearer formation-token-2026"}


@pytest.fixture(scope="module")
def client():
    spec = importlib.util.spec_from_file_location("marketplace_api_app", APP_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.app.test_client()


def test_health_without_auth(client):
    assert client.get("/health").get_json()["status"] == "ok"


def test_orders_require_bearer_token(client):
    assert client.get("/orders?date=2026-04-07").status_code == 401


def test_orders_are_deterministic(client):
    first = client.get("/orders?date=2026-04-07", headers=AUTH).get_json()
    second = client.get("/orders?date=2026-04-07", headers=AUTH).get_json()
    assert first["count"] > 0
    assert first == second


def test_different_dates_give_different_orders(client):
    a = client.get("/orders?date=2026-04-07", headers=AUTH).get_json()
    b = client.get("/orders?date=2026-04-08", headers=AUTH).get_json()
    assert a["orders"][0]["order_id"] != b["orders"][0]["order_id"]


def test_invalid_date_is_rejected(client):
    assert client.get("/orders?date=07-04-2026", headers=AUTH).status_code == 400
