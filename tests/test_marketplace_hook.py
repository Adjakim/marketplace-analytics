"""Tests unitaires du MarketplaceAPIHook : requests et la Connection sont mockés."""
from unittest import mock

import pytest
import requests
from airflow.exceptions import AirflowException

from marketplace.hooks.marketplace_api_hook import MarketplaceAPIHook

MODULE = "marketplace.hooks.marketplace_api_hook"


def response(status, payload=None):
    resp = mock.Mock(spec=requests.Response)
    resp.status_code = status
    resp.json.return_value = payload
    resp.text = str(payload)
    return resp


@pytest.fixture
def hook():
    conn = mock.Mock(host="http://api-marketplace:5000", password="secret-token", schema=None, port=None)
    with mock.patch.object(MarketplaceAPIHook, "get_connection", return_value=conn), \
            mock.patch(f"{MODULE}.time.sleep"):
        yield MarketplaceAPIHook(max_retries=2)


def test_bearer_token_read_from_connection(hook):
    session = hook.get_conn()
    assert session.headers["Authorization"] == "Bearer secret-token"
    assert hook.base_url == "http://api-marketplace:5000"


def test_host_without_scheme_is_completed():
    conn = mock.Mock(host="api-marketplace", password="t", schema="http", port=5000)
    with mock.patch.object(MarketplaceAPIHook, "get_connection", return_value=conn):
        hook = MarketplaceAPIHook()
        hook.get_conn()
    assert hook.base_url == "http://api-marketplace:5000"


def test_missing_token_fails():
    conn = mock.Mock(host="http://api", password=None)
    with mock.patch.object(MarketplaceAPIHook, "get_connection", return_value=conn):
        with pytest.raises(AirflowException, match="token"):
            MarketplaceAPIHook().get_conn()


def test_get_orders_calls_endpoint_with_date(hook):
    orders = [{"order_id": "ORD-1"}]
    with mock.patch.object(requests.Session, "get", return_value=response(200, {"orders": orders})) as get:
        assert hook.get_orders("2026-04-07") == orders
    get.assert_called_once_with("http://api-marketplace:5000/orders",
                                params={"date": "2026-04-07"}, timeout=hook.timeout)


def test_get_orders_rejects_bad_date(hook):
    with pytest.raises(ValueError):
        hook.get_orders("07/04/2026")


def test_retries_on_5xx_then_succeeds(hook):
    side_effect = [response(503), response(200, {"sellers": [{"seller_id": "S1"}]})]
    with mock.patch.object(requests.Session, "get", side_effect=side_effect) as get:
        assert hook.get_sellers() == [{"seller_id": "S1"}]
    assert get.call_count == 2


def test_retries_on_timeout(hook):
    side_effect = [requests.Timeout("lent"), response(200, {"products": []})]
    with mock.patch.object(requests.Session, "get", side_effect=side_effect) as get:
        assert hook.get_products() == []
    assert get.call_count == 2


def test_gives_up_after_max_retries(hook):
    with mock.patch.object(requests.Session, "get", return_value=response(500)) as get:
        with pytest.raises(AirflowException, match="3 tentatives"):
            hook.get_orders("2026-04-07")
    assert get.call_count == 3  # 1 essai + 2 retries


def test_no_retry_on_401(hook):
    with mock.patch.object(requests.Session, "get", return_value=response(401, {"error": "Unauthorized"})) as get:
        with pytest.raises(AirflowException, match="401"):
            hook.get_orders("2026-04-07")
    assert get.call_count == 1
