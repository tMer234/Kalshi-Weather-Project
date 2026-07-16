"""NWSClient transport tests: retry/backoff, conditional GET (304), and problem+json
error parsing — the mechanics every collector relies on, isolated from any real
collector logic. All HTTP mocked via `responses`; no live calls.
"""

import pytest
import responses

from kalshi_weather.nws_client import NWSClient, NWSError

BASE = "https://api.weather.gov"


@pytest.fixture
def client():
    # retry_initial_seconds=0 so retry tests don't actually sleep
    return NWSClient(user_agent="kalshi-weather-test (test@example.com)", retry_initial_seconds=0)


@responses.activate
def test_user_agent_header_sent(client):
    responses.get(f"{BASE}/points/40.7789,-73.9692", json={"properties": {}})
    client.get_points(40.7789, -73.9692)
    assert (
        responses.calls[0].request.headers["User-Agent"]
        == "kalshi-weather-test (test@example.com)"
    )


@responses.activate
def test_coords_trimmed_to_4_decimals_without_trailing_zeros(client):
    responses.get(f"{BASE}/points/41.79,-87.75", json={"properties": {}})
    client.get_points(41.7900, -87.7500)
    assert str(responses.calls[0].request.url).endswith("/points/41.79,-87.75")


@responses.activate
def test_retries_on_429_then_succeeds(client):
    responses.get(f"{BASE}/points/40,-73", status=429, json={"detail": "slow down"})
    responses.get(f"{BASE}/points/40,-73", json={"properties": {"cwa": "OKX"}})
    payload = client.get_points(40.0, -73.0)
    assert payload["properties"]["cwa"] == "OKX"
    assert len(responses.calls) == 2


@responses.activate
def test_retry_exhaustion_raises_nws_error_with_problem_detail(client):
    for _ in range(4):
        responses.get(
            f"{BASE}/points/40,-73",
            status=503,
            json={"title": "Service Unavailable", "detail": "upstream down"},
            content_type="application/problem+json",
        )
    with pytest.raises(NWSError) as exc:
        client.get_points(40.0, -73.0)
    assert "upstream down" in str(exc.value)
    assert exc.value.status == 503
    assert len(responses.calls) == 4


@responses.activate
def test_non_retryable_4xx_fails_immediately(client):
    responses.get(
        f"{BASE}/points/40,-73",
        status=404,
        json={"detail": "no data"},
        content_type="application/problem+json",
    )
    with pytest.raises(NWSError) as exc:
        client.get_points(40.0, -73.0)
    assert exc.value.status == 404
    assert len(responses.calls) == 1  # no retries on 404


@responses.activate
def test_conditional_get_sends_header_and_handles_304(client):
    url = f"{BASE}/gridpoints/OKX/33,37"
    responses.get(url, status=304)
    resp = client.get_grid_data(url, if_modified_since="Wed, 08 Jul 2026 12:00:00 GMT")
    assert resp.not_modified
    assert resp.payload is None
    assert (
        responses.calls[0].request.headers["If-Modified-Since"]
        == "Wed, 08 Jul 2026 12:00:00 GMT"
    )


@responses.activate
def test_last_modified_captured(client):
    url = f"{BASE}/gridpoints/OKX/33,37"
    responses.get(
        url,
        json={"properties": {}},
        headers={"Last-Modified": "Wed, 08 Jul 2026 12:00:00 GMT"},
    )
    resp = client.get_grid_data(url)
    assert resp.last_modified == "Wed, 08 Jul 2026 12:00:00 GMT"


@responses.activate
def test_cli_products_listing_unwraps_graph_and_limits_client_side(client):
    responses.get(
        f"{BASE}/products/types/CLI/locations/OKX",
        json={"@graph": [{"id": "abc"}, {"id": "def"}, {"id": "ghi"}]},
    )
    products = client.get_cli_products("OKX", limit=2)
    assert products == [{"id": "abc"}, {"id": "def"}]
    # this endpoint 400s on query params — make sure none are sent
    assert "?" not in str(responses.calls[0].request.url)
