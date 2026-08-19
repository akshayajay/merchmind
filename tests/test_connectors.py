from __future__ import annotations

import httpx
import pytest

from merchmind.connectors import fetch_bls_series, fetch_sec_company_facts


def test_sec_connector_pads_cik_and_identifies_caller() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/CIK0000320193.json")
        assert request.headers["user-agent"] == "MERCHMIND contact@example.com"
        return httpx.Response(200, json={"cik": 320193, "entityName": "Example"})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = fetch_sec_company_facts(
            320193,
            "MERCHMIND contact@example.com",
            client=client,
        )
    assert result["cik"] == 320193


def test_sec_connector_rejects_anonymous_user_agent() -> None:
    with pytest.raises(ValueError, match="email"):
        fetch_sec_company_facts(320193, "anonymous")


def test_bls_connector_validates_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        return httpx.Response(
            200,
            json={"status": "REQUEST_SUCCEEDED", "Results": {"series": []}},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = fetch_bls_series(["CUUR0000SAA1"], 2024, 2025, client=client)
    assert result["status"] == "REQUEST_SUCCEEDED"
