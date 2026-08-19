from __future__ import annotations

from collections.abc import Iterable

import httpx

SEC_COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
BLS_TIMESERIES_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"


def fetch_sec_company_facts(
    cik: str | int,
    user_agent: str,
    client: httpx.Client | None = None,
) -> dict[str, object]:
    """Fetch a company's standardized XBRL facts from SEC EDGAR.

    SEC asks automated clients to identify themselves, so a descriptive user agent
    containing an email address is required instead of silently using a generic one.
    """
    if "@" not in user_agent:
        raise ValueError("SEC user_agent must identify the caller and include an email address")
    padded_cik = str(cik).removeprefix("CIK").zfill(10)
    url = SEC_COMPANY_FACTS_URL.format(cik=padded_cik)
    owns_client = client is None
    resolved_client = client or httpx.Client(timeout=30)
    try:
        response = resolved_client.get(
            url,
            headers={"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"},
        )
        response.raise_for_status()
        return response.json()
    finally:
        if owns_client:
            resolved_client.close()


def fetch_bls_series(
    series_ids: Iterable[str],
    start_year: int,
    end_year: int,
    registration_key: str | None = None,
    client: httpx.Client | None = None,
) -> dict[str, object]:
    """Fetch one or more Bureau of Labor Statistics time series."""
    series = list(dict.fromkeys(series_ids))
    if not series:
        raise ValueError("At least one BLS series ID is required")
    if end_year < start_year:
        raise ValueError("end_year must be greater than or equal to start_year")

    payload: dict[str, object] = {
        "seriesid": series,
        "startyear": str(start_year),
        "endyear": str(end_year),
    }
    if registration_key:
        payload["registrationkey"] = registration_key

    owns_client = client is None
    resolved_client = client or httpx.Client(timeout=30)
    try:
        response = resolved_client.post(BLS_TIMESERIES_URL, json=payload)
        response.raise_for_status()
        body = response.json()
        if body.get("status") != "REQUEST_SUCCEEDED":
            message = "; ".join(body.get("message", [])) or "BLS request failed"
            raise RuntimeError(message)
        return body
    finally:
        if owns_client:
            resolved_client.close()
