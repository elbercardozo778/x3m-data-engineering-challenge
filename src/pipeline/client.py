from __future__ import annotations

from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline.observability import log_event

BASE_URL = "https://dummyjson.com"
USER_AGENT = "x3m-dummyjson-pipeline/1.0"


def build_session(retries: int = 5, backoff_factor: float = 1.0) -> requests.Session:
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_all(
    session: requests.Session,
    resource: str,
    page_size: int = 100,
    base_url: str = BASE_URL,
    timeout: float = 30.0,
) -> tuple[list[dict[str, Any]], int]:
    """Trae todos los registros de una colección paginada de DummyJSON y el `total` que informa la API."""
    items: list[dict[str, Any]] = []
    skip = 0
    total = 0
    while True:
        response = session.get(
            f"{base_url}/{resource}",
            params={"limit": page_size, "skip": skip},
            timeout=timeout,
        )
        response.raise_for_status()
        body = response.json()
        page = body[resource]
        items.extend(page)
        total = body["total"]
        log_event(
            "api_page_fetched",
            f"Página de {resource} recibida",
            skip=skip,
            limit=page_size,
            records=len(page),
            accumulated=f"{len(items)}/{total}",
        )
        skip += len(page)
        if not page or skip >= total:
            break
    return items, total
