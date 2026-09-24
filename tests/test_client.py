import json
from pathlib import Path
from unittest.mock import MagicMock

from pipeline.client import USER_AGENT, build_session, fetch_all

FIXTURES = Path(__file__).parent / "fixtures"


def _response(body):
    response = MagicMock()
    response.json.return_value = body
    response.raise_for_status.return_value = None
    return response


def _paged_session(resource, items, total=None):
    """Sesión cuyo GET sirve `items` respetando limit/skip, como DummyJSON."""
    total = len(items) if total is None else total
    session = MagicMock()

    def get(url, params, timeout):
        start, limit = params["skip"], params["limit"]
        return _response({resource: items[start : start + limit], "total": total})

    session.get.side_effect = get
    return session


def test_fetch_all_walks_every_page():
    carts = json.loads((FIXTURES / "carts.json").read_text())
    items = [dict(carts[i % len(carts)], id=i) for i in range(1, 251)]
    session = _paged_session("carts", items)

    result, api_total = fetch_all(session, "carts", page_size=100)

    assert [c["id"] for c in result] == list(range(1, 251))
    assert api_total == 250
    skips = [call.kwargs["params"]["skip"] for call in session.get.call_args_list]
    assert skips == [0, 100, 200]


def test_session_sends_user_agent_and_retries_transient_errors():
    session = build_session()

    assert session.headers["User-Agent"] == USER_AGENT
    retry = session.get_adapter("https://dummyjson.com").max_retries
    assert retry.total == 5
    assert {429, 500, 502, 503, 504} <= set(retry.status_forcelist)
