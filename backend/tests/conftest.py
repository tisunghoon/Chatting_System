"""
tests/conftest.py

공유 픽스처
-----------
- `app_state_reset`: 각 테스트 전에 싱글톤 상태(store, manager, presence_server)를 초기화한다.
  인메모리 구현이기 때문에 테스트 간 데이터가 오염되지 않도록 반드시 필요하다.
- `test_app`: 상태가 초기화된 FastAPI 앱 인스턴스를 반환한다.
- `async_client`: REST API 호출용 httpx.AsyncClient.
- `registered_users`: alice/bob/carol을 미리 등록하고 토큰을 반환한다.
"""

import asyncio
import importlib
import sys
import pytest
import pytest_asyncio
import httpx
from fastapi.testclient import TestClient


def _reset_singletons():
    """
    모듈 레벨 싱글톤을 테스트 격리를 위해 새 인스턴스로 교체한다.
    Python 모듈 캐시를 활용: 이미 import된 모듈 객체의 싱글톤을 직접 교체한다.
    """
    # 1. storage.store 초기화
    from storage import store as store_mod
    from storage.store import InMemoryStore
    store_mod.store = InMemoryStore()

    # 2. chat.chat_server.manager 초기화 (WebSocket 연결 맵)
    from chat import chat_server as chat_mod
    from chat.chat_server import ConnectionManager
    chat_mod.manager = ConnectionManager()

    # 3. presence_server 초기화
    from presence import presence_server as pres_mod
    from presence.presence_server import PresenceServer
    pres_mod.presence_server = PresenceServer()

    # 4. api_server가 presence_server를 직접 import해서 쓰므로 동기화
    from api import api_server as api_mod
    api_mod.presence_server = pres_mod.presence_server

    # 5. chat_server가 참조하는 store / presence_server도 동기화
    chat_mod.store = store_mod.store
    chat_mod.presence_server = pres_mod.presence_server


@pytest.fixture(autouse=True)
def app_state_reset():
    """모든 테스트 전에 싱글톤 상태를 초기화한다."""
    _reset_singletons()
    yield
    # teardown: 다시 초기화 (혹시 남은 상태 제거)
    _reset_singletons()


@pytest.fixture
def test_app(app_state_reset):
    """상태가 초기화된 FastAPI app."""
    # main을 재 import하면 lifespan이 다시 붙어 문제가 생기므로
    # 이미 import된 app 객체를 그대로 사용한다.
    import main
    return main.app


@pytest_asyncio.fixture
async def async_client(test_app):
    """REST API 테스트용 httpx.AsyncClient (ASGI transport)."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=test_app),
        base_url="http://test",
    ) as client:
        yield client


@pytest_asyncio.fixture
async def registered_users(async_client):
    """
    alice / bob / carol을 미리 등록하고,
    {user_id: token} 딕셔너리를 반환한다.
    """
    users = [
        ("alice", "Alice", "pw_alice"),
        ("bob", "Bob", "pw_bob"),
        ("carol", "Carol", "pw_carol"),
    ]
    tokens = {}
    for uid, name, pw in users:
        await async_client.post("/api/register", json={
            "user_id": uid, "username": name, "password": pw,
        })
        r = await async_client.post("/api/login", json={"user_id": uid, "password": pw})
        tokens[uid] = r.json()["token"]
    return tokens
