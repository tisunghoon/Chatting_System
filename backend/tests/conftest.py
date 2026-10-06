"""
tests/conftest.py

e2e 테스트 공용 픽스처
----------------------
- 세션 동안 실제 uvicorn 서버를 백그라운드 스레드로 하나 띄운다.
  starlette TestClient의 WebSocket receive에는 타임아웃이 없어서
  "메시지가 안 온다"를 검증하는 순간 테스트가 멈춘다. 실제 소켓을 쓰면
  websockets 클라이언트의 recv(timeout=...)로 기다릴 시간을 정할 수 있다.
- 서버는 세션 내내 재사용하고, 테스트마다 인메모리 싱글톤만 갈아끼운다.
"""

import socket
import threading
import time

import httpx
import pytest
import uvicorn
from websockets.sync.client import connect

import main
from api import api_server
from chat import chat_server
from presence import presence_server as presence_mod
from storage import store as store_mod


def _reset_singletons():
    """
    각 모듈이 `from x import store` 로 들고 있는 참조까지 전부 새 객체로 교체한다.
    한 곳이라도 빠지면 API가 쓰는 저장소와 채팅 서버가 읽는 저장소가 갈라진다.
    """
    store = store_mod.InMemoryStore()
    for mod in (store_mod, api_server, chat_server, presence_mod):
        mod.store = store

    presence = presence_mod.PresenceServer()
    for mod in (presence_mod, api_server, chat_server):
        mod.presence_server = presence

    chat_server.manager = chat_server.ConnectionManager()


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def server_addr():
    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(main.app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("uvicorn 서버가 10초 안에 뜨지 않음")
        time.sleep(0.05)

    yield f"127.0.0.1:{port}"

    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture(autouse=True)
def reset_state():
    _reset_singletons()
    yield
    _reset_singletons()


@pytest.fixture
def api(server_addr):
    with httpx.Client(base_url=f"http://{server_addr}", timeout=5) as client:
        yield client


@pytest.fixture
def ws_connect(server_addr):
    """`with ws_connect("alice") as ws:` 형태로 쓰는 WebSocket 연결 팩토리."""
    def _connect(user_id: str):
        return connect(f"ws://{server_addr}/ws/{user_id}", open_timeout=5)
    return _connect
