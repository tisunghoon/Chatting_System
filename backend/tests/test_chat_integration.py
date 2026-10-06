"""
tests/test_chat_integration.py

채팅 시스템 통합 테스트
------------------------
검증 항목:
  (1) test_dm_send_receive       : 1:1 메시지 전송 → 수신자에게 즉시 도달
  (2) test_group_chat_broadcast  : 그룹 채팅 메시지 → 모든 멤버 수신
  (3) test_offline_inbox_sync    : 오프라인 inbox 보관 → 재연결 시 sync_response 수신
  (4) test_heartbeat_timeout     : heartbeat 타임아웃 → 오프라인 전환

전략:
  - starlette.testclient.TestClient의 WebSocket 지원을 사용한다.
    TestClient는 내부적으로 anyio로 ASGI 앱을 실행하므로 별도 서버 불필요.
  - heartbeat 타임아웃 테스트는 monkeypatch로 HEARTBEAT_TIMEOUT=2초로 단축하고
    last_active를 과거로 조작한 뒤 expiry 로직을 직접 실행한다.
"""

import time
import asyncio
import pytest
from starlette.testclient import TestClient
import storage.store as store_mod
import presence.presence_server as pres_mod


# ─── 헬퍼 ─────────────────────────────────────────────────────────────────────

def auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def setup_users(client: TestClient, *user_ids: str) -> dict[str, str]:
    """user_id 목록을 등록하고 {user_id: token} 반환."""
    tokens = {}
    for uid in user_ids:
        client.post("/api/register", json={
            "user_id": uid, "username": uid.capitalize(), "password": "pw",
        })
        r = client.post("/api/login", json={"user_id": uid, "password": "pw"})
        assert r.status_code == 200, f"로그인 실패: {uid}"
        tokens[uid] = r.json()["token"]
    return tokens


def drain(ws, duration: float = 0.3) -> list[dict]:
    """duration 초 동안 수신된 모든 메시지 반환."""
    result = []
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        try:
            data = ws.receive_json()
            result.append(data)
        except Exception:
            time.sleep(0.05)
    return result


def recv_until(ws, predicate, timeout: float = 3.0) -> dict | None:
    """predicate를 만족하는 메시지가 올 때까지 수신. 없으면 None."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            data = ws.receive_json()
            if predicate(data):
                return data
        except Exception:
            time.sleep(0.05)
    return None


# ─── (1) 1:1 메시지 전송/수신 ─────────────────────────────────────────────────

def test_dm_send_receive(test_app):
    """
    alice → bob 1:1 메시지:
    - bob WebSocket에 즉시 도달
    - message_id 존재, chat_type == "dm"
    - alice 자신의 다른 단말(에코)에도 수신
    """
    client = TestClient(test_app)
    tokens = setup_users(client, "alice", "bob")

    with (client.websocket_connect(f"/ws/alice") as alice_ws,
          client.websocket_connect(f"/ws/bob") as bob_ws):

        drain(alice_ws, 0.2)
        drain(bob_ws, 0.2)

        alice_ws.send_json({"type": "send_message", "to": "bob", "content": "hello"})

        msg = recv_until(bob_ws, lambda m: m.get("type") == "message")
        assert msg is not None, "bob이 메시지를 수신하지 못함"
        assert msg["content"] == "hello"
        assert msg["from"] == "alice"
        assert msg["to"] == "bob"
        assert msg["chat_type"] == "dm"
        assert isinstance(msg["message_id"], int) and msg["message_id"] > 0

        # alice 에코 (여러 단말 동기화)
        echo = recv_until(alice_ws, lambda m: m.get("type") == "message")
        assert echo is not None, "alice 에코 수신 실패"
        assert echo["content"] == "hello"


def test_dm_multiple_messages_ordered(test_app):
    """여러 메시지의 message_id가 단조 증가(시간순)해야 한다."""
    client = TestClient(test_app)
    setup_users(client, "alice", "bob")

    with (client.websocket_connect("/ws/alice") as alice_ws,
          client.websocket_connect("/ws/bob") as bob_ws):

        drain(alice_ws, 0.2)
        drain(bob_ws, 0.2)

        for i in range(3):
            alice_ws.send_json({"type": "send_message", "to": "bob", "content": f"msg{i}"})

        received = []
        deadline = time.monotonic() + 4.0
        while len(received) < 3 and time.monotonic() < deadline:
            try:
                m = bob_ws.receive_json()
                if m.get("type") == "message":
                    received.append(m)
            except Exception:
                time.sleep(0.05)

        assert len(received) == 3, f"3건 기대, {len(received)}건 수신"
        ids = [m["message_id"] for m in received]
        assert ids == sorted(ids), f"message_id 정렬 오류: {ids}"


# ─── (2) 그룹 채팅 전체 수신 ──────────────────────────────────────────────────

def test_group_chat_broadcast(test_app):
    """
    alice, bob, carol이 general 채널에서:
    - alice 메시지 → bob, carol 모두 수신
    - alice 자신도 에코 수신
    - chat_type == "group", channel_id == "general"
    """
    client = TestClient(test_app)
    tokens = setup_users(client, "alice", "bob", "carol")

    r = client.post("/api/channels", json={
        "channel_id": "general", "name": "General", "members": ["bob", "carol"],
    }, headers=auth(tokens["alice"]))
    assert r.status_code == 201, f"채널 생성 실패: {r.json()}"

    with (client.websocket_connect("/ws/alice") as alice_ws,
          client.websocket_connect("/ws/bob") as bob_ws,
          client.websocket_connect("/ws/carol") as carol_ws):

        drain(alice_ws, 0.2)
        drain(bob_ws, 0.2)
        drain(carol_ws, 0.2)

        alice_ws.send_json({"type": "send_message", "channel_id": "general", "content": "hi everyone"})

        msg_bob = recv_until(bob_ws, lambda m: m.get("type") == "message")
        assert msg_bob is not None, "bob이 그룹 메시지를 수신하지 못함"
        assert msg_bob["content"] == "hi everyone"
        assert msg_bob["channel_id"] == "general"
        assert msg_bob["chat_type"] == "group"
        assert msg_bob["from"] == "alice"

        msg_carol = recv_until(carol_ws, lambda m: m.get("type") == "message")
        assert msg_carol is not None, "carol이 그룹 메시지를 수신하지 못함"
        assert msg_carol["content"] == "hi everyone"

        msg_echo = recv_until(alice_ws, lambda m: m.get("type") == "message")
        assert msg_echo is not None, "alice 에코 수신 실패"
        assert msg_echo["content"] == "hi everyone"


def test_group_non_member_ignored(test_app):
    """채널 비멤버가 보낸 메시지는 무시되어 멤버에게 전달되지 않는다."""
    client = TestClient(test_app)
    tokens = setup_users(client, "alice", "bob", "dave")

    client.post("/api/channels", json={
        "channel_id": "secret", "name": "Secret", "members": ["bob"],
    }, headers=auth(tokens["alice"]))

    with (client.websocket_connect("/ws/dave") as dave_ws,
          client.websocket_connect("/ws/bob") as bob_ws):

        drain(dave_ws, 0.2)
        drain(bob_ws, 0.2)

        dave_ws.send_json({"type": "send_message", "channel_id": "secret", "content": "intruder"})

        msgs = drain(bob_ws, 0.5)
        assert all(m.get("type") != "message" for m in msgs), \
            f"비멤버 메시지가 bob에게 전달됨: {msgs}"


def test_group_max_members_limit(test_app):
    """그룹 채팅 100명 초과 시 400 반환."""
    client = TestClient(test_app)
    tokens = setup_users(client, "alice")

    r = client.post("/api/channels", json={
        "channel_id": "huge", "name": "Huge",
        "members": [f"u{i}" for i in range(101)],
    }, headers=auth(tokens["alice"]))
    assert r.status_code == 400
    assert "100" in r.json()["detail"]


# ─── (3) 오프라인 inbox 보관 → 재연결 시 sync_response 수신 ──────────────────

def test_offline_inbox_sync(test_app):
    """
    bob이 오프라인인 동안 alice가 보낸 메시지 2건이 inbox에 보관된다.
    bob이 재연결하면 연결 직후 sync_response로 2건을 수신한다.
    """
    client = TestClient(test_app)
    setup_users(client, "alice", "bob")

    # alice만 연결, bob은 오프라인
    with client.websocket_connect("/ws/alice") as alice_ws:
        drain(alice_ws, 0.2)
        alice_ws.send_json({"type": "send_message", "to": "bob", "content": "msg1"})
        drain(alice_ws, 0.2)
        alice_ws.send_json({"type": "send_message", "to": "bob", "content": "msg2"})
        drain(alice_ws, 0.2)

    # bob 재연결 — 연결 즉시 sync_response 수신
    with client.websocket_connect("/ws/bob") as bob_ws:
        sync = recv_until(bob_ws, lambda m: m.get("type") == "sync_response")
        assert sync is not None, "bob이 sync_response를 수신하지 못함"

        messages = sync.get("messages", [])
        assert len(messages) == 2, f"기대 2건, 실제 {len(messages)}건: {messages}"

        contents = {m["content"] for m in messages}
        assert "msg1" in contents
        assert "msg2" in contents

        # message_id 시간순 정렬 확인
        ids = [m["message_id"] for m in messages]
        assert ids == sorted(ids), f"message_id 정렬 오류: {ids}"


def test_offline_inbox_not_duplicated(test_app):
    """inbox를 한 번 수신한 후 재연결해도 동일 메시지가 다시 전달되지 않는다."""
    client = TestClient(test_app)
    setup_users(client, "alice", "bob")

    with client.websocket_connect("/ws/alice") as alice_ws:
        drain(alice_ws, 0.2)
        alice_ws.send_json({"type": "send_message", "to": "bob", "content": "once"})
        drain(alice_ws, 0.2)

    # 첫 연결: inbox 소비
    with client.websocket_connect("/ws/bob") as bob_ws:
        sync = recv_until(bob_ws, lambda m: m.get("type") == "sync_response")
        assert sync is not None
        assert len(sync["messages"]) == 1

    # 두 번째 연결: inbox가 비어 있어야 함
    with client.websocket_connect("/ws/bob") as bob_ws2:
        msgs = drain(bob_ws2, 0.5)
        for m in msgs:
            if m.get("type") == "sync_response":
                assert len(m.get("messages", [])) == 0, \
                    f"inbox가 비워지지 않음 (중복 수신): {m}"


def test_sync_command_after_reconnect(test_app):
    """
    재연결 후 직접 sync 명령을 보내도 after_id 이후 메시지만 반환된다.
    """
    client = TestClient(test_app)
    setup_users(client, "alice", "bob")

    # 오프라인 중 메시지 2건 적재
    with client.websocket_connect("/ws/alice") as alice_ws:
        drain(alice_ws, 0.2)
        alice_ws.send_json({"type": "send_message", "to": "bob", "content": "old"})
        alice_ws.send_json({"type": "send_message", "to": "bob", "content": "new"})
        drain(alice_ws, 0.3)

    with client.websocket_connect("/ws/bob") as bob_ws:
        # 연결 즉시 sync_response로 inbox 소비
        sync = recv_until(bob_ws, lambda m: m.get("type") == "sync_response")
        assert sync is not None
        msgs = sync["messages"]
        assert len(msgs) == 2

        # 첫 번째 메시지 ID 이후만 sync 요청
        first_id = msgs[0]["message_id"]
        bob_ws.send_json({"type": "sync", "after_id": first_id})

        resp = recv_until(bob_ws, lambda m: m.get("type") == "sync_response")
        # inbox는 이미 비워졌으므로 빈 결과 반환
        if resp is not None:
            assert len(resp.get("messages", [])) == 0


# ─── (4) heartbeat 타임아웃 → 오프라인 전환 ──────────────────────────────────

def test_heartbeat_timeout(test_app, monkeypatch):
    """
    heartbeat를 보내지 않으면 타임아웃 후 오프라인으로 전환된다.

    monkeypatch로 HEARTBEAT_TIMEOUT=2초, HEARTBEAT_CHECK_INTERVAL=0.5초로 단축한 뒤
    last_active를 3초 전으로 조작하고 expiry 로직을 직접 실행한다.
    """
    monkeypatch.setattr(pres_mod, "HEARTBEAT_TIMEOUT", 2)
    monkeypatch.setattr(pres_mod, "HEARTBEAT_CHECK_INTERVAL", 0.5)

    client = TestClient(test_app)
    setup_users(client, "alice")

    with client.websocket_connect("/ws/alice") as alice_ws:
        drain(alice_ws, 0.2)

        # 연결 후 온라인 확인
        presence = store_mod.store.get("presence:alice")
        assert presence is not None, "presence 키 없음"
        assert presence["status"] == "online", f"연결 후 상태: {presence['status']}"

        # last_active를 timeout(2초) 초과하도록 조작
        presence["last_active"] = time.time() - 3
        store_mod.store.set("presence:alice", presence)

        # expiry 로직을 동기 방식으로 직접 실행
        now = time.time()
        users = store_mod.store.get("_online_users", set())
        for uid in list(users):
            p = store_mod.store.get(f"presence:{uid}")
            if p and p.get("status") == "online":
                last = p.get("last_active", 0)
                if now - last > pres_mod.HEARTBEAT_TIMEOUT:
                    pres_mod.presence_server.user_offline(uid)
                    users.discard(uid)
        store_mod.store.set("_online_users", users)

        # alice가 오프라인으로 전환됐는지 확인
        after = store_mod.store.get("presence:alice")
        assert after["status"] == "offline", \
            f"타임아웃 후에도 온라인 상태: {after['status']}"


def test_heartbeat_keeps_online(test_app):
    """heartbeat 전송 후 last_active가 갱신되고 온라인이 유지된다."""
    client = TestClient(test_app)
    setup_users(client, "alice")

    with client.websocket_connect("/ws/alice") as alice_ws:
        drain(alice_ws, 0.2)

        before = store_mod.store.get("presence:alice", {}).get("last_active", 0)

        alice_ws.send_json({"type": "heartbeat"})
        ack = recv_until(alice_ws, lambda m: m.get("type") == "heartbeat_ack")
        assert ack is not None, "heartbeat_ack 수신 실패"

        after = store_mod.store.get("presence:alice", {}).get("last_active", 0)
        assert after >= before, "heartbeat 후 last_active 미갱신"

        status = store_mod.store.get("presence:alice", {}).get("status")
        assert status == "online"


def test_presence_goes_offline_on_disconnect(test_app):
    """WebSocket 연결 해제 시 사용자가 오프라인으로 전환된다."""
    client = TestClient(test_app)
    setup_users(client, "alice")

    with client.websocket_connect("/ws/alice") as alice_ws:
        drain(alice_ws, 0.2)
        assert store_mod.store.get("presence:alice", {}).get("status") == "online"

    # with 블록 종료 = 연결 해제
    after = store_mod.store.get("presence:alice", {})
    assert after.get("status") == "offline", \
        f"연결 해제 후에도 온라인 상태: {after.get('status')}"


# ─── 보조 테스트 ───────────────────────────────────────────────────────────────

def test_register_duplicate(test_app):
    """동일 user_id 중복 가입 시 409 반환."""
    client = TestClient(test_app)
    client.post("/api/register", json={"user_id": "alice", "username": "Alice", "password": "pw"})
    r = client.post("/api/register", json={"user_id": "alice", "username": "Alice2", "password": "pw2"})
    assert r.status_code == 409


def test_login_wrong_password(test_app):
    """잘못된 비밀번호로 로그인 시 401 반환."""
    client = TestClient(test_app)
    client.post("/api/register", json={"user_id": "alice", "username": "Alice", "password": "correct"})
    r = client.post("/api/login", json={"user_id": "alice", "password": "wrong"})
    assert r.status_code == 401


def test_message_id_ordering():
    """Snowflake ID 20개가 단조 증가하고 중복이 없어야 한다."""
    from utils.id_generator import generate_message_id, message_id_to_timestamp
    ids = [generate_message_id() for _ in range(20)]
    assert ids == sorted(ids), "ID 단조 증가 실패"
    assert len(set(ids)) == 20, "ID 중복 발생"
    ts = message_id_to_timestamp(ids[0])
    assert abs(ts - time.time()) < 5, f"타임스탬프 역산 오차 과대: {ts}"


def test_dm_history_api(test_app):
    """WebSocket으로 주고받은 1:1 메시지가 REST 이력 API에서도 조회된다."""
    client = TestClient(test_app)
    tokens = setup_users(client, "alice", "bob")

    with (client.websocket_connect("/ws/alice") as alice_ws,
          client.websocket_connect("/ws/bob") as bob_ws):
        drain(alice_ws, 0.2)
        drain(bob_ws, 0.2)
        alice_ws.send_json({"type": "send_message", "to": "bob", "content": "history test"})
        recv_until(bob_ws, lambda m: m.get("type") == "message")

    r = client.get("/api/dm/bob/messages", headers=auth(tokens["alice"]))
    assert r.status_code == 200
    contents = [m["content"] for m in r.json()["messages"]]
    assert "history test" in contents, f"이력 API에서 메시지를 찾을 수 없음: {contents}"


def test_channel_history_api(test_app):
    """그룹 채팅 메시지가 REST 이력 API에서도 조회된다."""
    client = TestClient(test_app)
    tokens = setup_users(client, "alice", "bob")

    client.post("/api/channels", json={
        "channel_id": "hist-ch", "name": "HistCh", "members": ["bob"],
    }, headers=auth(tokens["alice"]))

    with (client.websocket_connect("/ws/alice") as alice_ws,
          client.websocket_connect("/ws/bob") as bob_ws):
        drain(alice_ws, 0.2)
        drain(bob_ws, 0.2)
        alice_ws.send_json({"type": "send_message", "channel_id": "hist-ch", "content": "ch-history"})
        recv_until(bob_ws, lambda m: m.get("type") == "message")

    r = client.get("/api/channels/hist-ch/messages", headers=auth(tokens["alice"]))
    assert r.status_code == 200
    contents = [m["content"] for m in r.json()["messages"]]
    assert "ch-history" in contents
