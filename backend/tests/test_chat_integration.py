"""
tests/test_chat_integration.py

채팅 시스템 e2e 테스트
----------------------
conftest가 띄운 실제 uvicorn 서버에 httpx(REST)와 websockets(WebSocket)로 붙는다.

검증 항목:
  (1) 1:1 메시지 전송 → 수신자에게 즉시 도달
  (2) 그룹 채팅 메시지 → 모든 멤버 수신
  (3) 오프라인 inbox 보관 → 재연결 시 sync_response 수신
  (4) heartbeat 타임아웃 → 오프라인 전환
"""

import json
import time

import storage.store as store_mod
import presence.presence_server as pres_mod


# ─── 헬퍼 ─────────────────────────────────────────────────────────────────────

def auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def setup_users(api, *user_ids: str) -> dict[str, str]:
    """user_id 목록을 등록하고 {user_id: token} 반환."""
    tokens = {}
    for uid in user_ids:
        api.post("/api/register", json={
            "user_id": uid, "username": uid.capitalize(), "password": "pw",
        })
        r = api.post("/api/login", json={"user_id": uid, "password": "pw"})
        assert r.status_code == 200, f"로그인 실패: {uid}"
        tokens[uid] = r.json()["token"]
    return tokens


def recv_all(ws, timeout: float = 0.3) -> list[dict]:
    """timeout 동안 새 메시지가 없을 때까지 받은 메시지를 전부 반환."""
    result = []
    try:
        while True:
            result.append(json.loads(ws.recv(timeout=timeout)))
    except TimeoutError:
        return result


def recv_until(ws, predicate, timeout: float = 3.0) -> dict | None:
    """predicate를 만족하는 메시지가 올 때까지 수신. 없으면 None."""
    deadline = time.monotonic() + timeout
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            data = json.loads(ws.recv(timeout=remaining))
        except TimeoutError:
            return None
        if predicate(data):
            return data
    return None


def is_message(m: dict) -> bool:
    return m.get("type") == "message"


def wait_for(condition, timeout: float = 2.0) -> bool:
    """서버 쪽 비동기 처리(연결 해제 후 정리 등)가 끝날 때까지 폴링."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return condition()


def presence_status(user_id: str) -> str | None:
    return store_mod.store.get(f"presence:{user_id}", {}).get("status")


# ─── (1) 1:1 메시지 전송/수신 ─────────────────────────────────────────────────

def test_dm_send_receive(api, ws_connect):
    """
    alice → bob 1:1 메시지:
    - bob WebSocket에 즉시 도달
    - message_id 존재, chat_type == "dm"
    - alice 자신의 다른 단말(에코)에도 수신
    """
    setup_users(api, "alice", "bob")

    with ws_connect("alice") as alice_ws, ws_connect("bob") as bob_ws:
        alice_ws.send(json.dumps({"type": "send_message", "to": "bob", "content": "hello"}))

        msg = recv_until(bob_ws, is_message)
        assert msg is not None, "bob이 메시지를 수신하지 못함"
        assert msg["content"] == "hello"
        assert msg["from"] == "alice"
        assert msg["to"] == "bob"
        assert msg["chat_type"] == "dm"
        assert isinstance(msg["message_id"], int) and msg["message_id"] > 0

        echo = recv_until(alice_ws, is_message)
        assert echo is not None, "alice 에코 수신 실패"
        assert echo["content"] == "hello"


def test_dm_multiple_messages_ordered(api, ws_connect):
    """여러 메시지의 message_id가 단조 증가(시간순)해야 한다."""
    setup_users(api, "alice", "bob")

    with ws_connect("alice") as alice_ws, ws_connect("bob") as bob_ws:
        for i in range(3):
            alice_ws.send(json.dumps({"type": "send_message", "to": "bob", "content": f"msg{i}"}))

        received = [m for m in recv_all(bob_ws, 0.5) if is_message(m)]

        assert [m["content"] for m in received] == ["msg0", "msg1", "msg2"]
        ids = [m["message_id"] for m in received]
        assert ids == sorted(ids), f"message_id 정렬 오류: {ids}"


# ─── (2) 그룹 채팅 전체 수신 ──────────────────────────────────────────────────

def test_group_chat_broadcast(api, ws_connect):
    """
    alice, bob, carol이 general 채널에서:
    - alice 메시지 → bob, carol 모두 수신
    - alice 자신도 에코 수신
    - chat_type == "group", channel_id == "general"
    """
    tokens = setup_users(api, "alice", "bob", "carol")

    r = api.post("/api/channels", json={
        "channel_id": "general", "name": "General", "members": ["bob", "carol"],
    }, headers=auth(tokens["alice"]))
    assert r.status_code == 201, f"채널 생성 실패: {r.json()}"

    with ws_connect("alice") as alice_ws, ws_connect("bob") as bob_ws, ws_connect("carol") as carol_ws:
        alice_ws.send(json.dumps({"type": "send_message", "channel_id": "general", "content": "hi everyone"}))

        msg_bob = recv_until(bob_ws, is_message)
        assert msg_bob is not None, "bob이 그룹 메시지를 수신하지 못함"
        assert msg_bob["content"] == "hi everyone"
        assert msg_bob["channel_id"] == "general"
        assert msg_bob["chat_type"] == "group"
        assert msg_bob["from"] == "alice"

        msg_carol = recv_until(carol_ws, is_message)
        assert msg_carol is not None, "carol이 그룹 메시지를 수신하지 못함"
        assert msg_carol["content"] == "hi everyone"

        msg_echo = recv_until(alice_ws, is_message)
        assert msg_echo is not None, "alice 에코 수신 실패"
        assert msg_echo["content"] == "hi everyone"


def test_group_non_member_ignored(api, ws_connect):
    """채널 비멤버가 보낸 메시지는 무시되어 멤버에게 전달되지 않는다."""
    tokens = setup_users(api, "alice", "bob", "dave")

    api.post("/api/channels", json={
        "channel_id": "secret", "name": "Secret", "members": ["bob"],
    }, headers=auth(tokens["alice"]))

    with ws_connect("dave") as dave_ws, ws_connect("bob") as bob_ws:
        dave_ws.send(json.dumps({"type": "send_message", "channel_id": "secret", "content": "intruder"}))

        msgs = recv_all(bob_ws, 0.5)
        assert not any(is_message(m) for m in msgs), f"비멤버 메시지가 bob에게 전달됨: {msgs}"


def test_group_max_members_limit(api):
    """그룹 채팅 100명 초과 시 400 반환."""
    tokens = setup_users(api, "alice")

    r = api.post("/api/channels", json={
        "channel_id": "huge", "name": "Huge",
        "members": [f"u{i}" for i in range(101)],
    }, headers=auth(tokens["alice"]))
    assert r.status_code == 400
    assert "100" in r.json()["detail"]


# ─── (3) 오프라인 inbox 보관 → 재연결 시 sync_response 수신 ──────────────────

def test_offline_inbox_sync(api, ws_connect):
    """
    bob이 오프라인인 동안 alice가 보낸 메시지 2건이 inbox에 보관된다.
    bob이 재연결하면 연결 직후 sync_response로 2건을 수신한다.
    """
    setup_users(api, "alice", "bob")

    with ws_connect("alice") as alice_ws:
        for content in ("msg1", "msg2"):
            alice_ws.send(json.dumps({"type": "send_message", "to": "bob", "content": content}))
            assert recv_until(alice_ws, is_message) is not None

    with ws_connect("bob") as bob_ws:
        sync = recv_until(bob_ws, lambda m: m.get("type") == "sync_response")
        assert sync is not None, "bob이 sync_response를 수신하지 못함"

        messages = sync["messages"]
        assert [m["content"] for m in messages] == ["msg1", "msg2"]
        ids = [m["message_id"] for m in messages]
        assert ids == sorted(ids), f"message_id 정렬 오류: {ids}"


def test_offline_inbox_not_duplicated(api, ws_connect):
    """inbox를 한 번 수신한 후 재연결해도 동일 메시지가 다시 전달되지 않는다."""
    setup_users(api, "alice", "bob")

    with ws_connect("alice") as alice_ws:
        alice_ws.send(json.dumps({"type": "send_message", "to": "bob", "content": "once"}))
        assert recv_until(alice_ws, is_message) is not None

    with ws_connect("bob") as bob_ws:
        sync = recv_until(bob_ws, lambda m: m.get("type") == "sync_response")
        assert sync is not None
        assert len(sync["messages"]) == 1

    with ws_connect("bob") as bob_ws2:
        msgs = recv_all(bob_ws2, 0.5)
        assert msgs == [], f"inbox가 비워지지 않음 (중복 수신): {msgs}"


def test_live_dm_not_redelivered_on_reconnect(api, ws_connect):
    """온라인일 때 실시간으로 받은 DM은 재접속 시 sync_response로 다시 오지 않는다."""
    setup_users(api, "alice", "bob")

    with ws_connect("alice") as alice_ws, ws_connect("bob") as bob_ws:
        alice_ws.send(json.dumps({"type": "send_message", "to": "bob", "content": "live"}))
        assert recv_until(bob_ws, is_message) is not None

    with ws_connect("bob") as bob_ws:
        msgs = recv_all(bob_ws, 0.5)
        assert msgs == [], f"이미 받은 메시지가 재전달됨: {msgs}"


def test_group_inbox_only_for_offline_members(api, ws_connect):
    """그룹 메시지는 온라인 멤버에게는 실시간으로만, 오프라인 멤버에게는 inbox로 간다."""
    tokens = setup_users(api, "alice", "bob", "carol")
    api.post("/api/channels", json={
        "channel_id": "team", "name": "Team", "members": ["bob", "carol"],
    }, headers=auth(tokens["alice"]))

    with ws_connect("alice") as alice_ws, ws_connect("bob") as bob_ws:
        alice_ws.send(json.dumps({"type": "send_message", "channel_id": "team", "content": "standup"}))
        assert recv_until(bob_ws, is_message) is not None

    with ws_connect("bob") as bob_ws:
        assert recv_all(bob_ws, 0.5) == [], "온라인으로 받은 그룹 메시지가 재전달됨"

    with ws_connect("carol") as carol_ws:
        sync = recv_until(carol_ws, lambda m: m.get("type") == "sync_response")
        assert sync is not None, "오프라인이던 carol이 inbox 메시지를 받지 못함"
        assert [m["content"] for m in sync["messages"]] == ["standup"]


def test_sync_command_after_reconnect(api, ws_connect):
    """연결 시 inbox를 이미 비웠으므로 이후 sync 요청에는 돌려줄 메시지가 없다."""
    setup_users(api, "alice", "bob")

    with ws_connect("alice") as alice_ws:
        for content in ("old", "new"):
            alice_ws.send(json.dumps({"type": "send_message", "to": "bob", "content": content}))
            assert recv_until(alice_ws, is_message) is not None

    with ws_connect("bob") as bob_ws:
        sync = recv_until(bob_ws, lambda m: m.get("type") == "sync_response")
        assert sync is not None
        msgs = sync["messages"]
        assert len(msgs) == 2

        bob_ws.send(json.dumps({"type": "sync", "after_id": msgs[0]["message_id"]}))
        assert recv_all(bob_ws, 0.5) == []


# ─── (4) heartbeat 타임아웃 → 오프라인 전환 ──────────────────────────────────

def test_heartbeat_timeout(api, ws_connect, monkeypatch):
    """
    heartbeat를 보내지 않으면 타임아웃 후 오프라인으로 전환된다.
    HEARTBEAT_TIMEOUT을 2초로 줄이고 last_active를 3초 전으로 돌린 뒤 만료 로직을 실행한다.
    """
    monkeypatch.setattr(pres_mod, "HEARTBEAT_TIMEOUT", 2)
    setup_users(api, "alice")

    with ws_connect("alice"):
        assert wait_for(lambda: presence_status("alice") == "online")

        assert pres_mod.presence_server.expire_stale() == [], "방금 접속한 사용자가 만료됨"

        presence = store_mod.store.get("presence:alice")
        presence["last_active"] = time.time() - 3
        store_mod.store.set("presence:alice", presence)

        assert pres_mod.presence_server.expire_stale() == ["alice"]
        assert presence_status("alice") == "offline"


def test_heartbeat_keeps_online(api, ws_connect):
    """heartbeat 전송 후 last_active가 갱신되고 온라인이 유지된다."""
    setup_users(api, "alice")

    with ws_connect("alice") as alice_ws:
        assert wait_for(lambda: presence_status("alice") == "online")
        before = store_mod.store.get("presence:alice")["last_active"]

        alice_ws.send(json.dumps({"type": "heartbeat"}))
        ack = recv_until(alice_ws, lambda m: m.get("type") == "heartbeat_ack")
        assert ack is not None, "heartbeat_ack 수신 실패"

        after = store_mod.store.get("presence:alice")["last_active"]
        assert after >= before, "heartbeat 후 last_active 미갱신"
        assert presence_status("alice") == "online"


def test_presence_goes_offline_on_disconnect(api, ws_connect):
    """WebSocket 연결 해제 시 사용자가 오프라인으로 전환된다."""
    setup_users(api, "alice")

    with ws_connect("alice"):
        assert wait_for(lambda: presence_status("alice") == "online")

    assert wait_for(lambda: presence_status("alice") == "offline"), \
        f"연결 해제 후에도 상태가 {presence_status('alice')}"


def test_presence_event_sent_to_friends(api, ws_connect):
    """친구가 접속하거나 나가면 presence 이벤트를 받고, 친구가 아니면 받지 않는다."""
    tokens = setup_users(api, "alice", "bob", "carol")
    r = api.post("/api/friends", json={"friend_id": "bob"}, headers=auth(tokens["alice"]))
    assert r.status_code == 200

    def is_presence(m: dict) -> bool:
        return m.get("type") == "presence"

    with ws_connect("bob") as bob_ws, ws_connect("carol") as carol_ws:
        with ws_connect("alice"):
            online = recv_until(bob_ws, is_presence)
            assert online == {"type": "presence", "user_id": "alice", "status": "online"}

        offline = recv_until(bob_ws, is_presence)
        assert offline == {"type": "presence", "user_id": "alice", "status": "offline"}

        assert not any(is_presence(m) for m in recv_all(carol_ws, 0.3)), \
            "친구가 아닌 carol에게 presence 이벤트가 전달됨"


# ─── 보조 테스트 ───────────────────────────────────────────────────────────────

def test_register_duplicate(api):
    """동일 user_id 중복 가입 시 409 반환."""
    api.post("/api/register", json={"user_id": "alice", "username": "Alice", "password": "pw"})
    r = api.post("/api/register", json={"user_id": "alice", "username": "Alice2", "password": "pw2"})
    assert r.status_code == 409


def test_login_wrong_password(api):
    """잘못된 비밀번호로 로그인 시 401 반환."""
    api.post("/api/register", json={"user_id": "alice", "username": "Alice", "password": "correct"})
    r = api.post("/api/login", json={"user_id": "alice", "password": "wrong"})
    assert r.status_code == 401


def test_message_id_ordering():
    """Snowflake ID 20개가 단조 증가하고 중복이 없어야 한다."""
    from utils.id_generator import generate_message_id, message_id_to_timestamp
    ids = [generate_message_id() for _ in range(20)]
    assert ids == sorted(ids), "ID 단조 증가 실패"
    assert len(set(ids)) == 20, "ID 중복 발생"
    ts = message_id_to_timestamp(ids[0])
    assert abs(ts - time.time()) < 5, f"타임스탬프 역산 오차 과대: {ts}"


def test_dm_history_api(api, ws_connect):
    """WebSocket으로 주고받은 1:1 메시지가 REST 이력 API에서도 조회된다."""
    tokens = setup_users(api, "alice", "bob")

    with ws_connect("alice") as alice_ws, ws_connect("bob") as bob_ws:
        alice_ws.send(json.dumps({"type": "send_message", "to": "bob", "content": "history test"}))
        assert recv_until(bob_ws, is_message) is not None

    r = api.get("/api/dm/bob/messages", headers=auth(tokens["alice"]))
    assert r.status_code == 200
    contents = [m["content"] for m in r.json()["messages"]]
    assert "history test" in contents, f"이력 API에서 메시지를 찾을 수 없음: {contents}"


def test_channel_history_api(api, ws_connect):
    """그룹 채팅 메시지가 REST 이력 API에서도 조회된다."""
    tokens = setup_users(api, "alice", "bob")

    api.post("/api/channels", json={
        "channel_id": "hist-ch", "name": "HistCh", "members": ["bob"],
    }, headers=auth(tokens["alice"]))

    with ws_connect("alice") as alice_ws, ws_connect("bob") as bob_ws:
        alice_ws.send(json.dumps({"type": "send_message", "channel_id": "hist-ch", "content": "ch-history"}))
        assert recv_until(bob_ws, is_message) is not None

    r = api.get("/api/channels/hist-ch/messages", headers=auth(tokens["alice"]))
    assert r.status_code == 200
    contents = [m["content"] for m in r.json()["messages"]]
    assert "ch-history" in contents
