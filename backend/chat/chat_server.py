"""
chat/chat_server.py

WebSocket 채팅 서버
---------------------
설계 결정 (12장 §메시지 흐름):

1. 클라이언트 ↔ 서버 통신: WebSocket (양방향, 낮은 지연)
   - HTTP 폴링/롱폴링 대신 WebSocket 선택 이유:
     서버가 클라이언트에게 비동기로 메시지를 push할 수 있어야 하기 때문.

2. 메시지 처리 흐름 (1:1):
   클라이언트A → 채팅서버 → ID 생성 → inbox 큐(B) → 키-값 저장소
                                     → B가 온라인이면 즉시 WebSocket 전달
                                     → B가 오프라인이면 inbox에만 보관

3. 메시지 처리 흐름 (그룹):
   클라이언트A → 채팅서버 → ID 생성 → 각 수신자 inbox 큐에 복사
                                     → 온라인 수신자에게 즉시 WebSocket 전달

4. 여러 단말 동기화:
   - 각 단말은 cur_max_message_id를 추적
   - WebSocket 연결 시 after_id 파라미터로 미수신 메시지를 pull

5. 그룹 채팅 인원 제한: 100명 (12장 요구사항)

WebSocket 이벤트 타입:
  클라이언트 → 서버:
    - {"type": "send_message", "to": user_id, "content": "..."}        (1:1)
    - {"type": "send_message", "channel_id": "...", "content": "..."}  (그룹)
    - {"type": "heartbeat"}
    - {"type": "sync", "after_id": 123456}  (단말 동기화)

  서버 → 클라이언트:
    - {"type": "message", ...message_dict}
    - {"type": "presence", "user_id": "...", "status": "online|offline"}
    - {"type": "error", "message": "..."}
    - {"type": "sync_response", "messages": [...]}
"""

import json
import asyncio
from typing import Optional

from fastapi import WebSocket, WebSocketDisconnect

from storage.store import store
from utils.id_generator import generate_message_id
from presence.presence_server import presence_server

# 그룹 채팅 최대 인원 (12장 요구사항: 최대 100명)
GROUP_MAX_MEMBERS = 100


class ConnectionManager:
    """
    WebSocket 연결 관리자.
    상태 유지 서비스(Stateful Service): 각 클라이언트와 독립적인 WebSocket 연결을 유지한다.
    실제 멀티 서버 환경에서는 이 맵이 Redis Pub/Sub으로 대체된다.
    """

    def __init__(self):
        # user_id → list[WebSocket] (한 계정으로 여러 단말 동시 접속 지원)
        self._connections: dict[str, list[WebSocket]] = {}

    async def connect(self, user_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        if user_id not in self._connections:
            self._connections[user_id] = []
        self._connections[user_id].append(websocket)

    def disconnect(self, user_id: str, websocket: WebSocket) -> None:
        if user_id in self._connections:
            self._connections[user_id].discard(websocket) if hasattr(
                self._connections[user_id], "discard"
            ) else None
            try:
                self._connections[user_id].remove(websocket)
            except ValueError:
                pass
            if not self._connections[user_id]:
                del self._connections[user_id]

    def is_online(self, user_id: str) -> bool:
        return bool(self._connections.get(user_id))

    async def send_to_user(self, user_id: str, payload: dict) -> None:
        """사용자의 모든 단말에 메시지 전송 (여러 단말 동기화)"""
        sockets = self._connections.get(user_id, [])
        dead = []
        for ws in sockets:
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(user_id, ws)

    async def broadcast_to_users(self, user_ids: list[str], payload: dict) -> None:
        """여러 사용자에게 동시 전송"""
        tasks = [self.send_to_user(uid, payload) for uid in user_ids]
        await asyncio.gather(*tasks, return_exceptions=True)


# 싱글톤
manager = ConnectionManager()


def _dm_channel_key(user_a: str, user_b: str) -> str:
    """1:1 채팅 채널 키: 두 user_id를 정렬하여 방향에 무관하게 동일한 키 생성"""
    return "dm:" + ":".join(sorted([user_a, user_b]))


def _group_channel_key(channel_id: str) -> str:
    return f"group:{channel_id}"


async def handle_send_message(sender_id: str, data: dict) -> None:
    """
    메시지 전송 처리 (1:1 및 그룹).
    12장 §1:1 채팅 메시지 처리 흐름을 구현한다.
    """
    content = data.get("content", "").strip()
    if not content:
        return

    # ── 1:1 메시지 ───────────────────────────────────────────────
    if "to" in data:
        recipient_id = data["to"]
        channel_key = _dm_channel_key(sender_id, recipient_id)

        # Step 2: ID 생성기로 message_id 결정
        message_id = generate_message_id()

        message = {
            "type": "message",
            "message_id": message_id,
            "channel_key": channel_key,
            "from": sender_id,
            "to": recipient_id,
            "content": content,
            "chat_type": "dm",
        }

        # Step 4: 키-값 저장소에 영구 보관
        store.append_message(channel_key, message)

        # Step 3 → 5a: 수신자 inbox 큐에 복사 후 즉시 전달 시도
        store.push_inbox(recipient_id, message)

        if manager.is_online(recipient_id):
            # 5a: 수신자가 온라인 → 즉시 WebSocket 전달
            await manager.send_to_user(recipient_id, message)
        # else: 5b: 오프라인 → inbox에만 보관 (실제 운영: 푸시 알림 서버로 전달)

        # 발신자의 다른 단말에도 동기화 (여러 단말 지원)
        await manager.send_to_user(sender_id, message)

    # ── 그룹 메시지 ──────────────────────────────────────────────
    elif "channel_id" in data:
        channel_id = data["channel_id"]
        channel_key = _group_channel_key(channel_id)

        # 채널 멤버 목록 조회
        members: list = store.get(f"channel_members:{channel_id}", [])
        if sender_id not in members:
            return  # 채널 멤버가 아니면 무시

        message_id = generate_message_id()
        message = {
            "type": "message",
            "message_id": message_id,
            "channel_key": channel_key,
            "channel_id": channel_id,
            "from": sender_id,
            "content": content,
            "chat_type": "group",
        }

        # 그룹 채팅 이력 저장
        store.append_message(channel_key, message)

        # 12장 §소그룹 채팅 메시지 흐름:
        # 발신자를 제외한 모든 수신자의 inbox 큐에 메시지 복사
        recipients = [m for m in members if m != sender_id]
        for recipient_id in recipients:
            store.push_inbox(recipient_id, message)

        # 온라인 수신자에게 즉시 전달
        await manager.broadcast_to_users(recipients, message)
        # 발신자의 모든 단말에도 전송
        await manager.send_to_user(sender_id, message)


async def handle_sync(user_id: str, data: dict, websocket: WebSocket) -> None:
    """
    단말 동기화: cur_max_message_id 이후의 미수신 메시지를 inbox에서 전달.
    12장 §여러 단말 사이의 메시지 동기화 구현.
    """
    after_id: Optional[int] = data.get("after_id")
    # inbox에서 미수신 메시지 꺼내기
    pending = store.pop_inbox(user_id, after_id=after_id)
    if pending:
        await websocket.send_json({
            "type": "sync_response",
            "messages": pending,
        })


async def notify_presence_to_friends(user_id: str, status: str) -> None:
    """
    접속 상태 변경을 친구들에게 통지.
    12장 §상태 정보의 전송: 발행-구독 모델.
    A의 상태 변경 → A와 친구인 사용자들에게 presence 이벤트 전달.
    """
    friends: list = store.get(f"friends:{user_id}", [])
    payload = {"type": "presence", "user_id": user_id, "status": status}
    await manager.broadcast_to_users(friends, payload)


async def websocket_endpoint(websocket: WebSocket, user_id: str) -> None:
    """
    WebSocket 엔드포인트 핸들러.
    각 클라이언트는 이 함수를 통해 채팅 서버와 연결을 유지한다.
    """
    await manager.connect(user_id, websocket)
    presence_server.register_online(user_id)
    presence_server.user_online(user_id)

    # 접속 상태 변경을 친구들에게 비동기 통지
    asyncio.create_task(notify_presence_to_friends(user_id, "online"))

    # 연결 즉시 inbox에 쌓인 미수신 메시지 전달
    pending = store.pop_inbox(user_id)
    if pending:
        await websocket.send_json({"type": "sync_response", "messages": pending})

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_json({"type": "error", "message": "Invalid JSON"})
                continue

            event_type = data.get("type")

            if event_type == "send_message":
                await handle_send_message(user_id, data)

            elif event_type == "heartbeat":
                # 12장 §접속 장애: 5초마다 heartbeat → 30초 무응답 시 오프라인
                presence_server.heartbeat(user_id)
                await websocket.send_json({"type": "heartbeat_ack"})

            elif event_type == "sync":
                await handle_sync(user_id, data, websocket)

            else:
                await websocket.send_json({
                    "type": "error",
                    "message": f"Unknown event type: {event_type}",
                })

    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(user_id, websocket)
        presence_server.unregister_online(user_id)
        presence_server.user_offline(user_id)
        asyncio.create_task(notify_presence_to_friends(user_id, "offline"))
