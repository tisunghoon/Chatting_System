"""
presence/presence_server.py

접속 상태 서버 (Presence Server)
-----------------------------------
설계 결정 (12장 §접속상태 표시):
  - 사용자의 온라인/오프라인 상태를 키-값 저장소에 보관
  - 하트비트 기반 접속 감지:
    - 클라이언트가 5초마다 heartbeat 이벤트를 전송
    - 마지막 heartbeat로부터 30초 이상 경과 시 오프라인 처리
  - 상태 변경 시 발행-구독(pub/sub) 모델로 친구에게 통지:
    - 각 친구 관계마다 채널을 두고, 상태 변경 시 해당 채널에 이벤트 발행
    - 현재 구현에서는 WebSocket 연결 맵을 통해 직접 전달

하트비트 타이머:
  - asyncio.create_task로 백그라운드에서 주기적으로 만료 검사
  - 실제 운영에서는 Redis TTL 또는 별도 cron으로 처리
"""

import asyncio
import time
from typing import Callable

from storage.store import store


HEARTBEAT_TIMEOUT = 30  # 초: 이 시간 동안 heartbeat 없으면 오프라인
HEARTBEAT_CHECK_INTERVAL = 10  # 초: 만료 검사 주기


class PresenceServer:
    def __init__(self):
        # user_id → 상태 변경 콜백 (WebSocket으로 친구에게 전파할 때 사용)
        self._status_change_callbacks: list[Callable] = []

    # ── 상태 갱신 ────────────────────────────────────────────────

    def user_online(self, user_id: str) -> None:
        """사용자 로그인 또는 WebSocket 연결 시 호출"""
        store.set(f"presence:{user_id}", {
            "status": "online",
            "last_active": time.time(),
        })
        self._notify_status_change(user_id, "online")

    def user_offline(self, user_id: str) -> None:
        """사용자 로그아웃 또는 WebSocket 연결 해제 시 호출"""
        presence = store.get(f"presence:{user_id}", {})
        presence["status"] = "offline"
        store.set(f"presence:{user_id}", presence)
        self._notify_status_change(user_id, "offline")

    def heartbeat(self, user_id: str) -> None:
        """
        클라이언트가 5초마다 보내는 heartbeat 수신.
        last_active 타임스탬프를 갱신하여 온라인 상태를 유지한다.
        """
        presence = store.get(f"presence:{user_id}")
        if presence:
            presence["last_active"] = time.time()
            presence["status"] = "online"
            store.set(f"presence:{user_id}", presence)
        else:
            self.user_online(user_id)

    # ── 상태 조회 ────────────────────────────────────────────────

    def get_status(self, user_id: str) -> str:
        """사용자의 현재 접속 상태 반환"""
        presence = store.get(f"presence:{user_id}")
        if not presence:
            return "offline"
        return presence.get("status", "offline")

    def get_presence(self, user_id: str) -> dict:
        """사용자의 전체 접속 정보 반환"""
        return store.get(f"presence:{user_id}", {"status": "offline", "last_active": None})

    # ── 만료 검사 (하트비트 타임아웃) ────────────────────────────

    async def run_expiry_checker(self) -> None:
        """
        백그라운드 태스크: HEARTBEAT_TIMEOUT 초과 사용자를 오프라인으로 전환.
        uvicorn lifespan에서 asyncio.create_task로 실행한다.
        """
        while True:
            await asyncio.sleep(HEARTBEAT_CHECK_INTERVAL)
            now = time.time()
            # 접속 중인 모든 사용자의 presence 검사
            # (실제 운영에서는 Redis TTL로 처리)
            users = store.get("_online_users", set())
            expired = []
            for uid in list(users):
                presence = store.get(f"presence:{uid}")
                if not presence:
                    continue
                if presence.get("status") == "online":
                    last = presence.get("last_active", 0)
                    if now - last > HEARTBEAT_TIMEOUT:
                        expired.append(uid)

            for uid in expired:
                self.user_offline(uid)
                users.discard(uid)
            store.set("_online_users", users)

    def register_online(self, user_id: str) -> None:
        """온라인 사용자 집합에 등록 (만료 검사용)"""
        users = store.get("_online_users", set())
        users.add(user_id)
        store.set("_online_users", users)

    def unregister_online(self, user_id: str) -> None:
        users = store.get("_online_users", set())
        users.discard(user_id)
        store.set("_online_users", users)

    # ── 상태 변경 통지 (발행-구독 모델) ──────────────────────────

    def add_status_change_callback(self, callback: Callable) -> None:
        """
        상태 변경 시 호출할 콜백 등록.
        채팅 서버가 이 콜백을 통해 친구들에게 WebSocket 메시지를 전달한다.
        발행-구독 모델: A의 상태 변경 → A-B, A-C 채널에 이벤트 발행.
        """
        self._status_change_callbacks.append(callback)

    def _notify_status_change(self, user_id: str, status: str) -> None:
        for cb in self._status_change_callbacks:
            cb(user_id, status)


# 싱글톤
presence_server = PresenceServer()
