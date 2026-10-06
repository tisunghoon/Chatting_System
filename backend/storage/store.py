"""
storage/store.py

인메모리 키-값 저장소 (Redis 대체)
-------------------------------------
설계 결정:
  - 12장에서는 채팅 이력 보관에 키-값 저장소(Redis/HBase 등)를 권장한다.
  - 토이 프로젝트 범위에서는 외부 의존성 없이 인메모리로 구현한다.
  - 실제 운영에서는 이 클래스를 Redis 클라이언트로 교체하면 된다.

데이터 모델 (12장 §저장소):
  - 1:1 채팅 메시지: key = "dm:{user_a_id}:{user_b_id}" (정렬된 쌍)
  - 그룹 채팅 메시지: key = "group:{channel_id}"
  - 각 key에는 message_id 순서로 정렬된 메시지 리스트를 보관
  - 수신자별 메시지 동기화 큐: key = "inbox:{user_id}"

접속 상태:
  - key = "presence:{user_id}" → {"status": "online"|"offline", "last_active": timestamp}
"""

import threading
from typing import Any, Optional
from collections import defaultdict


class InMemoryStore:
    """
    스레드 안전한 인메모리 키-값 저장소.
    메시지 이력은 sorted list(message_id 오름차순)로 보관한다.
    """

    def __init__(self):
        self._data: dict[str, Any] = {}
        # 채팅 이력: {channel_key: [message_dict, ...]} (message_id 순 정렬)
        self._messages: dict[str, list] = defaultdict(list)
        self._lock = threading.RLock()

    # ── 일반 키-값 ──────────────────────────────────────────────

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._data.get(key, default)

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    # ── 채팅 이력 (메시지 append + range 조회) ──────────────────

    def append_message(self, channel_key: str, message: dict) -> None:
        """
        메시지를 채널에 추가한다.
        message_id가 Snowflake ID이므로 append 순서 = 시간 순서가 보장된다.
        """
        with self._lock:
            self._messages[channel_key].append(message)

    def get_messages(
        self,
        channel_key: str,
        after_id: Optional[int] = None,
        limit: int = 50,
    ) -> list[dict]:
        """
        채널의 메시지를 조회한다.
        after_id가 주어지면 해당 ID보다 큰 메시지만 반환 (단말 동기화용).
        """
        with self._lock:
            msgs = self._messages[channel_key]
            if after_id is not None:
                msgs = [m for m in msgs if m["message_id"] > after_id]
            # 최신 메시지 기준 limit개 반환
            return msgs[-limit:]

    # ── 수신자별 메시지 동기화 큐 (inbox) ───────────────────────

    def push_inbox(self, user_id: str, message: dict) -> None:
        """
        소그룹/1:1 메시지를 수신자의 inbox 큐에 복사한다.
        12장 §소그룹 채팅 메시지 흐름: 수신자별 독립 큐로 동기화 단순화.
        """
        key = f"inbox:{user_id}"
        with self._lock:
            if key not in self._data:
                self._data[key] = []
            self._data[key].append(message)

    def pop_inbox(self, user_id: str, after_id: Optional[int] = None) -> list[dict]:
        """수신자의 inbox에서 미확인 메시지를 꺼낸다."""
        key = f"inbox:{user_id}"
        with self._lock:
            msgs: list = self._data.get(key, [])
            if after_id is not None:
                result = [m for m in msgs if m["message_id"] > after_id]
            else:
                result = list(msgs)
            # 꺼낸 메시지는 큐에서 제거
            self._data[key] = []
            return result


# 싱글톤: 서버 전체에서 단일 저장소 사용
store = InMemoryStore()
