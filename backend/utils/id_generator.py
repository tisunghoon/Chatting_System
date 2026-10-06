"""
utils/id_generator.py

Snowflake-like 64-bit 메시지 ID 생성기
---------------------------------------
설계 결정:
  - message_id는 시간순 정렬이 가능해야 한다 (12장 요구사항)
  - 같은 채널/세션 내에서만 유일성 보장하면 충분하지만,
    단순성을 위해 전역 유일성을 보장하는 구조로 구현

ID 구조 (64비트):
  [ 41bit: ms 타임스탬프 ] [ 10bit: worker_id ] [ 12bit: sequence ]

  - 타임스탬프: 기준 에포크(EPOCH)로부터 경과 밀리초 → ~69년 사용 가능
  - worker_id: 멀티 서버 환경 대비 (지금은 단일 서버라 0 고정)
  - sequence: 같은 밀리초 내 최대 4096개 메시지 처리 가능

NoSQL(키-값 저장소)에서 auto_increment가 없기 때문에
이 방식으로 정렬 가능한 고유 ID를 직접 생성한다.
"""

import time
import threading

# 2024-01-01 00:00:00 UTC 기준 에포크 (밀리초)
# 타임스탬프 비트를 아끼기 위해 커스텀 에포크를 사용한다
EPOCH = 1704067200000

WORKER_ID_BITS = 10
SEQUENCE_BITS = 12

MAX_WORKER_ID = (1 << WORKER_ID_BITS) - 1       # 1023
MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1          # 4095

WORKER_ID_SHIFT = SEQUENCE_BITS                   # 12
TIMESTAMP_SHIFT = WORKER_ID_BITS + SEQUENCE_BITS  # 22


class SnowflakeIDGenerator:
    """스레드 안전한 Snowflake ID 생성기"""

    def __init__(self, worker_id: int = 0):
        if worker_id > MAX_WORKER_ID or worker_id < 0:
            raise ValueError(f"worker_id must be between 0 and {MAX_WORKER_ID}")
        self.worker_id = worker_id
        self._sequence = 0
        self._last_timestamp = -1
        self._lock = threading.Lock()

    def _current_ms(self) -> int:
        return int(time.time() * 1000)

    def _wait_next_ms(self, last_timestamp: int) -> int:
        ts = self._current_ms()
        while ts <= last_timestamp:
            ts = self._current_ms()
        return ts

    def next_id(self) -> int:
        with self._lock:
            ts = self._current_ms()

            if ts < self._last_timestamp:
                # 시계가 뒤로 가는 경우 (NTP 동기화 등) 대기
                ts = self._wait_next_ms(self._last_timestamp)

            if ts == self._last_timestamp:
                self._sequence = (self._sequence + 1) & MAX_SEQUENCE
                if self._sequence == 0:
                    # 같은 ms 내 sequence 소진 → 다음 ms 대기
                    ts = self._wait_next_ms(self._last_timestamp)
            else:
                self._sequence = 0

            self._last_timestamp = ts

            return (
                ((ts - EPOCH) << TIMESTAMP_SHIFT)
                | (self.worker_id << WORKER_ID_SHIFT)
                | self._sequence
            )


# 싱글톤: 서버 전체에서 단일 생성기 사용
_generator = SnowflakeIDGenerator(worker_id=0)


def generate_message_id() -> int:
    """전역 메시지 ID 생성 (시간순 정렬 가능, 전역 유일)"""
    return _generator.next_id()


def message_id_to_timestamp(message_id: int) -> float:
    """message_id에서 생성 시각(Unix timestamp 초)을 역산"""
    ms = (message_id >> TIMESTAMP_SHIFT) + EPOCH
    return ms / 1000.0
