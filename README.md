# 채팅 시스템 토이 프로젝트

가상 인터뷰 책 **12장 채팅 시스템 설계**를 기반으로 구현한 토이 프로젝트입니다.

## 아키텍처 개요

```
┌─────────────────────────────────────────────────────┐
│                  단일 FastAPI 서버                    │
│                                                     │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │  API 서버   │  │  채팅 서버   │  │ 접속상태   │ │
│  │ (무상태)    │  │ (WebSocket)  │  │   서버     │ │
│  │ /api/*      │  │ /ws/{uid}    │  │ heartbeat  │ │
│  └─────────────┘  └──────────────┘  └────────────┘ │
│                          │                          │
│              ┌───────────────────────┐              │
│              │   인메모리 키-값 저장소│              │
│              │  (Redis 대체)         │              │
│              │  - 채팅 이력          │              │
│              │  - 수신자별 inbox 큐  │              │
│              │  - 접속 상태          │              │
│              └───────────────────────┘              │
└─────────────────────────────────────────────────────┘
```

## 구현된 기능

| 기능 | 설명 |
|------|------|
| **1:1 채팅** | WebSocket 양방향 실시간 메시지 |
| **그룹 채팅** | 최대 100명, 수신자별 inbox 큐 복사 방식 |
| **여러 단말 동기화** | `cur_max_message_id` 기반, 재연결 시 미수신 메시지 자동 수신 |
| **접속 상태** | 온라인/오프라인, 하트비트 5초 주기, 30초 무응답 시 오프라인 전환 |
| **Snowflake ID** | 41bit 타임스탬프 + 10bit worker + 12bit sequence → 시간순 정렬 가능 |
| **친구 목록** | 양방향 친구 관계, presence 이벤트 수신 범위 결정 |

## 설계 결정 요약

### 프로토콜: WebSocket
- HTTP 폴링/롱폴링 대신 WebSocket을 선택
- 서버 → 클라이언트 비동기 push 필요 (새 메시지, 접속 상태 변경)

### 저장소: 인메모리 (Redis 대체)
- 채팅 이력은 키-값 저장소가 적합 (수평 확장, 낮은 지연, long-tail 데이터)
- 토이 프로젝트 범위에서는 외부 의존성 없이 `InMemoryStore`로 구현

### 메시지 ID: Snowflake-like
- NoSQL에는 `auto_increment`가 없으므로 직접 생성
- 41bit 타임스탬프 → 시간순 정렬 보장

### 그룹 채팅 메시지 흐름 (위챗 방식)
- 발신 메시지를 수신자별 inbox 큐에 복사
- 단말이 연결되면 자신의 큐만 폴링하면 되므로 동기화 단순화
- 100명 제한이 있으므로 복사 비용이 허용 가능

## 실행 방법

```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

브라우저에서 [http://localhost:8000/static/index.html](http://localhost:8000/static/index.html) 접속

API 문서: [http://localhost:8000/docs](http://localhost:8000/docs)

## 프로젝트 구조

```
Chatting_System/
├── backend/
│   ├── main.py                  # FastAPI 앱 진입점
│   ├── requirements.txt
│   ├── api/
│   │   └── api_server.py        # 무상태 REST API (로그인/회원가입/채널 관리)
│   ├── chat/
│   │   └── chat_server.py       # WebSocket 채팅 서버 (1:1 + 그룹)
│   ├── presence/
│   │   └── presence_server.py   # 접속 상태 서버 (heartbeat + 만료 검사)
│   ├── storage/
│   │   └── store.py             # 인메모리 키-값 저장소 (Redis 대체)
│   └── utils/
│       └── id_generator.py      # Snowflake ID 생성기
└── frontend/
    └── index.html               # 순수 HTML/JS 채팅 클라이언트
```

## 범위 밖 (의도적 제외)

- 종단 간 암호화
- 미디어 파일 전송 (사진, 동영상)
- 멀티 서버 분산 (Zookeeper 서비스 탐색, Redis Pub/Sub)
- 푸시 알림 서버 (오프라인 사용자 알림)
