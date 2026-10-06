"""
main.py

애플리케이션 진입점
---------------------
채팅 시스템의 세 가지 서비스를 단일 FastAPI 인스턴스에 통합:
  1. API 서버 (무상태 서비스): /api/*
  2. WebSocket 채팅 서버 (상태 유지 서비스): /ws/{user_id}
  3. 접속 상태 서버: 내부 컴포넌트 (presence_server 싱글톤)

토이 프로젝트이므로 단일 프로세스로 실행한다.
실제 운영에서는 채팅 서버를 별도 인스턴스로 분리하고
로드밸런서 + 서비스 탐색(Zookeeper 등)으로 조율한다.

실행 방법:
  cd backend
  pip install -r requirements.txt
  uvicorn main:app --reload --port 8000
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from api.api_server import router as api_router
from chat.chat_server import websocket_endpoint
from presence.presence_server import presence_server


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    앱 시작 시 백그라운드 태스크를 실행한다.
    - heartbeat 만료 검사기: 30초 무응답 사용자를 오프라인으로 전환
    """
    task = asyncio.create_task(presence_server.run_expiry_checker())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="Chat System",
    description="12장 채팅 시스템 설계 기반 토이 프로젝트",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS: 프론트엔드(HTML 파일 직접 열기)에서 API 호출 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── 라우터 등록 ───────────────────────────────────────────────────────────────

# 무상태 REST API
app.include_router(api_router)


# ─── WebSocket 엔드포인트 ──────────────────────────────────────────────────────

@app.websocket("/ws/{user_id}")
async def websocket_route(websocket: WebSocket, user_id: str):
    """
    12장 §서비스 탐색:
    클라이언트는 /api/login 응답에서 받은 chat_server_url로 WebSocket 연결한다.
    URL: ws://host:8000/ws/{user_id}
    """
    await websocket_endpoint(websocket, user_id)


# ─── 정적 파일 (프론트엔드) ───────────────────────────────────────────────────

# frontend/ 디렉토리를 /static으로 서빙
# http://localhost:8000/static/index.html 로 접근
import os
frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(frontend_path):
    app.mount("/static", StaticFiles(directory=frontend_path), name="static")


@app.get("/")
def root():
    return {
        "message": "Chat System API",
        "docs": "/docs",
        "frontend": "/static/index.html",
        "websocket": "/ws/{user_id}",
    }
