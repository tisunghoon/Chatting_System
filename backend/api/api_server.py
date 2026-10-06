"""
api/api_server.py

API 서버 (무상태 서비스, Stateless Service)
----------------------------------------------
설계 결정 (12장 §개략적 설계안):
  - 로그인, 회원가입, 프로파일 조회/수정, 채널 관리 등
    요청/응답 형태의 전통적인 REST API를 처리한다.
  - 무상태 서버이므로 로드밸런서 뒤에 여러 인스턴스를 두는 수평 확장이 가능하다.
  - 인증: JWT 토큰 기반 (실제 운영 수준 간소화 — 서명 없이 user_id를 페이로드로 사용)

가정:
  - 비밀번호는 plaintext 저장 (토이 프로젝트 — 실제 운영에서는 bcrypt 필수)
  - JWT 대신 단순 Bearer 토큰(user_id 자체)을 사용
"""

from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel
from typing import Optional

from storage.store import store
from presence.presence_server import presence_server

router = APIRouter(prefix="/api")


# ─── 요청/응답 스키마 ──────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    user_id: str
    username: str
    password: str


class LoginRequest(BaseModel):
    user_id: str
    password: str


class UpdateProfileRequest(BaseModel):
    username: Optional[str] = None


class CreateChannelRequest(BaseModel):
    channel_id: str
    name: str
    members: list[str]  # 초기 멤버 목록 (채널 생성자 포함)


class AddFriendRequest(BaseModel):
    friend_id: str


# ─── 인증 헬퍼 ────────────────────────────────────────────────────────────────

def get_current_user(authorization: str = Header(...)) -> str:
    """
    Bearer 토큰에서 user_id를 추출한다.
    실제 운영에서는 JWT 서명 검증을 수행한다.
    형식: Authorization: Bearer <user_id>
    """
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization header")
    user_id = authorization[len("Bearer "):]
    user = store.get(f"user:{user_id}")
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user_id


# ─── 회원가입 / 로그인 ────────────────────────────────────────────────────────

@router.post("/register", status_code=201)
def register(req: RegisterRequest):
    """
    회원가입.
    user_id는 고유해야 하며, 중복 시 409 반환.
    """
    if store.get(f"user:{req.user_id}"):
        raise HTTPException(status_code=409, detail="user_id already exists")

    user = {
        "user_id": req.user_id,
        "username": req.username,
        "password": req.password,  # 토이 — 실제는 bcrypt hash
    }
    store.set(f"user:{req.user_id}", user)
    # 친구 목록 초기화
    store.set(f"friends:{req.user_id}", [])

    return {"message": "registered", "user_id": req.user_id}


@router.post("/login")
def login(req: LoginRequest):
    """
    로그인.
    성공 시 Bearer 토큰(user_id)을 반환한다.
    12장 §서비스 탐색: 실제 구현이라면 여기서 최적 채팅 서버 주소도 함께 반환한다.
    """
    user = store.get(f"user:{req.user_id}")
    if not user or user["password"] != req.password:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    return {
        "token": req.user_id,          # 실제 운영에서는 JWT
        "user_id": req.user_id,
        "username": user["username"],
        # 12장 §서비스 탐색: 클라이언트가 접속할 채팅 서버 주소 반환
        "chat_server_url": "ws://localhost:8000/ws",
    }


# ─── 프로파일 ─────────────────────────────────────────────────────────────────

@router.get("/profile/{user_id}")
def get_profile(user_id: str, current_user: str = Depends(get_current_user)):
    user = store.get(f"user:{user_id}")
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    presence = presence_server.get_presence(user_id)
    return {
        "user_id": user["user_id"],
        "username": user["username"],
        "status": presence.get("status", "offline"),
    }


@router.patch("/profile")
def update_profile(
    req: UpdateProfileRequest,
    current_user: str = Depends(get_current_user),
):
    user = store.get(f"user:{current_user}")
    if req.username:
        user["username"] = req.username
    store.set(f"user:{current_user}", user)
    return {"message": "updated", "user": {"user_id": current_user, "username": user["username"]}}


# ─── 친구 관리 ────────────────────────────────────────────────────────────────

@router.post("/friends")
def add_friend(req: AddFriendRequest, current_user: str = Depends(get_current_user)):
    """
    친구 추가 (양방향).
    12장 §상태 정보의 전송: 친구 목록이 presence 이벤트 수신자 범위를 결정한다.
    """
    if not store.get(f"user:{req.friend_id}"):
        raise HTTPException(status_code=404, detail="Friend not found")

    # A → B
    friends_a: list = store.get(f"friends:{current_user}", [])
    if req.friend_id not in friends_a:
        friends_a.append(req.friend_id)
    store.set(f"friends:{current_user}", friends_a)

    # B → A (양방향)
    friends_b: list = store.get(f"friends:{req.friend_id}", [])
    if current_user not in friends_b:
        friends_b.append(current_user)
    store.set(f"friends:{req.friend_id}", friends_b)

    return {"message": "friend added"}


@router.get("/friends")
def list_friends(current_user: str = Depends(get_current_user)):
    friends: list = store.get(f"friends:{current_user}", [])
    result = []
    for fid in friends:
        u = store.get(f"user:{fid}")
        if u:
            presence = presence_server.get_presence(fid)
            result.append({
                "user_id": fid,
                "username": u["username"],
                "status": presence.get("status", "offline"),
            })
    return {"friends": result}


# ─── 그룹 채널 관리 ───────────────────────────────────────────────────────────

@router.post("/channels", status_code=201)
def create_channel(
    req: CreateChannelRequest,
    current_user: str = Depends(get_current_user),
):
    """
    그룹 채팅 채널 생성.
    채널 인원 제한: 최대 100명 (12장 요구사항).
    """
    if store.get(f"channel:{req.channel_id}"):
        raise HTTPException(status_code=409, detail="Channel already exists")

    # 생성자를 멤버에 포함
    members = list(set(req.members + [current_user]))
    if len(members) > 100:
        raise HTTPException(status_code=400, detail="Group chat limit is 100 members")

    channel = {
        "channel_id": req.channel_id,
        "name": req.name,
        "creator": current_user,
    }
    store.set(f"channel:{req.channel_id}", channel)
    store.set(f"channel_members:{req.channel_id}", members)

    return {"message": "channel created", "channel_id": req.channel_id, "members": members}


@router.get("/channels/{channel_id}")
def get_channel(channel_id: str, current_user: str = Depends(get_current_user)):
    channel = store.get(f"channel:{channel_id}")
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    members: list = store.get(f"channel_members:{channel_id}", [])
    if current_user not in members:
        raise HTTPException(status_code=403, detail="Not a channel member")
    return {**channel, "members": members, "member_count": len(members)}


@router.get("/channels/{channel_id}/messages")
def get_channel_messages(
    channel_id: str,
    after_id: Optional[int] = None,
    limit: int = 50,
    current_user: str = Depends(get_current_user),
):
    """채팅 이력 조회 (키-값 저장소에서 최근 메시지 반환)"""
    members: list = store.get(f"channel_members:{channel_id}", [])
    if current_user not in members:
        raise HTTPException(status_code=403, detail="Not a channel member")

    channel_key = f"group:{channel_id}"
    messages = store.get_messages(channel_key, after_id=after_id, limit=limit)
    return {"messages": messages, "count": len(messages)}


@router.get("/dm/{other_user_id}/messages")
def get_dm_messages(
    other_user_id: str,
    after_id: Optional[int] = None,
    limit: int = 50,
    current_user: str = Depends(get_current_user),
):
    """1:1 채팅 이력 조회"""
    # DM 채널 키는 두 user_id를 정렬하여 생성 (chat_server.py와 동일 로직)
    channel_key = "dm:" + ":".join(sorted([current_user, other_user_id]))
    messages = store.get_messages(channel_key, after_id=after_id, limit=limit)
    return {"messages": messages, "count": len(messages)}
