#!/usr/bin/env python3
"""
多Bot群聊Web — 中继服务器
WebSocket + HTTP + SQLite + Bot Worker 管理
"""

import asyncio
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import websockets
from websockets.asyncio.server import serve, ServerConnection

# ── 路径 ────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
STATIC_DIR = BASE_DIR / "static"
WORKERS_DIR = BASE_DIR / "workers"
DB_PATH = DATA_DIR / "chatroom.db"
WORKER_SCRIPT = WORKERS_DIR / "worker.py"
CONFIG_PATH = WORKERS_DIR / "config.json"

# ── 配置 ────────────────────────────────────────
HOST = "0.0.0.0"
WS_PORT = 9091
HTTP_PORT = 9092  # HTTP 服务端口（提供聊天页面 + API）
AVAILABLE_BOTS = {
    "小帅": {"profile": "writer", "name": "小帅", "avatar": "🎭"},
    "YY":   {"profile": "editor", "name": "YY",   "avatar": "🌟"},
    "读者": {"profile": "reader", "name": "读者", "avatar": "👤"},
}

# ── 数据库 ───────────────────────────────────────
def init_db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rooms (
            id   TEXT PRIMARY KEY,
            name TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS room_members (
            room_id  TEXT NOT NULL,
            username TEXT NOT NULL,
            role     TEXT NOT NULL DEFAULT 'user',  -- user / bot
            joined_at REAL NOT NULL,
            PRIMARY KEY (room_id, username)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id  TEXT NOT NULL,
            username TEXT NOT NULL,
            content  TEXT NOT NULL,
            msg_type TEXT NOT NULL DEFAULT 'message',  -- message / system
            created_at REAL NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_room ON messages(room_id, id)")
    # 确保默认房间存在
    conn.execute("INSERT OR IGNORE INTO rooms (id, name) VALUES ('main', '主房间')")
    conn.commit()
    conn.close()

def db_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def save_message(room_id, username, content, msg_type="message"):
    conn = db_conn()
    now = time.time()
    conn.execute(
        "INSERT INTO messages (room_id, username, content, msg_type, created_at) VALUES (?, ?, ?, ?, ?)",
        (room_id, username, content, msg_type, now)
    )
    msg_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()
    return msg_id, now

def get_history(room_id, limit=50):
    conn = db_conn()
    rows = conn.execute(
        "SELECT id, username, content, msg_type, created_at FROM messages "
        "WHERE room_id=? ORDER BY id DESC LIMIT ?",
        (room_id, limit)
    ).fetchall()
    conn.close()
    messages = [{
        "id": r["id"],
        "username": r["username"],
        "content": r["content"],
        "msg_type": r["msg_type"],
        "time": datetime.fromtimestamp(r["created_at"]).strftime("%H:%M:%S")
    } for r in reversed(rows)]  # 反转回正序
    return messages

def get_members(room_id):
    conn = db_conn()
    rows = conn.execute(
        "SELECT username, role FROM room_members WHERE room_id=?", (room_id,)
    ).fetchall()
    conn.close()
    return [{"username": r["username"], "role": r["role"]} for r in rows]

def add_member(room_id, username, role="user"):
    conn = db_conn()
    conn.execute(
        "INSERT OR IGNORE INTO room_members (room_id, username, role, joined_at) VALUES (?, ?, ?, ?)",
        (room_id, username, role, time.time())
    )
    conn.commit()
    conn.close()

def remove_member(room_id, username):
    conn = db_conn()
    conn.execute(
        "DELETE FROM room_members WHERE room_id=? AND username=?",
        (room_id, username)
    )
    conn.commit()
    conn.close()

# ── Bot Worker 管理 ──────────────────────────────
PROCESSES: dict[str, subprocess.Popen] = {}  # bot_name → Popen

def spawn_bot_worker(bot_name):
    """启动一个 Bot Worker 子进程"""
    if bot_name in PROCESSES and PROCESSES[bot_name].poll() is None:
        print(f"[relay] Bot {bot_name} 已在运行")
        return True
    
    bot_info = AVAILABLE_BOTS.get(bot_name)
    if not bot_info:
        print(f"[relay] 未知 bot: {bot_name}")
        return False
    
    env = os.environ.copy()
    env["BOT_NAME"] = bot_name
    env["BOT_PROFILE"] = bot_info["profile"]
    env["RELAY_WS_URL"] = f"ws://127.0.0.1:{WS_PORT}/ws"
    env["ROOM_ID"] = "main"
    
    venv_python = str(BASE_DIR.parent / "chatroom-venv/bin/python3")
    if not os.path.exists(venv_python):
        venv_python = sys.executable
    
    # 优先使用 Hermes venv 的 Python（兼容 oneshot 的 C 扩展）
    hermes_python = "/root/hermes-agent/hermes-agent-2026.5.16/venv/bin/python3"
    if os.path.exists(hermes_python):
        venv_python = hermes_python
    
    try:
        proc = subprocess.Popen(
            [venv_python, str(WORKER_SCRIPT)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        PROCESSES[bot_name] = proc
        print(f"[relay] 🚀 Bot Worker '{bot_name}' 已启动 (PID {proc.pid})")
        return True
    except Exception as e:
        print(f"[relay] ❌ 启动 {bot_name} 失败: {e}")
        return False

def kill_bot_worker(bot_name):
    """杀掉 Bot Worker 子进程"""
    if bot_name not in PROCESSES:
        print(f"[relay] Bot {bot_name} 没有运行中的进程")
        return True
    
    proc = PROCESSES[bot_name]
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    del PROCESSES[bot_name]
    print(f"[relay] 🛑 Bot Worker '{bot_name}' 已停止")
    return True

def kill_all_workers():
    for name in list(PROCESSES.keys()):
        kill_bot_worker(name)

# ── WebSocket 房间管理 ──────────────────────────
class Room:
    """聊天房间"""
    def __init__(self, room_id):
        self.room_id = room_id
        self.clients: dict[str, set[ServerConnection]] = {}  # username → {ws connections}
    
    def add_client(self, username, ws):
        if username not in self.clients:
            self.clients[username] = set()
        self.clients[username].add(ws)
    
    def remove_client(self, username, ws):
        if username in self.clients:
            self.clients[username].discard(ws)
            if not self.clients[username]:
                del self.clients[username]
    
    def broadcast(self, data, exclude_ws=None):
        """广播消息给房间所有客户端"""
        message = json.dumps(data, ensure_ascii=False)
        for username, ws_set in list(self.clients.items()):
            for ws in list(ws_set):
                if ws is exclude_ws:
                    continue
                try:
                    asyncio.ensure_future(ws.send(message))
                except:
                    pass

rooms: dict[str, Room] = {}

def get_room(room_id):
    if room_id not in rooms:
        rooms[room_id] = Room(room_id)
    return rooms[room_id]

# ── HTTP 服务（提供聊天页面 + API）───────────────
async def handle_http(reader, writer):
    """简单的 HTTP 服务"""
    try:
        request_data = await asyncio.wait_for(reader.read(65536), timeout=10)
    except asyncio.TimeoutError:
        writer.close()
        return
    
    if not request_data:
        writer.close()
        return
    
    request_text = request_data.decode("utf-8", errors="replace")
    lines = request_text.split("\r\n")
    if not lines:
        writer.close()
        return
    
    first_line = lines[0]
    method = first_line.split(" ")[0] if " " in first_line else "GET"
    path = first_line.split(" ")[1] if first_line.count(" ") >= 1 else "/"
    
    def json_response(data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        header = f"HTTP/1.1 {status} OK\r\nContent-Type: application/json; charset=utf-8\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {len(body)}\r\n\r\n"
        return header.encode("utf-8") + body
    
    # ── API ──
    if path == "/api/bots":
        writer.write(json_response(list(AVAILABLE_BOTS.keys())))
        await writer.drain()
        writer.close()
        return
    
    if path == "/api/members":
        writer.write(json_response(get_members("main")))
        await writer.drain()
        writer.close()
        return
    
    if path == "/api/history":
        writer.write(json_response(get_history("main")))
        await writer.drain()
        writer.close()
        return
    
    if path.startswith("/api/add_bot"):
        bot_name = path.split("=")[-1] if "=" in path else ""
        if bot_name in AVAILABLE_BOTS:
            add_member("main", bot_name, "bot")
            spawn_bot_worker(bot_name)
            # 🔔 唤醒对应的 Feishu gateway
            bot_profile = AVAILABLE_BOTS[bot_name].get("profile", "")
            if bot_profile:
                proc = await asyncio.create_subprocess_exec(
                    "bash", "/novel/scripts/gateway-wake.sh", bot_profile,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL
                )
                # 不 await，让唤醒在后台进行
            room = get_room("main")
            room.broadcast({
                "type": "system",
                "content": f"{AVAILABLE_BOTS[bot_name]['avatar']} {bot_name} 加入了群聊"
            })
            room.broadcast({"type": "members_updated"})
            writer.write(json_response({"ok": True}))
        else:
            writer.write(json_response({"ok": False, "error": "未知 bot"}))
        await writer.drain()
        writer.close()
        return
    
    if path.startswith("/api/kick_bot"):
        bot_name = path.split("=")[-1] if "=" in path else ""
        if bot_name in AVAILABLE_BOTS:
            remove_member("main", bot_name)
            kill_bot_worker(bot_name)
            room = get_room("main")
            room.broadcast({
                "type": "system",
                "content": f"{bot_name} 被移出了群聊"
            })
            room.broadcast({"type": "members_updated"})
            writer.write(json_response({"ok": True}))
        else:
            writer.write(json_response({"ok": False}))
        await writer.drain()
        writer.close()
        return
    
    # ── 静态文件 ──
    if path == "/" or path == "/index.html":
        file_path = STATIC_DIR / "chat.html"
    else:
        # 去掉前导 /
        file_path = STATIC_DIR / path.lstrip("/")
        # 安全检查：不允许跳出 static 目录
        try:
            file_path = file_path.resolve()
            if not str(file_path).startswith(str(STATIC_DIR.resolve())):
                writer.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
                writer.close()
                return
        except:
            writer.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            writer.close()
            return
    
    if file_path.exists() and file_path.is_file():
        content = file_path.read_bytes()
        ext = file_path.suffix
        mime = {
            ".html": "text/html; charset=utf-8",
            ".css":  "text/css; charset=utf-8",
            ".js":   "application/javascript; charset=utf-8",
            ".png":  "image/png",
            ".svg":  "image/svg+xml",
            ".ico":  "image/x-icon",
        }.get(ext, "application/octet-stream")
        header = f"HTTP/1.1 200 OK\r\nContent-Type: {mime}\r\nContent-Length: {len(content)}\r\n\r\n"
        response = header.encode("utf-8") + content
        writer.write(response)
    else:
        writer.write(b"HTTP/1.1 404 Not Found\r\n\r\nPage Not Found")
    
    await writer.drain()
    writer.close()

# ── WebSocket 处理 ───────────────────────────────
async def handle_ws(ws: ServerConnection):
    """处理 WebSocket 连接"""
    username = None
    room_id = "main"
    room = get_room(room_id)
    
    try:
        async for raw in ws:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            
            msg_type = data.get("type", "")
            
            if msg_type == "join":
                username = data.get("username", "匿名用户")
                role = data.get("role", "user")
                room.add_client(username, ws)
                add_member(room_id, username, role)
                
                # 发送历史消息
                history = get_history(room_id)
                await ws.send(json.dumps({
                    "type": "history",
                    "messages": history
                }, ensure_ascii=False))
                
                # 发送当前成员列表
                members = get_members(room_id)
                await ws.send(json.dumps({
                    "type": "members",
                    "members": members
                }, ensure_ascii=False))
                
                # 非 bot 加入时广播系统消息
                if role != "bot":
                    room.broadcast({
                        "type": "system",
                        "content": f"👤 {username} 加入了群聊"
                    }, exclude_ws=ws)
                    room.broadcast({"type": "members_updated"})
                
                print(f"[relay] {role} '{username}' 加入房间 {room_id}")
            
            elif msg_type == "message":
                if not username:
                    continue
                content = data.get("content", "").strip()
                if not content:
                    continue
                
                msg_id, timestamp = save_message(room_id, username, content)
                time_str = datetime.fromtimestamp(timestamp).strftime("%H:%M:%S")
                
                # 广播消息给房间所有人
                room.broadcast({
                    "type": "message",
                    "id": msg_id,
                    "username": username,
                    "content": content,
                    "time": time_str
                })
                
                print(f"[relay] [{time_str}] {username}: {content[:60]}")
            
            elif msg_type == "ping":
                await ws.send(json.dumps({"type": "pong"}))
    
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if username and room_id:
            room.remove_client(username, ws)
            if username not in [m["username"] for m in get_members(room_id) if m["role"] == "bot"]:
                # 人类用户断开
                room.broadcast({
                    "type": "system",
                    "content": f"👤 {username} 离开了群聊"
                })
                room.broadcast({"type": "members_updated"})
            print(f"[relay] '{username}' 断开连接")

# ── 主入口 ────────────────────────────────────────
async def main():
    init_db()
    
    # 启动 HTTP 服务
    http_server = await asyncio.start_server(handle_http, HOST, HTTP_PORT)
    print(f"[relay] 🌐 HTTP 服务: http://{HOST}:{HTTP_PORT}/")
    
    # 启动 WebSocket 服务
    async with serve(handle_ws, HOST, WS_PORT) as ws_server:
        print(f"[relay] 🔌 WebSocket 服务: ws://{HOST}:{WS_PORT}/ws")
        print(f"[relay] 🚀 中继服务器已启动")
        print(f"[relay] 📍 聊天页面: http://192.168.1.111:{HTTP_PORT}/")
        print(f"[relay] 📡 WebSocket 端口: {WS_PORT}")
        print()
        
        # 处理 SIGTERM/SIGINT
        stop = asyncio.get_running_loop().run_in_executor
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                asyncio.get_running_loop().add_signal_handler(
                    sig, lambda: asyncio.ensure_future(shutdown(ws_server, http_server))
                )
            except NotImplementedError:
                pass
        
        await ws_server.serve_forever()

async def shutdown(ws_server, http_server):
    print("\n[relay] 🛑 正在关闭中继服务器...")
    kill_all_workers()
    ws_server.close()
    http_server.close()
    print("[relay] ✅ 已关闭")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        kill_all_workers()
        print("\n[relay] 👋 再见")
