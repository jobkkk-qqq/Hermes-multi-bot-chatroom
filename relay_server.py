#!/usr/bin/env python3
"""
多Bot群聊Web — 中继服务器
WebSocket + HTTP + SQLite + Bot Worker 管理
"""

import asyncio
import json
import os
import re
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

import yaml
import websockets
from websockets.asyncio.server import serve, ServerConnection
from websockets.http11 import Response, Headers

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
PORT = 9092  # 单一端口，同时提供 HTTP 页面 + API + WebSocket
HERMES_BASE = Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes")))

# ── 工具函数（需在 detect_profiles 之前定义）─────
def load_bot_name(profile):
    """从 profile 配置读取 bot.display_name"""
    if profile == "default":
        config_path = HERMES_BASE / "config.yaml"
    else:
        config_path = HERMES_BASE / "profiles" / profile / "config.yaml"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        return cfg.get("bot", {}).get("display_name", profile)
    except Exception:
        return profile

def check_gateway_status(profile):
    """检查 Hermes gateway 运行状态"""
    svc = "hermes-gateway" if profile == "default" else f"hermes-gateway-{profile}"
    try:
        r = subprocess.run(
            ["systemctl", "show", "-p", "ActiveState", svc],
            capture_output=True, text=True, timeout=5
        )
        state = r.stdout.strip().split("=")[-1] if "=" in r.stdout else "inactive"
        return state if state else "inactive"
    except Exception:
        return "unknown"

# ── Profile 自动发现 ──────────────────────────────
PROFILE_AVATARS = ["🐏", "🦀", "🎭", "🌟", "👤", "🤖", "🦊", "🐱", "🐶", "🐰",
                    "🐼", "🐨", "🦁", "🐯", "🦄", "🐲", "🐧", "🦉", "🐺", "🐸"]

def detect_profiles():
    """自动检测 Hermes profile，生成 AVAILABLE_BOTS"""
    bots = {}
    # 使用绝对路径，兼容受限 PATH 环境
    hermes_cmd = os.environ.get("HERMES_CLI", "/root/.local/bin/hermes")
    try:
        r = subprocess.run([hermes_cmd, "profile", "list"],
                           capture_output=True, text=True, timeout=10)
        lines = r.stdout.strip().split("\n")
        profiles = []
        for line in lines:
            line = line.strip()
            if not line or line.startswith("─") or "Profile" in line:
                continue
            # 去掉 ◆ 标记，取第一列 profile 名
            name = line.replace("◆", "").strip().split()[0]
            if name:
                profiles.append(name)
        
        for i, name in enumerate(profiles):
            display = load_bot_name(name)
            avatar = PROFILE_AVATARS[i % len(PROFILE_AVATARS)]
            bots[name] = {"profile": name, "avatar": avatar, "name": display}
        
        if bots:
            print(f"[relay] 🔍 自动发现 {len(bots)} 个 profile: {', '.join(bots.keys())}")
            return bots
    except FileNotFoundError:
        print("[relay] ⚠️  hermes 命令未找到，使用默认 bot 列表")
    except Exception as e:
        print(f"[relay] ⚠️  profile 检测失败: {e}")
    
    # 降级：空列表，用户可手动配置
    print("[relay] ℹ️  没有可用 profile，将使用空 bot 列表")
    return {}

AVAILABLE_BOTS = detect_profiles()

def ensure_config_json():
    """动态生成 worker config.json"""
    config = {"bots": {}}
    for key, info in AVAILABLE_BOTS.items():
        profile = info["profile"]
        if profile == "default":
            hermes_home = str(HERMES_BASE)
        else:
            hermes_home = str(HERMES_BASE / "profiles" / profile)
        config["bots"][key] = {
            "profile": profile,
            "hermes_home": hermes_home,
            "description": f"Hermes Agent ({profile})"
        }
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    print(f"[relay] 📝 已生成 {CONFIG_PATH} ({len(config['bots'])} bots)")

# 启动时生成 config.json
ensure_config_json()

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

# ── MEDIA 图片处理 ─────────────────────────────
UPLOAD_DIR = STATIC_DIR / "uploads"

def process_media_tags(content):
    """扫描消息中的 MEDIA:/path 标签，将图片复制到静态目录，替换为 markdown 图片链接"""
    if "MEDIA:" not in content:
        return content

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    def _replace_media(m):
        path = m.group(1)
        if not os.path.isfile(path):
            print(f"[relay] ⚠️ MEDIA 文件不存在: {path}")
            return f"[图片丢失: {os.path.basename(path)}]"

        ext = os.path.splitext(path)[1].lower()
        if ext not in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".svg"):
            ext = ".png"

        saved_name = f"{uuid.uuid4().hex}{ext}"
        saved_path = UPLOAD_DIR / saved_name
        try:
            shutil.copy2(path, saved_path)
            print(f"[relay] 🖼️ MEDIA 图片已复制: {path} → uploads/{saved_name}")
            return f"![{os.path.basename(path)}](/uploads/{saved_name})"
        except Exception as e:
            print(f"[relay] ❌ MEDIA 图片复制失败: {path}: {e}")
            return f"[图片加载失败: {os.path.basename(path)}]"

    # 只替换不在代码块内的 MEDIA:path（排除 ```...``` 和 `...` 内的匹配）
    result = []
    in_code_block = False
    in_inline_code = False
    i = 0
    while i < len(content):
        # 检测代码块开始/结束
        if content[i:i+3] == "```":
            in_code_block = not in_code_block
            result.append("```")
            i += 3
            continue
        # 检测行内代码
        if content[i] == '`' and not in_code_block:
            in_inline_code = not in_inline_code
            result.append('`')
            i += 1
            continue
        # 不在代码中才匹配 MEDIA:
        if not in_code_block and not in_inline_code and content[i:i+6] == "MEDIA:":
            # 找到 MEDIA: 后面的路径（直到空格或行尾）
            j = i + 6
            path_start = j
            while j < len(content) and content[j] not in (' ', '\t', '\n', '\r'):
                j += 1
            path = content[path_start:j]
            # 用 regex 替换
            matched = f"MEDIA:{path}"
            replaced = _replace_media(type('', (), {'group': lambda self, n: path})())
            result.append(replaced)
            i = j
        else:
            result.append(content[i])
            i += 1

    return "".join(result)

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
        "content": process_media_tags(r["content"]),  # 处理旧消息中的 MEDIA 标签
        "msg_type": r["msg_type"],
        "time": datetime.fromtimestamp(r["created_at"]).strftime("%H:%M:%S")
    } for r in reversed(rows)]
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
    env["BOT_DISPLAY_NAME"] = bot_info.get("name", bot_name)
    env["BOT_PROFILE"] = bot_info["profile"]
    env["RELAY_WS_URL"] = f"ws://127.0.0.1:{PORT}/ws"
    env["ROOM_ID"] = "main"
    
    venv_python = str(BASE_DIR.parent / "chatroom-venv/bin/python3")
    if not os.path.exists(venv_python):
        venv_python = sys.executable
    
    # 如果 HERMES_VENV_PYTHON 环境变量设置了，用那个（兼容不同的 Hermes 安装路径）
    hermes_python = os.environ.get("HERMES_VENV_PYTHON", "")
    if hermes_python and os.path.exists(hermes_python):
        venv_python = hermes_python
    # 尝试常见 Hermes 安装路径
    elif not hermes_python:
        for candidate in [
            "/root/hermes-agent/hermes-agent-2026.5.16/venv/bin/python3",
            os.path.expanduser("~/.hermes/hermes-agent/venv/bin/python3"),
        ]:
            if os.path.exists(candidate):
                venv_python = candidate
                break
    
    try:
        proc = subprocess.Popen(
            [venv_python, str(WORKER_SCRIPT)],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
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
    
    async def _safe_send(self, ws, message):
        """安全发送单条消息，处理断开连接等异常"""
        try:
            await ws.send(message)
        except websockets.exceptions.ConnectionClosed:
            pass  # 客户端断开，正常
        except Exception as e:
            print(f"[relay] ⚠️ 发送失败: {e}")

    async def broadcast(self, data, exclude_ws=None):
        """广播消息给房间所有客户端，并行发送并追踪所有任务"""
        message = json.dumps(data, ensure_ascii=False)
        tasks = []
        for username, ws_set in list(self.clients.items()):
            for ws in list(ws_set):
                if ws is exclude_ws:
                    continue
                tasks.append(asyncio.create_task(self._safe_send(ws, message)))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

rooms: dict[str, Room] = {}

def get_room(room_id):
    if room_id not in rooms:
        rooms[room_id] = Room(room_id)
    return rooms[room_id]

def get_online_members(room_id):
    """获取完整成员列表：数据库中的 bot + 当前在线的非 bot 用户"""
    members = get_members(room_id)  # 数据库中的 bot
    room = rooms.get(room_id)
    if room:
        for uname in room.clients:
            exists = any(m["username"] == uname for m in members)
            if not exists:
                members.append({"username": uname, "role": "user"})
    return members

# ── HTTP 处理（通过 WebSocket 服务的 process_request 回调）─
async def process_request(connection, request):
    """处理非 WebSocket 的 HTTP 请求（API + 静态文件）"""
    path = request.path

    # WebSocket 升级请求放行
    if path == "/ws":
        return None

    def json_resp(data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        return Response(status, "OK", Headers({
            "Content-Type": "application/json; charset=utf-8",
            "Access-Control-Allow-Origin": "*",
            "Content-Length": str(len(body)),
        }), body)

    # ── API ──
    if path == "/api/bots":
        return json_resp([
            {
                "key": k, "name": v.get("name", k), "avatar": v.get("avatar", "🤖"),
                "gateway": check_gateway_status(v["profile"])
            }
            for k, v in AVAILABLE_BOTS.items()
        ])

    if path == "/api/members":
        return json_resp(get_online_members("main"))

    if path == "/api/history":
        return json_resp(get_history("main"))

    if path.startswith("/api/add_bot"):
        bot_name = path.split("=")[-1] if "=" in path else ""
        if bot_name in AVAILABLE_BOTS:
            add_member("main", bot_name, "bot")
            spawn_bot_worker(bot_name)
            # 🔔 唤醒对应的 Feishu gateway（后台执行，不等待）
            bot_profile = AVAILABLE_BOTS[bot_name].get("profile", "")
            wake_script = "/novel/scripts/gateway-wake.sh"
            if bot_profile and os.path.exists(wake_script):
                asyncio.create_task(
                    asyncio.create_subprocess_exec(
                        "bash", wake_script, bot_profile,
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL
                    )
                )
            room = get_room("main")
            await room.broadcast({
                "type": "system",
                "content": f"{AVAILABLE_BOTS[bot_name]['avatar']} {bot_name} 加入了群聊"
            })
            await room.broadcast({"type": "members_updated"})
            return json_resp({"ok": True})
        return json_resp({"ok": False, "error": "未知 bot"})

    if path.startswith("/api/kick_bot"):
        bot_name = path.split("=")[-1] if "=" in path else ""
        if bot_name in AVAILABLE_BOTS:
            remove_member("main", bot_name)
            kill_bot_worker(bot_name)
            room = get_room("main")
            await room.broadcast({
                "type": "system",
                "content": f"{bot_name} 被移出了群聊"
            })
            await room.broadcast({"type": "members_updated"})
            return json_resp({"ok": True})
        return json_resp({"ok": False})

    # ── 静态文件 ──
    if path == "/" or path == "/index.html":
        file_path = STATIC_DIR / "chat.html"
    else:
        file_path = STATIC_DIR / path.lstrip("/")
        try:
            file_path = file_path.resolve()
            if not str(file_path).startswith(str(STATIC_DIR.resolve())):
                return Response(403, "Forbidden", Headers({"Content-Length": "0"}), b"")
        except:
            return Response(403, "Forbidden", Headers({"Content-Length": "0"}), b"")

    if file_path.exists() and file_path.is_file():
        content = file_path.read_bytes()
        ext = file_path.suffix
        mime = {
            ".html": "text/html; charset=utf-8",
            ".css":  "text/css; charset=utf-8",
            ".js":   "application/javascript; charset=utf-8",
            ".png":  "image/png",
            ".jpg":  "image/jpeg",
            ".jpeg": "image/jpeg",
            ".gif":  "image/gif",
            ".webp": "image/webp",
            ".bmp":  "image/bmp",
            ".svg":  "image/svg+xml",
            ".ico":  "image/x-icon",
        }.get(ext, "application/octet-stream")
        return Response(200, "OK", Headers({
            "Content-Type": mime,
            "Content-Length": str(len(content)),
        }), content)

    return Response(404, "Not Found", Headers({"Content-Length": "0"}), b"")

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
                # 只有 bot 成员持久化到数据库，普通用户仅在内存中跟踪
                if role == "bot":
                    add_member(room_id, username, role)
                
                # 发送历史消息
                history = get_history(room_id)
                await ws.send(json.dumps({
                    "type": "history",
                    "messages": history
                }, ensure_ascii=False))
                
                # 发送当前成员列表（数据库中的 bot + 内存中的在线用户）
                members = get_online_members(room_id)
                await ws.send(json.dumps({
                    "type": "members",
                    "members": members
                }, ensure_ascii=False))
                
                # 非 bot 加入时广播系统消息
                if role != "bot":
                    await room.broadcast({
                        "type": "system",
                        "content": f"👤 {username} 加入了群聊"
                    }, exclude_ws=ws)
                    await room.broadcast({"type": "members_updated"})
                
                print(f"[relay] {role} '{username}' 加入房间 {room_id}")
            
            elif msg_type == "message":
                if not username:
                    continue
                content = data.get("content", "").strip()
                if not content:
                    continue
                
                # 处理 MEDIA: 图片标签（bot 发图用）
                content = process_media_tags(content)
                
                msg_id, timestamp = save_message(room_id, username, content)
                time_str = datetime.fromtimestamp(timestamp).strftime("%H:%M:%S")
                
                # 广播消息给房间所有人
                await room.broadcast({
                    "type": "message",
                    "id": msg_id,
                    "username": username,
                    "content": content,
                    "time": time_str
                })
                
                print(f"[relay] [{time_str}] {username}: {content[:60]}")
            
            elif msg_type == "image_upload":
                if not username:
                    continue
                filename = data.get("filename", "image.png")
                b64_data = data.get("data", "")
                if not b64_data:
                    continue
                import base64, uuid
                try:
                    img_bytes = base64.b64decode(b64_data)
                except Exception:
                    continue
                ext = os.path.splitext(filename)[1].lower()
                if ext not in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".svg"):
                    ext = ".png"
                upload_dir = STATIC_DIR / "uploads"
                upload_dir.mkdir(parents=True, exist_ok=True)
                saved_name = f"{uuid.uuid4().hex}{ext}"
                saved_path = upload_dir / saved_name
                saved_path.write_bytes(img_bytes)
                img_url = f"/uploads/{saved_name}"
                content = f"![{filename}]({img_url})"
                msg_id, timestamp = save_message(room_id, username, content)
                time_str = datetime.fromtimestamp(timestamp).strftime("%H:%M:%S")
                await room.broadcast({
                    "type": "message",
                    "id": msg_id,
                    "username": username,
                    "content": content,
                    "time": time_str
                })
                print(f"[relay] [{time_str}] {username} 上传了图片: {saved_name}")

            elif msg_type == "ping":
                await ws.send(json.dumps({"type": "pong"}))
            
            elif msg_type in ("thinking", "thinking_end"):
                # 广播 bot 思考状态给房间所有人
                await room.broadcast(data)
    
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if username and room_id:
            room.remove_client(username, ws)
            if username not in [m["username"] for m in get_members(room_id) if m["role"] == "bot"]:
                # 人类用户断开
                await room.broadcast({
                    "type": "system",
                    "content": f"👤 {username} 离开了群聊"
                })
                await room.broadcast({"type": "members_updated"})
            print(f"[relay] '{username}' 断开连接")

# ── 工具 ───────────────────────────────────────────
def get_local_ip():
    """获取本机局域网 IP"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

# ── 外部消息注入 API（port 9093）─────────────────
EXTERNAL_API_PORT = 9093

async def handle_external_http(reader, writer):
    """处理外部 HTTP 请求（简单 POST API）"""
    try:
        raw = await asyncio.wait_for(reader.read(65536), timeout=10)
        if not raw:
            writer.close()
            return
        
        # 解析 HTTP 请求行
        text = raw.decode("utf-8", errors="replace")
        lines = text.split("\r\n")
        if not lines:
            writer.close()
            return
        
        request_line = lines[0]
        parts = request_line.split(" ")
        method = parts[0] if len(parts) > 0 else ""
        path = parts[1] if len(parts) > 1 else ""
        
        # 只处理 POST /api/external/message
        if method == "POST" and path == "/api/external/message":
            # 找空行（header/body 分隔）
            body_start = text.find("\r\n\r\n")
            if body_start == -1:
                body = ""
            else:
                body = text[body_start + 4:]
            
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                await _send_http_response(writer, 400, {"error": "invalid JSON"})
                return
            
            username = data.get("username", "").strip()
            content = data.get("content", "").strip()
            msg_time = data.get("time", "")
            
            if not username or not content:
                await _send_http_response(writer, 400, {"error": "username and content required"})
                return
            
            # 处理 MEDIA: 标签 → 复制图片到 static/uploads/
            content = process_media_tags(content)
            
            # 保存到数据库
            msg_id, timestamp = save_message("main", username, content)
            
            # 格式化时间
            time_str = msg_time or datetime.fromtimestamp(timestamp).strftime("%H:%M:%S")
            
            # 广播给所有 WebSocket 客户端
            room = get_room("main")
            await room.broadcast({
                "type": "message",
                "id": msg_id,
                "username": username,
                "content": content,
                "time": time_str
            })
            
            print(f"[relay/external] [{time_str}] {username}: {content[:60]}")
            await _send_http_response(writer, 200, {"ok": True, "id": msg_id})
        else:
            await _send_http_response(writer, 404, {"error": "not found"})
    
    except asyncio.TimeoutError:
        writer.close()
    except Exception as e:
        print(f"[relay/external] ❌ 错误: {e}")
        try:
            await _send_http_response(writer, 500, {"error": str(e)})
        except:
            writer.close()

async def _send_http_response(writer, status, data):
    """发送 HTTP JSON 响应"""
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    status_text = {200: "OK", 400: "Bad Request", 404: "Not Found", 500: "Internal Server Error"}.get(status, "Unknown")
    response = (
        f"HTTP/1.1 {status} {status_text}\r\n"
        f"Content-Type: application/json; charset=utf-8\r\n"
        f"Access-Control-Allow-Origin: *\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode("utf-8") + body
    writer.write(response)
    await writer.drain()
    writer.close()

# ── 主入口 ────────────────────────────────────────
async def main():
    init_db()
    
    # 启动外部消息 API 服务器
    ext_server = await asyncio.start_server(
        handle_external_http, "0.0.0.0", EXTERNAL_API_PORT
    )
    print(f"[relay] 📡 外部消息 API: http://0.0.0.0:{EXTERNAL_API_PORT}/api/external/message")
    
    async with serve(handle_ws, HOST, PORT, process_request=process_request) as ws_server:
        local_ip = get_local_ip()
        print(f"[relay] 🚀 中继服务器已启动")
        print(f"[relay] 📍 聊天页面: http://{local_ip}:{PORT}/")
        print(f"[relay] 🔌 WebSocket（同端口）: ws://{local_ip}:{PORT}/ws")
        print()
        
        # 自动恢复所有可用 bot
        for bot_key, bot_info in AVAILABLE_BOTS.items():
            display_name = bot_info.get("name", bot_key)
            add_member("main", display_name, "bot")
            spawn_bot_worker(bot_key)
            print(f"[relay] 🔄 自动恢复 bot: {display_name}")
        
        print()
        
        # 处理 SIGTERM/SIGINT
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                asyncio.get_running_loop().add_signal_handler(
                    sig, lambda: asyncio.ensure_future(shutdown(ws_server, ext_server))
                )
            except NotImplementedError:
                pass
        
        # 同时等待两个服务器
        await asyncio.gather(
            ws_server.serve_forever(),
            ext_server.serve_forever()
        )
async def shutdown(ws_server, ext_server=None):
    print("\n[relay] 🛑 正在关闭中继服务器...")
    kill_all_workers()
    ws_server.close()
    if ext_server:
        ext_server.close()
    print("[relay] ✅ 已关闭")

if __name__ == "__main__":
    import asyncio
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        kill_all_workers()
        print("\n[relay] 👋 再见")
    except asyncio.CancelledError:
        print("\n[relay] ⚠️ 主任务被取消")
        kill_all_workers()
