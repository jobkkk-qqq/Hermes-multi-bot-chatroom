# Multi-Bot Chatroom 🤖

A real-time web-based chatroom where multiple AI bot agents coexist. Each bot can be independently added, managed, and @mentioned — only mentioned bots consume tokens; silent bots stay quiet.

Built with Python `asyncio` + WebSocket + SQLite. Zero external framework dependencies for the server.

## Features

- **Multi-bot architecture** — Add/remove bot agents dynamically via the sidebar
- **@mention routing** — Only @mentioned bots respond; silent bots cost nothing
- **Bot 发送目标选择器** — 点击「发送」按钮弹出模态框，列出所有 bot，上下左右键导航选择，Enter 确认发送（支持「全部群聊」一键 @ 所有 agent）
- **Real-time WebSocket** — All messages broadcast instantly to connected clients
- **Persistent history** — SQLite-backed message and membership storage
- **Bot worker isolation** — Each bot runs as a separate subprocess with its own Hermes AI profile
- **Auto-restore** — Bots automatically reconnect on server restart
- **@mention autocomplete** — Type `@` to see and filter available bots
- **图片上传** — 点击 🖼 按钮选择图片，base64 上传到服务端，自动渲染为 Markdown 图片
- **文章预览卡片** — 📖 标记触发嵌入 iframe 的文章预览，支持展开/收起
- **真实心跳状态** — 显示 bot 实际处理时长（前 10 秒）+ 进程 RSS 内存占用（超过 10 秒时），卡死时秒数继续涨而 RSS 不变化 → 肉眼可见
- **思考状态 60s 超时兜底** — 防止丢失 thinking_end 信号导致状态永久显示
- **External message API** — POST port 9093 注入外部消息到群聊
- **Responsive design** — Mobile-friendly with sliding sidebar
- **Single port** — HTTP (chat UI + REST API) and WebSocket share one port

<img width="1512" height="921" alt="Image" src="https://github.com/user-attachments/assets/0ed0e49b-01cd-4fa2-a315-2982c14b409f" />

## Quick Start

### Prerequisites

- Python 3.11+
- `websockets` (Python package)
- `pyyaml` (Python package)

### Install

```bash
pip install websockets pyyaml

# Or use a virtual environment
python3 -m venv venv
source venv/bin/activate
pip install websockets pyyaml
```

### Run

```bash
python3 relay_server.py
```

```
[relay] 🚀 中继服务器已启动
[relay] 📍 聊天页面: http://192.168.1.xxx:9092/
[relay] 🔌 WebSocket（同端口）: ws://192.168.1.xxx:9092/ws
```

Open `http://<your-ip>:9092/` in a browser.

### Bot Workers (AI Integration)

The bots connect to [Hermes AI Agent](https://github.com/NousResearch/hermes-agent) for intelligent responses. Each bot worker runs as a subprocess with its own profile.

Configure bots in `workers/config.json`:

```json
{
  "bots": {
    "writer": {
      "profile": "writer",
      "hermes_home": "/path/to/hermes/profile",
      "description": "Story writing specialist"
    }
  }
}
```

Without Hermes, bots run in simulation mode (acknowledge @mentions without AI replies).

## Architecture

```
Browser ──WebSocket──▶ relay_server.py ──subprocess──▶ worker.py ──▶ Hermes AI
           │              │                    │
           └──HTTP/API────┘                    └──config.json
                      │
                   SQLite
              (messages, members)
```

| Component | Description |
|-----------|-------------|
| `relay_server.py` | Central relay: WebSocket + HTTP API + SQLite + worker management |
| `workers/worker.py` | Bot worker process: listens for @mentions, calls AI, replies |
| `static/chat.html` | Frontend: chat UI with @mention autocomplete, member list, bot management, bot selector, image upload |
| `workers/config.json` | Bot worker configurations (profile, Hermes home path) |

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Chat web UI |
| `/api/bots` | GET | List available bots (key, name, avatar, gateway status) |
| `/api/members` | GET | List current room members |
| `/api/history` | GET | Recent message history (with MEDIA image processing) |
| `/api/add_bot?name=<key>` | GET | Add a bot to the room |
| `/api/kick_bot?name=<key>` | GET | Remove a bot from the room |
| `/api/external/message` | POST | Inject external message into chatroom (port 9093) |

### WebSocket Protocol

| Message Type | Direction | Description |
|-------------|-----------|-------------|
| `join` | Client → Server | Join a room with username |
| `message` | Bidirectional | Chat message |
| `image_upload` | Client → Server | Upload image (base64 payload) |
| `thinking` | Server → Client | Bot processing status (real elapsed time + RSS) |
| `thinking_end` | Server → Client | Bot finished processing |
| `ping` / `pong` | Bidirectional | Keep-alive |
| `history` | Server → Client | Recent message history (on join) |
| `members` | Server → Client | Current member list (on join) |
| `system` | Server → Client | System notification (join/leave) |
| `members_updated` | Server → Client | Signal to refresh member list |

## Configuration

Edit the top of `relay_server.py`:

```python
HOST = "0.0.0.0"
PORT = 9092  # HTTP + WebSocket on same port

AVAILABLE_BOTS = {
    "writer": {"profile": "writer", "avatar": "🎭"},
    "editor": {"profile": "editor", "avatar": "🌟"},
}
```

## 关键特性详解

### Bot 发送目标选择器

点击「发送」按钮不再直接发送消息，而是弹出 bot 选择模态框：

- **上下左右键** — 在选项间导航（循环）
- **Enter** — 发送给当前选中的 bot
- **Escape** — 取消发送
- **全部群聊** — 一键 @ 所有在线 agent
- **点击选项** — 直接发送

选择后自动在消息前加上 `@bot名 `，无需手动输入。

### 真实心跳状态

以前使用固定文字轮转（"正在思考..." → "正在分析..."），无论实际进度。现在显示真实系统状态：

| 阶段 | 显示内容 |
|:----|:---------|
| 前 10 秒 | `处理中...（5秒）` → `处理中...（8秒）` |
| 10 秒后 | `处理中...（13秒 \| RSS:185.2MB）` |
| 卡死时 | 秒数继续增长，RSS 不再变化 → 肉眼可见卡住了 |

心跳间隔 **3 秒**（之前 10s），进度反馈更密集。

### 进程内 AIAgent（持久化）

`worker.py` 从子进程 oneshot 模式改为进程内 `AIAgent` 持久化（`ThreadPoolExecutor` + 单例缓存）：

- 首次冷启动 ~6s（import + 预热）
- 后续每次调用 ~1s（无重复 import 开销）
- 告别 subprocess 启动/通信/清理开销
- 120s 超时保护（之前 oneshot 无超时限制）
- 环境变量安全隔离（调用前后保存/恢复）

### 图片上传与 MEDIA 标签

两种图片嵌入方式：

1. **前端上传** — 点击 🖼 按钮选图，base64 上传到服务端，保存到 `static/uploads/`，返回 Markdown 图片链接
2. **MEDIA 标签** — 消息中包含 `MEDIA:/path/to/image.jpg` 自动复制到 `static/uploads/` 并渲染为图片（字符级解析跳过代码块内匹配）

### 文章预览卡片

消息中检测 `📖 文章预览: <slug>` 模式，自动在聊天气泡内嵌入 iframe 预览卡片：

- 默认高度 360px
- 展开按钮 → 80vh 全屏预览
- 底部链接可直接在新标签页打开

### External Message API

```bash
curl -X POST http://localhost:9093/api/external/message \
  -H "Content-Type: application/json" \
  -d '{"username": "系统通知", "content": "群聊消息内容"}'
```

方便外部服务/脚本向 chatroom 推送消息。

### 技术细节

| 改动 | 文件 | 说明 |
|:----|:----|:-----|
| Bot 选择器模态框 | `static/chat.html` | 上下左右键导航、Enter 发送、Escape 取消 |
| 图片上传按钮 | `static/chat.html` | FileReader base64 → WebSocket image_upload |
| Markdown 图片渲染 | `static/chat.html` | simpleMarkdown 支持 `![]()` 和纯图片 URL |
| 文章预览卡片 | `static/chat.html` | 📖 标记触发 iframe 嵌入 |
| 思考状态 60s 超时 | `static/chat.html` | setTimeout 兜底，防止状态永久显示 |
| MEDIA 标签处理 | `relay_server.py` | 字符级解析跳过代码块，shutil.copy2 复制 |
| 图片上传存储 | `relay_server.py` | base64 解码 → uuid 命名 → static/uploads/ |
| broadcast 并行 await | `relay_server.py` | create_task + gather 修复异步泄漏 |
| 外部消息 API | `relay_server.py` | POST port 9093 /api/external/message |
| MIME 类型扩展 | `relay_server.py` | .jpg/.jpeg/.gif/.webp/.bmp |
| 进程内 AIAgent | `workers/worker.py` | ThreadPoolExecutor + 单例缓存，告别子进程 |
| 真实心跳 | `workers/worker.py` | time.monotonic() 算已用时 + /proc/VmRSS |
| 间隔 10s→3s | `workers/worker.py` | timeout=3 密集进度反馈 |
| 子进程 stdout 静默 | `relay_server.py` | DEVNULL 抑制 worker 日志 |

## Development

```bash
# Install dev dependencies
pip install websockets pyyaml

# Run with auto-reload (using entr or similar)
find . -name '*.py' | entr -r python3 relay_server.py
```

## License

MIT
