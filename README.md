# Multi-Bot Chatroom 🤖

A real-time web-based chatroom where multiple AI bot agents coexist. Each bot can be independently added, managed, and @mentioned — only mentioned bots consume tokens; silent bots stay quiet.

Built with Python `asyncio` + WebSocket + SQLite. Zero external framework dependencies for the server.

## Features

- **Multi-bot architecture** — Add/remove bot agents dynamically via the sidebar
- **@mention routing** — Only @mentioned bots respond; silent bots cost nothing
- **Real-time WebSocket** — All messages broadcast instantly to connected clients
- **Persistent history** — SQLite-backed message and membership storage
- **Bot worker isolation** — Each bot runs as a separate subprocess with its own Hermes AI profile
- **Auto-restore** — Bots automatically reconnect on server restart
- **@mention autocomplete** — Type `@` to see and filter available bots
- **Responsive design** — Mobile-friendly with sliding sidebar
- **Single port** — HTTP (chat UI + REST API) and WebSocket share one port

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
| `static/chat.html` | Frontend: chat UI with @mention autocomplete, member list, bot management |
| `workers/config.json` | Bot worker configurations (profile, Hermes home path) |

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Chat web UI |
| `/api/bots` | GET | List available bots (key, name, avatar) |
| `/api/members` | GET | List current room members |
| `/api/history` | GET | Recent message history |
| `/api/add_bot?name=<key>` | GET | Add a bot to the room |
| `/api/kick_bot?name=<key>` | GET | Remove a bot from the room |

### WebSocket Protocol

| Message Type | Direction | Description |
|-------------|-----------|-------------|
| `join` | Client → Server | Join a room with username |
| `message` | Bidirectional | Chat message |
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

## Development

```bash
# Install dev dependencies
pip install websockets pyyaml

# Run with auto-reload (using entr or similar)
find . -name '*.py' | entr -r python3 relay_server.py
```

## License

MIT
