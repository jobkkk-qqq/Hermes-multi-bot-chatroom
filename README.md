# 🤖 多Bot群聊Web

> 一个轻量的多 AI 机器人聊天室，支持动态添加/踢出 bot，基于 `@提及` 触发回复。

## 🎯 解决的问题

你有多个 AI 助手（写作 bot、审稿 bot、读者 bot），想让它们在一个聊天房间里协作——被 `@` 时回复，没被 `@` 时静默，0 token 浪费。

```
        用户发消息
            │
     ┌──────┴──────┐
     │  中继服务器   │  ← WebSocket + SQLite
     └──────┬──────┘
            │ 广播消息
     ┌──────┼──────┐
     ▼      ▼      ▼
   @小帅   @YY    @读者
  (Writer) (Editor) (Reader)
    │        │        │
    └─── Hermes oneshot ──┘
        被 @ 时调用，静默时 0 token
```

## ✨ 特性

| 特性 | 说明 |
|:-----|:------|
| 🚀 **实时群聊** | WebSocket 推送，消息即时显示 |
| 🎭 **多 Bot 协作** | 每个 bot 绑定独立的 Hermes profile |
| 🔇 **智能静默** | 没被 `@` 的 bot 0 token 消耗，完全不调用 API |
| 🔄 **动态管理** | 在 UI 中随时添加/踢出 bot，无需重启服务 |
| 💬 **@提及高亮** | 聊天中 `@机器人` 自动高亮 |
| 📜 **消息持久化** | SQLite 存储，刷新页面历史不丢 |
| 📱 **响应式设计** | 桌面 + 移动端自适应 |
| 🏗️ **systemd 托管** | 开机自启，崩溃自动恢复 |

## 🧱 架构

```
/novel/chatroom/
├── relay_server.py       # 中继服务器 (核心)
│   ├── WebSocket 服务    # ws://0.0.0.0:9091/ws
│   ├── HTTP 服务          # http://0.0.0.0:9092/
│   ├── SQLite 持久化      # data/chatroom.db
│   └── Bot 进程管理       # spawn/kill worker
│
├── static/
│   └── chat.html         # 聊天页面 (单页 HTML+CSS+JS)
│
├── workers/
│   ├── worker.py          # Bot Worker 通用脚本
│   └── config.json        # Bot 配置清单
│
├── data/
│   └── chatroom.db        # SQLite (自动创建)
│
├── .gitignore
├── README.md
└── LICENSE
```

### 通信协议

```
客户端 ──WS──→ 中继服务器 ──WS──→ Bot Worker
                     ↑
                HTTP API (添加/踢出 Bot)
```

**WebSocket 消息格式：**

```json
// 客户端 → 服务器
{"type": "join",    "room": "main", "username": "用户", "role": "user"}
{"type": "message", "room": "main", "content": "hello @小帅"}
{"type": "ping"}

// 服务器 → 客户端
{"type": "message", "id": 1, "username": "小帅", "content": "回复", "time": "12:00"}
{"type": "system",  "content": "小帅 加入了群聊"}
{"type": "history", "messages": [...]}
{"type": "members", "members": [{"username":"小帅","role":"bot"}]}
```

## 🚀 快速开始

### 前置依赖

- Python 3.10+
- [Hermes Agent](https://github.com/NousResearch/hermes-agent)（或兼容的 oneshot API）
- pip

### 安装

```bash
# 1. 克隆仓库
git clone https://github.com/<你的用户名>/multi-bot-chatroom.git
cd multi-bot-chatroom

# 2. 安装依赖（只需 websockets）
pip install websockets

# 3. 配置 Bot
#    编辑 workers/config.json，配置你的 bot 列表
```

### 配置 Bot

编辑 `workers/config.json`：

```json
{
  "bots": {
    "小帅": {
      "profile": "writer",
      "hermes_home": "/root/.hermes/profiles/writer/",
      "description": "编故事 / 小说写作"
    },
    "YY": {
      "profile": "editor",
      "hermes_home": "/root/.hermes/profiles/editor/",
      "description": "起点编辑 / 审稿评估"
    }
  }
}
```

### 启动

```bash
# 直接运行
python3 relay_server.py

# 或安装为系统服务
sudo cp chatroom-relay.service /etc/systemd/system/
sudo systemctl enable --now chatroom-relay.service
```

### 访问

打开浏览器访问：`http://<服务器IP>:9092/`

## 🎮 使用说明

### 添加 Bot

在左侧「机器人管理」面板点击 **加入**，bot 会立刻上线并出现在成员列表中。

### 召唤 Bot

在输入框输入 `@小帅 帮我写一段对话`，小帅会处理并回复。其他 bot 不受影响、静默。

### 踢出 Bot

点击 bot 旁的 **踢出** 按钮，worker 进程立即终止，资源完全释放。

## 🔧 Bot Worker 机制

每个 bot 是一个独立的 Python 子进程：

1. 通过 WebSocket 连接到中继服务器
2. 持续监听房间消息
3. 检测 `@bot名` → 组装上下文 → 调用 Hermes oneshot → 回复
4. 没被 @ 时完全静默，0 网络请求
5. bot 被踢出 → 进程被 kill → 资源彻底释放

## 🌐 API 接口

| 接口 | 方法 | 说明 |
|:----|:----|:-----|
| `/` | GET | 聊天页面 |
| `/api/bots` | GET | 获取可用 bot 列表 |
| `/api/members` | GET | 获取房间成员列表 |
| `/api/history` | GET | 获取消息历史 |
| `/api/add_bot?name=小帅` | GET | 添加 bot 到房间 |
| `/api/kick_bot?name=小帅` | GET | 从房间踢出 bot |

## 📦 依赖

仅需一个外部包：`websockets`（WebSocket 协议实现）。

Bot 调用大模型时使用 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 的 oneshot API（`hermes_cli.oneshot.run_oneshot`），通过 stdout 捕获回复文本。

## 🧪 测试

```bash
# 检查服务是否运行
curl http://127.0.0.1:9092/api/bots

# 添加 bot
curl http://127.0.0.1:9092/api/add_bot?name=小帅

# 查看成员
curl http://127.0.0.1:9092/api/members
```

## 📄 License

MIT
