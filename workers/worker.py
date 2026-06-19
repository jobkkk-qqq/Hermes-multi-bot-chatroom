#!/usr/bin/env python3
"""
多Bot群聊 — Bot Worker
通过 WebSocket 连接到中继服务器，监听 @mention 并调用 Hermes oneshot 回复
"""

import asyncio
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# ── Bot 配置（从环境变量读取） ────────────────────
BOT_NAME = os.environ.get("BOT_NAME", "")
BOT_DISPLAY_NAME = os.environ.get("BOT_DISPLAY_NAME", BOT_NAME)
BOT_PROFILE = os.environ.get("BOT_PROFILE", "")
RELAY_WS_URL = os.environ.get("RELAY_WS_URL", "ws://127.0.0.1:9091/ws")
ROOM_ID = os.environ.get("ROOM_ID", "main")

# 加载 config.json
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

BOT_CONFIG = CONFIG["bots"].get(BOT_NAME, {})
HERMES_HOME = BOT_CONFIG.get("hermes_home", "")

if not BOT_NAME or not BOT_PROFILE:
    print(f"[worker] ❌ 缺少环境变量 BOT_NAME/BOT_PROFILE", flush=True)
    sys.exit(1)

print(f"[worker] 🤖 Bot '{BOT_NAME}' (profile={BOT_PROFILE}) 启动中...", flush=True)
print(f"[worker] 📍 中继: {RELAY_WS_URL}", flush=True)
print(f"[worker] 📁 HERMES_HOME: {HERMES_HOME}", flush=True)

# ── 检查 Hermes CLI ────────────────────────────
HERMES_CLI = "/root/.local/bin/hermes"

def check_hermes_cli():
    """检查 hermes CLI 是否可执行"""
    if os.path.isfile(HERMES_CLI) and os.access(HERMES_CLI, os.X_OK):
        print(f"[worker] ✅ Hermes CLI 可用: {HERMES_CLI}", flush=True)
        return True
    print(f"[worker] ⚠️  Hermes CLI 不可用: {HERMES_CLI}", flush=True)
    return False

hermes_ok = check_hermes_cli()

# ── 方案B：进程内 Hermes 调用（代替子进程） ──────────
_hermes_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="hermes")
_hermes_agent = None  # 持久化 agent，避免每次冷启动

def _init_hermes_agent(hermes_home: str):
    """创建并缓存 AIAgent，大幅节省后续调用时间（6s 冷启动 → 1s 复用）"""
    global _hermes_agent
    if _hermes_agent is not None:
        return _hermes_agent

    from hermes_cli.config import load_config
    from hermes_cli.runtime_provider import resolve_runtime_provider
    from hermes_cli.tools_config import _get_platform_tools
    from run_agent import AIAgent

    cfg = load_config()
    model_cfg = cfg.get("model") or {}
    cfg_model = (model_cfg if isinstance(model_cfg, str)
                 else (model_cfg.get("default") or model_cfg.get("model") or ""))
    toolsets_list = sorted(_get_platform_tools(cfg, "cli"))

    runtime = resolve_runtime_provider(target_model=cfg_model or None)

    def _clarify_callback(question, choices=None):
        """Oneshot 模式：无用户可问，让 agent 自己做合理选择"""
        if choices:
            return (f"[oneshot mode: no user available. Pick the best option from "
                    f"{choices} using your own judgment and continue.]")
        return ("[oneshot mode: no user available. Make the most reasonable "
                "assumption you can and continue.]")

    agent = AIAgent(
        api_key=runtime.get("api_key"),
        base_url=runtime.get("base_url"),
        provider=runtime.get("provider"),
        api_mode=runtime.get("api_mode"),
        model=cfg_model,
        enabled_toolsets=toolsets_list,
        quiet_mode=True,
        platform="cli",
        session_db=None,
        credential_pool=runtime.get("credential_pool"),
        clarify_callback=_clarify_callback,
    )
    agent.suppress_status_output = True
    agent.stream_delta_callback = None
    agent.tool_gen_callback = None

    # 预热：跑一次简单的 chat 并清空 session_messages
    try:
        agent.chat("预热")
    except Exception:
        pass
    agent._session_messages = []

    _hermes_agent = agent
    print(f"[worker] ✅ Hermes AIAgent 已初始化（缓存）", flush=True)
    return agent

def _run_hermes_oneshot(prompt: str, hermes_home: str) -> str:
    """在子线程中调用 Hermes 持久化 agent，无 300s 超时限制。

    首次调用会冷启动（~6s），后续每次约 1~2s。
    返回 AI 回复文本，失败时抛出异常。
    """
    # 暂存原环境变量
    old_home = os.environ.get("HERMES_HOME", "")
    old_yolo = os.environ.get("HERMES_YOLO_MODE", "")
    old_hooks = os.environ.get("HERMES_ACCEPT_HOOKS", "")

    try:
        os.environ["HERMES_HOME"] = hermes_home
        os.environ["HERMES_YOLO_MODE"] = "1"
        os.environ["HERMES_ACCEPT_HOOKS"] = "1"

        agent = _init_hermes_agent(hermes_home)
        result = agent.chat(prompt) or ""
        # 清空 session 消息，确保每次调用独立
        agent._session_messages = []
        return result
    finally:
        # 恢复原环境变量
        if old_home:
            os.environ["HERMES_HOME"] = old_home
        else:
            os.environ.pop("HERMES_HOME", None)
        if old_yolo:
            os.environ["HERMES_YOLO_MODE"] = old_yolo
        else:
            os.environ.pop("HERMES_YOLO_MODE", None)
        if old_hooks:
            os.environ["HERMES_ACCEPT_HOOKS"] = old_hooks
        else:
            os.environ.pop("HERMES_ACCEPT_HOOKS", None)

# ── 上下文管理 ──────────────────────────────────
class ContextManager:
    """管理最近 N 条消息作为上下文"""
    def __init__(self, max_messages=20):
        self.messages = []
        self.max_messages = max_messages
    
    def add(self, msg):
        self.messages.append(msg)
        if len(self.messages) > self.max_messages:
            self.messages = self.messages[-self.max_messages:]
    
    def build_prompt(self, target_message=None):
        """组装上下文 prompt"""
        if not self.messages:
            return ""
        
        parts = ["--- 以下是群聊上下文 ---"]
        for msg in self.messages:
            username = msg.get("username", "?")
            content = msg.get("content", "")
            time_str = msg.get("time", "")
            parts.append(f"[{time_str}] {username}: {content}")
        
        parts.append("--- 结束 ---")
        parts.append("")
        
        if target_message:
            parts.append(f"你被 @了，消息内容：{target_message}")
        
        parts.append("请直接回复（不要包含 @你的名字）：")
        
        return "\n".join(parts)

context = ContextManager(max_messages=30)

# ── WebSocket 连接 ──────────────────────────────
async def worker_main():
    import websockets
    from websockets.asyncio.client import connect
    
    while True:
        try:
            print(f"[worker] 🔌 连接中继 {RELAY_WS_URL}...", flush=True)
            async with connect(RELAY_WS_URL) as ws:
                print(f"[worker] ✅ 已连接中继", flush=True)
                
                # 加入房间
                await ws.send(json.dumps({
                    "type": "join",
                    "room": ROOM_ID,
                    "username": BOT_DISPLAY_NAME,
                    "role": "bot"
                }, ensure_ascii=False))
                print(f"[worker] 🚪 已加入房间 {ROOM_ID} 作为 {BOT_DISPLAY_NAME}", flush=True)
                
                # 监听消息
                async for raw in ws:
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    
                    msg_type = data.get("type", "")
                    
                    if msg_type == "message":
                        # 存入上下文
                        context.add(data)
                        
                        # 检查是否被 @（用显示名）
                        content = data.get("content", "")
                        if f"@{BOT_DISPLAY_NAME}" in content:
                            print(f"[worker] 📨 被 @了！消息: {content[:80]}...", flush=True)
                            await handle_mention(ws, data)
                    
                    elif msg_type == "history":
                        # 历史消息写入上下文
                        for msg in data.get("messages", []):
                            context.add(msg)
                    
                    elif msg_type == "members":
                        pass  # 不需要处理
                    
                    elif msg_type == "pong":
                        pass
        
        except websockets.exceptions.ConnectionClosed:
            print(f"[worker] 🔌 连接断开，5秒后重连...", flush=True)
        except Exception as e:
            print(f"[worker] ❌ 错误: {e}，5秒后重连...", flush=True)
        
        await asyncio.sleep(5)

async def handle_mention(ws, trigger_msg):
    """处理 @mention"""
    # 🔔 唤醒对应的 Feishu gateway（可选，文件存在时才执行）
    wake_script = "/novel/scripts/gateway-wake.sh"
    if os.path.exists(wake_script):
        try:
            proc = await asyncio.create_subprocess_exec(
                "bash", wake_script, BOT_PROFILE,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL
            )
            # 不 await，让唤醒在后台进行
            print(f"[worker] 🔔 已触发唤醒 Feishu gateway ({BOT_PROFILE})", flush=True)
        except Exception as e:
            print(f"[worker] ⚠️ 唤醒 gateway 失败: {e}", flush=True)
    else:
        print(f"[worker] ℹ️  gateway-wake.sh 不存在，跳过唤醒", flush=True)
    
    content = trigger_msg.get("content", "")
    # 去掉 @机器人 部分，提取实际提问
    mention = f"@{BOT_DISPLAY_NAME}"
    question = content.replace(mention, "").strip()
    if not question:
        question = content
    
    # 构建上下文 prompt
    prompt = context.build_prompt(target_message=content)
    
    # ── 思考中心跳（每 5 秒发一次状态）──
    async def thinking_heartbeat(stop_event):
        """每 10 秒发一次 thinking 状态，直到 stop_event 被设置"""
        statuses = ["正在思考...", "正在分析内容...", "仍在处理中...", "快好了...", "马上就好..."]
        idx = 0
        while not stop_event.is_set():
            try:
                await ws.send(json.dumps({
                    "type": "thinking",
                    "username": BOT_DISPLAY_NAME,
                    "status": statuses[idx % len(statuses)]
                }, ensure_ascii=False))
                idx += 1
            except:
                pass
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=3)
            except asyncio.TimeoutError:
                continue
    
    stop_heartbeat = asyncio.Event()
    hb_task = asyncio.create_task(thinking_heartbeat(stop_heartbeat))
    
    print(f"[worker] 🧠 正在调用 Hermes oneshot（进程内）...", flush=True)

    reply = ""  # 默认值，防止 Pyright 报未绑定

    try:
        if not hermes_ok:
            reply = f"（{BOT_DISPLAY_NAME} 已收到消息，但 Hermes CLI 不可用）"
            print(f"[worker] ⚠️  Hermes CLI 不可用，跳过调用", flush=True)
        else:
            try:
                # ── 方案B：进程内 _run_agent，无 300s 超时 ──
                loop = asyncio.get_event_loop()
                response = await asyncio.wait_for(
                    loop.run_in_executor(
                        _hermes_executor, _run_hermes_oneshot, prompt, HERMES_HOME
                    ),
                    timeout=120  # 最多等 2 分钟
                )
                reply = response.strip() if response and response.strip() else "（处理完成，但未生成回复）"
            except asyncio.TimeoutError:
                reply = f"（{BOT_DISPLAY_NAME} 处理超时，请重试）"
                print(f"[worker] ⏰ Hermes 调用超时 (120s)", flush=True)
            except Exception as e:
                reply = f"（处理出错: {e}）"
                print(f"[worker] ❌ in-process oneshot 失败: {e}", flush=True)
    finally:
        # 停止心跳
        stop_heartbeat.set()
        await hb_task
        # 发送心跳结束信号
        try:
            await ws.send(json.dumps({
                "type": "thinking_end",
                "username": BOT_DISPLAY_NAME
            }, ensure_ascii=False))
        except:
            pass
    
    print(f"[worker] 💬 回复: {reply[:100]}...", flush=True)
    
    # 发送回复到房间
    await ws.send(json.dumps({
        "type": "message",
        "room": ROOM_ID,
        "content": reply
    }, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(worker_main())
