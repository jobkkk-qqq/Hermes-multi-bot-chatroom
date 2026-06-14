#!/usr/bin/env python3
"""
多Bot群聊 — Bot Worker
通过 WebSocket 连接到中继服务器，监听 @mention 并调用 Hermes oneshot 回复
"""

import asyncio
import io
import json
import os
import sys
import time
from datetime import datetime

# ── Bot 配置（从环境变量读取） ────────────────────
BOT_NAME = os.environ.get("BOT_NAME", "")
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

# ── 动态导入 Hermes oneshot ──────────────────────
def setup_hermes():
    """设置 Hermes 环境并导入 oneshot API"""
    if not HERMES_HOME:
        print(f"[worker] ⚠️  未配置 HERMES_HOME，将使用默认路径", flush=True)
        return None
    
    os.environ["HERMES_HOME"] = HERMES_HOME
    os.environ["HERMES_YOLO"] = "true"  # 自动审批模式
    
    # 找到 hermes_cli
    hermes_home = HERMES_HOME
    # 尝试各种路径
    possible_paths = [
        "/root/hermes-agent/hermes-agent-2026.5.16/venv/lib/python3.11/site-packages",
    ]
    
    for p in possible_paths:
        if os.path.exists(os.path.join(p, "hermes_cli")):
            if p not in sys.path:
                sys.path.insert(0, p)
            break
    
    try:
        from hermes_cli.oneshot import run_oneshot
        print(f"[worker] ✅ Hermes oneshot API 加载成功", flush=True)
        return run_oneshot
    except ImportError as e:
        print(f"[worker] ⚠️  Hermes oneshot 导入失败: {e}", flush=True)
        print(f"[worker] ⚠️  将使用模拟模式（仅打印不回复）", flush=True)
        return None

run_oneshot = setup_hermes()

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
                    "username": BOT_NAME,
                    "role": "bot"
                }, ensure_ascii=False))
                print(f"[worker] 🚪 已加入房间 {ROOM_ID} 作为 {BOT_NAME}", flush=True)
                
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
                        
                        # 检查是否被 @
                        content = data.get("content", "")
                        if f"@{BOT_NAME}" in content:
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
    content = trigger_msg.get("content", "")
    # 去掉 @机器人 部分，提取实际提问
    mention = f"@{BOT_NAME}"
    question = content.replace(mention, "").strip()
    if not question:
        question = content
    
    # 构建上下文 prompt
    prompt = context.build_prompt(target_message=content)
    
    print(f"[worker] 🧠 正在调用 Hermes oneshot...", flush=True)
    
    if run_oneshot is None:
        # 模拟模式
        reply = f"（{BOT_NAME} 已收到消息，但没有 Hermes oneshot API，无法生成回复）"
        print(f"[worker] ⚠️  模拟模式，不实际调用", flush=True)
    else:
        try:
            # 设置 HERMES_HOME 环境
            os.environ["HERMES_HOME"] = HERMES_HOME
            os.environ["HERMES_YOLO"] = "true"
            
            # 捕获 stdout（oneshot 把回复打印到 stdout，返回 int 退出码）
            stdout_capture = io.StringIO()
            old_stdout = sys.stdout
            sys.stdout = stdout_capture
            
            try:
                exit_code = run_oneshot(prompt=prompt)
            finally:
                sys.stdout = old_stdout
            
            captured = stdout_capture.getvalue().strip()
            
            if exit_code == 0 and captured:
                reply = captured
            elif exit_code == 0 and not captured:
                reply = "（处理完成，但未生成回复）"
            else:
                reply = f"（处理出错，退出码: {exit_code}）"
                print(f"[worker] ⚠️ oneshot 退出码: {exit_code}", flush=True)
        except Exception as e:
            reply = f"（处理出错: {e}）"
            print(f"[worker] ❌ oneshot 调用失败: {e}", flush=True)
    
    print(f"[worker] 💬 回复: {reply[:100]}...", flush=True)
    
    # 发送回复到房间
    await ws.send(json.dumps({
        "type": "message",
        "room": ROOM_ID,
        "content": reply
    }, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(worker_main())
