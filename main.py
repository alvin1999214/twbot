import os
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from telegram import Update
from telegram.error import Forbidden, NetworkError, RetryAfter, TimedOut
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telethon import TelegramClient, events
from telethon.sessions import StringSession

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
BOT_USERNAME = os.getenv("BOT_USERNAME")
USERBOT_KEY = int(os.getenv("USERBOT_KEY"))
USERBOT_HASH = os.getenv("USERBOT_HASH")

USERBOT_LIST = [int(x.strip()) for x in os.getenv("USERBOT_LIST", "").split(",") if x.strip()]
LISTENING_GROUPS = [int(x.strip()) for x in os.getenv("LISTENING_GROUPS", "").split(",") if x.strip()]

DATA_DIR = "/app/data"
USERS_JSON_PATH = os.path.join(DATA_DIR, "users.json")
SESSION_TXT_PATH = os.path.join(DATA_DIR, "userbot_session.txt")
MAX_SEND_RETRIES = int(os.getenv("MAX_SEND_RETRIES", "3"))

# 將預設併發數降至 5，避免觸發 Telegram 的瞬間併發限制
MAX_CONCURRENT_SENDS = int(os.getenv("MAX_CONCURRENT_SENDS", "5"))
send_semaphore = None  
broadcast_queue = None  # 全局廣播隊列

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

LAST_FORWARD_TIME = "無記錄"
LISTENING_GROUPS_INFO = {} 

# ================= 資料持久化 =================
def load_normal_users():
    if os.path.exists(USERS_JSON_PATH):
        try:
            with open(USERS_JSON_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
                if not isinstance(raw, list):
                    logger.error("users.json 格式錯誤，預期為 list。")
                    return set()
                users = set()
                for uid in raw:
                    try:
                        users.add(int(uid))
                    except (TypeError, ValueError):
                        logger.warning(f"跳過無效 user id: {uid}")
                return users
        except Exception as e:
            logger.error(f"{e}")
    return set()

def save_normal_users(users_set):
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp_path = f"{USERS_JSON_PATH}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(sorted(users_set), f, ensure_ascii=False, indent=4)
        os.replace(tmp_path, USERS_JSON_PATH)
    except Exception as e:
        logger.error(f"{e}")

normal_users = load_normal_users()
users_lock = asyncio.Lock()

async def get_latest_users_snapshot():
    global normal_users
    async with users_lock:
        normal_users = load_normal_users()
        return list(normal_users)

async def add_normal_user(uid):
    global normal_users
    async with users_lock:
        latest_users = load_normal_users()
        if uid in latest_users:
            normal_users = latest_users
            return False
        latest_users.add(uid)
        save_normal_users(latest_users)
        normal_users = latest_users
        return True

async def remove_blocked_user(uid):
    global normal_users
    async with users_lock:
        latest_users = load_normal_users()
        if uid in latest_users:
            latest_users.remove(uid)
            save_normal_users(latest_users)
            normal_users = latest_users
            logger.info(f"用戶 {uid} 已封鎖 Bot，已從接收名單中移除。")

# ================= 發送邏輯 =================
async def copy_single_with_retry(msg, uid):
    for attempt in range(1, MAX_SEND_RETRIES + 1):
        try:
            # 縮小 Semaphore 的鎖定範圍，只在打 API 的瞬間鎖定
            async with send_semaphore:
                await msg.copy(chat_id=uid)
            return
        except Forbidden:
            await remove_blocked_user(uid)
            return
        except RetryAfter as e:
            wait_seconds = float(getattr(e, "retry_after", 1))
            logger.warning(f"單圖發送給 {uid} 觸發限流，{wait_seconds}s 後重試。")
            # 在 Semaphore 外部等待，不佔用併發資源
            await asyncio.sleep(wait_seconds + 0.5)
        except (TimedOut, NetworkError) as e:
            if attempt >= MAX_SEND_RETRIES:
                logger.error(f"單圖發送給 {uid} 失敗（網路/逾時）: {e}")
                return
            await asyncio.sleep(min(2 ** attempt, 5))
        except Exception as e:
            logger.error(f"單圖發送給 {uid} 失敗: {e}")
            return

async def copy_album_with_retry(bot, from_chat_id, msg_ids, uid):
    for attempt in range(1, MAX_SEND_RETRIES + 1):
        try:
            # 縮小 Semaphore 的鎖定範圍
            async with send_semaphore:
                await bot.copy_messages(chat_id=uid, from_chat_id=from_chat_id, message_ids=msg_ids)
            return
        except Forbidden:
            await remove_blocked_user(uid)
            return
        except RetryAfter as e:
            wait_seconds = float(getattr(e, "retry_after", 1))
            logger.warning(f"相冊發送給 {uid} 觸發限流，{wait_seconds}s 後重試。")
            await asyncio.sleep(wait_seconds + 0.5)
        except (TimedOut, NetworkError) as e:
            if attempt >= MAX_SEND_RETRIES:
                logger.error(f"相冊發送給 {uid} 失敗（網路/逾時）: {e}")
                return
            await asyncio.sleep(min(2 ** attempt, 5))
        except Exception as e:
            logger.error(f"相冊發送給 {uid} 失敗: {e}")
            return

# ================= 背景廣播工人 (消費者) =================
async def broadcast_worker(bot):
    """
    永遠在背景運行的任務，從隊列中取出訊息並平滑廣播，確保不阻擋主事件迴圈。
    """
    logger.info("背景廣播工人已啟動！")
    while True:
        try:
            job = await broadcast_queue.get()
            job_type = job.get("type")
            users = job.get("users", [])

            if job_type == "single":
                msg = job["msg"]
                for uid in users:
                    asyncio.create_task(copy_single_with_retry(msg, uid))
                    # 單圖發送，0.05 秒產生一個任務 (最高 20 API/s)
                    await asyncio.sleep(0.05) 
            
            elif job_type == "album":
                from_chat_id = job["from_chat_id"]
                msg_ids = job["msg_ids"]
                album_size = len(msg_ids)
                for uid in users:
                    asyncio.create_task(copy_album_with_retry(bot, from_chat_id, msg_ids, uid))
                    # 相冊包含多張圖片，API 請求量倍增。依圖片數量動態延長間隔
                    await asyncio.sleep(0.05 * album_size)

            broadcast_queue.task_done()
        except Exception as e:
            logger.error(f"廣播隊列處理發生錯誤: {e}")

# ================= Bot 指令處理 =================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in USERBOT_LIST:
        await update.message.reply_text(
            "歡迎回來，Userbot 管理員。\n\n"
            "可用指令：\n"
            "/start - 啟動 Bot\n"
            "/check - 檢查機器人狀態及正在監聽的群組"
        )
        return
    added = await add_normal_user(user_id)
    if added:
        logger.info(f"新普通用戶加入: {user_id}")
    await update.message.reply_text(
        "歡迎使用閃圖司機！當有新媒體消息時，您將會同步收到，自動跟車。\n\n"
        "可用指令：\n"
        "/start - 啟動機器人\n"
        "/check - 檢查機器人狀態及正在跟車的群組"
    )

async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    users_snapshot = await get_latest_users_snapshot()
    if user_id in USERBOT_LIST or user_id in users_snapshot:
        queue_size = broadcast_queue.qsize() if broadcast_queue else 0
        status_msg = f"🤖 Bot 狀態：運行中\n"
        status_msg += f"📅 上次轉發媒體：{LAST_FORWARD_TIME}\n"
        status_msg += f"📦 目前等待發送的排隊任務數：{queue_size}\n\n"
        status_msg += f"📡 正在跟車的群組：\n"
        if LISTENING_GROUPS_INFO:
            for gid, title in LISTENING_GROUPS_INFO.items():
                status_msg += f"- {title} ({gid})\n"
        else:
            status_msg += "目前沒有跟車中的群組或正在初始化中。\n"
        await update.message.reply_text(status_msg)
    else:
        await update.message.reply_text("請先輸入 /start 啟動機器人。")

# ================= Bot 廣播接收邏輯 (生產者) =================
ptb_media_cache = {}
ptb_cache_lock = asyncio.Lock()

async def bot_inbox_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.effective_attachment:
        return

    global LAST_FORWARD_TIME
    hkt_tz = timezone(timedelta(hours=8))
    LAST_FORWARD_TIME = datetime.now(hkt_tz).strftime("%Y-%m-%d %H:%M:%S")

    current_users = await get_latest_users_snapshot()
    if not current_users:
        return

    gid = msg.media_group_id
    if not gid:
        # 單圖轉發：將任務放入隊列後瞬間結束，不阻塞！
        await broadcast_queue.put({
            "type": "single",
            "msg": msg,
            "users": current_users
        })
    else:
        # 相冊收集邏輯
        async with ptb_cache_lock:
            if gid not in ptb_media_cache:
                ptb_media_cache[gid] = []
                asyncio.create_task(process_ptb_album(msg.chat_id, gid, context.bot))
            ptb_media_cache[gid].append(msg.message_id)

async def process_ptb_album(from_chat_id, gid, bot):
    await asyncio.sleep(0.6)
    async with ptb_cache_lock:
        msg_ids = ptb_media_cache.pop(gid, [])
    
    if not msg_ids:
        return
    
    msg_ids.sort()
    current_users = await get_latest_users_snapshot()
    
    # 相冊轉發：將任務放入隊列後瞬間結束！
    await broadcast_queue.put({
        "type": "album",
        "from_chat_id": from_chat_id,
        "msg_ids": msg_ids,
        "users": current_users
    })

# ================= Userbot 監聽邏輯 =================
media_groups_cache = {}
cache_lock = asyncio.Lock()

async def run_userbot(bot_app: Application):
    os.chdir(DATA_DIR)
    session_string = ""
    if os.path.exists(SESSION_TXT_PATH):
        with open(SESSION_TXT_PATH, "r") as f:
            session_string = f.read().strip()

    client = TelegramClient(StringSession(session_string), USERBOT_KEY, USERBOT_HASH)
    
    await client.connect()
    if not await client.is_user_authorized():
        logger.error("Userbot 未授權或 Session 已失效！請先獲取有效 Session 字串存入 userbot_session.txt，腳本已停止 Userbot 部分。")
        return
    
    new_session_string = client.session.save()
    with open(SESSION_TXT_PATH, "w") as f:
        f.write(new_session_string)
        
    logger.info("Userbot 已成功啟動！正在監聽群組...")
    
    global LISTENING_GROUPS_INFO
    for group_id in LISTENING_GROUPS:
        try:
            entity = await client.get_entity(group_id)
            LISTENING_GROUPS_INFO[group_id] = getattr(entity, 'title', str(group_id))
        except Exception as e:
            logger.error(f"獲取群組 {group_id} 資訊失敗: {e}")
            LISTENING_GROUPS_INFO[group_id] = "未知群組"
    
    @client.on(events.NewMessage(chats=LISTENING_GROUPS))
    async def handler(event):
        if not event.message.media:
            return
        
        if event.message.sticker:
            return
        
        media_group_id = event.message.grouped_id
        
        if media_group_id is None:
            try:
                await client.forward_messages(BOT_USERNAME, event.message)
            except Exception as e:
                logger.error(f"Userbot 單圖轉發至 Bot 失敗: {e}")
        else:
            async with cache_lock:
                if media_group_id not in media_groups_cache:
                    media_groups_cache[media_group_id] = []
                    asyncio.create_task(process_media_group(client, media_group_id))
                media_groups_cache[media_group_id].append(event.message)

    async def process_media_group(client, media_group_id):
        await asyncio.sleep(0.6)
        async with cache_lock:
            messages = media_groups_cache.pop(media_group_id, [])
        if messages:
            messages.sort(key=lambda x: x.id)
            try:
                await client.forward_messages(BOT_USERNAME, messages)
            except Exception as e:
                logger.error(f"Userbot 相冊轉發至 Bot 失敗: {e}")

    await client.run_until_disconnected()

async def post_init(application: Application):
    global send_semaphore, broadcast_queue
    # 初始化 Semaphore 與 Queue
    send_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SENDS)
    broadcast_queue = asyncio.Queue()
    
    # 啟動背景廣播工人
    application.create_task(broadcast_worker(application.bot))
    # 啟動 Userbot
    application.create_task(run_userbot(application))

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    bot_app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    
    bot_app.add_handler(CommandHandler("start", start_command))
    bot_app.add_handler(CommandHandler("check", check_command))
    
    bot_app.add_handler(MessageHandler(filters.User(USERBOT_LIST) & filters.ALL, bot_inbox_handler))
    
    logger.info("Telegram Bot 正在啟動...")
    bot_app.run_polling()

if __name__ == "__main__":
    main()
