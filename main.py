import os
import json
import asyncio
import logging
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update
from telegram.error import Forbidden
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

LAST_FORWARD_TIME = "無記錄"
LISTENING_GROUPS_INFO = {}  # 快取監聽群組資訊 {group_id: "群組名稱"}

# ================= 資料持久化 =================
def load_normal_users():
    if os.path.exists(USERS_JSON_PATH):
        try:
            with open(USERS_JSON_PATH, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception as e:
            logger.error(f"{e}")
    return set()

def save_normal_users(users_set):
    try:
        with open(USERS_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(list(users_set), f, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.error(f"{e}")

normal_users = load_normal_users()

def remove_blocked_user(uid):
    if uid in normal_users:
        normal_users.remove(uid)
        save_normal_users(normal_users)
        logger.info(f"用戶 {uid} 已封鎖 Bot，已從接收名單中移除。")

# ================= Bot 指令處理 =================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in USERBOT_LIST:
        await update.message.reply_text("歡迎回來，Userbot 管理員。")
        return
    if user_id not in normal_users:
        normal_users.add(user_id)
        save_normal_users(normal_users)
        logger.info(f"新普通用戶加入: {user_id}")
    await update.message.reply_text("Bot 已啟動！當有新媒體消息時，您將會同步收到。")

async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in USERBOT_LIST or user_id in normal_users:
        status_msg = f"🤖 Bot 狀態：運行中\n📅 上次轉發媒體時間：{LAST_FORWARD_TIME}\n\n📡 正在監聽的群組：\n"
        if LISTENING_GROUPS_INFO:
            for gid, title in LISTENING_GROUPS_INFO.items():
                status_msg += f"- {title} ({gid})\n"
        else:
            status_msg += "目前沒有監聽中的群組或正在初始化中。\n"
        await update.message.reply_text(status_msg)
    else:
        await update.message.reply_text("請先輸入 /start 啟動 Bot。")

# ================= Bot 廣播接收邏輯 =================
ptb_media_cache = {}
ptb_cache_lock = asyncio.Lock()

async def bot_inbox_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 只接收來自 Userbot 帳號發送/轉發給 Bot 的訊息
    msg = update.message
    if not msg or not msg.effective_attachment:
        return

    global LAST_FORWARD_TIME
    LAST_FORWARD_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    current_users = list(normal_users)
    if not current_users:
        return

    gid = msg.media_group_id
    if not gid:
        # 單圖轉發
        for uid in current_users:
            try:
                await msg.copy(chat_id=uid)
            except Forbidden:
                remove_blocked_user(uid)
            except Exception as e:
                logger.error(f"單一發送給 {uid} 失敗: {e}")
    else:
        # 相冊收集並透過原生 copy_messages 完美群發
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
    for uid in list(normal_users):
        try:
            await bot.copy_messages(chat_id=uid, from_chat_id=from_chat_id, message_ids=msg_ids)
        except Forbidden:
            remove_blocked_user(uid)
        except Exception as e:
            logger.error(f"相冊發送給 {uid} 失敗: {e}")

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
    
    # 防止啟動時阻塞等待終端輸入驗證碼
    await client.connect()
    if not await client.is_user_authorized():
        logger.error("Userbot 未授權或 Session 已失效！請先獲取有效 Session 字串存入 userbot_session.txt，腳本已停止 Userbot 部分。")
        return
    
    new_session_string = client.session.save()
    with open(SESSION_TXT_PATH, "w") as f:
        f.write(new_session_string)
        
    logger.info("Userbot 已成功啟動！正在監聽群組...")
    
    # 取得監聽群組資訊並儲存到 memory 中
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
        
        media_group_id = event.message.grouped_id
        
        if media_group_id is None:
            # 透過 Username 發送，避開 ID 解析錯誤
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
    application.create_task(run_userbot(application))

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    bot_app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    
    bot_app.add_handler(CommandHandler("start", start_command))
    bot_app.add_handler(CommandHandler("check", check_command))
    
    # 攔截 Userbot 發來的訊息
    bot_app.add_handler(MessageHandler(filters.User(USERBOT_LIST) & filters.ALL, bot_inbox_handler))
    
    logger.info("Telegram Bot 正在啟動...")
    bot_app.run_polling()

if __name__ == "__main__":
    main()
