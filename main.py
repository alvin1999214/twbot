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

BOT_TOKEN        = os.getenv("BOT_TOKEN")
BOT_USERNAME     = os.getenv("BOT_USERNAME")
USERBOT_KEY      = int(os.getenv("USERBOT_KEY"))
USERBOT_HASH     = os.getenv("USERBOT_HASH")

USERBOT_LIST     = [int(x.strip()) for x in os.getenv("USERBOT_LIST",     "").split(",") if x.strip()]
LISTENING_GROUPS = [int(x.strip()) for x in os.getenv("LISTENING_GROUPS", "").split(",") if x.strip()]

DATA_DIR          = "/app/data"
USERS_JSON_PATH   = os.path.join(DATA_DIR, "users.json")
SESSION_TXT_PATH  = os.path.join(DATA_DIR, "userbot_session.txt")
MAX_SEND_RETRIES  = int(os.getenv("MAX_SEND_RETRIES",  "3"))

# ── Rate-limit budget ──────────────────────────────────────────────────────────
# Telegram allows ~30 messages/s globally and ~1 msg/s per chat (bot API).
# We stay well under the global cap with a token-bucket at 25 msg/s.
# Each individual send is also serialised through the bucket, so we never
# need to fire-and-forget 1 000 tasks simultaneously.
GLOBAL_RATE_LIMIT = int(os.getenv("GLOBAL_RATE_LIMIT", "25"))  # tokens / second

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

LAST_FORWARD_TIME    = "無記錄"
LISTENING_GROUPS_INFO: dict = {}

# ================= Token-Bucket Rate Limiter ==================================
class TokenBucket:
    """
    Leaky-bucket / token-bucket that caps outgoing API calls to
    `rate` calls per second regardless of how many coroutines are waiting.

    Usage:
        await bucket.acquire()   # blocks until a token is available
    """
    def __init__(self, rate: int):
        self._rate      = rate          # tokens refilled per second
        self._tokens    = float(rate)   # start full
        self._last_refill = asyncio.get_event_loop().time()
        self._lock      = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now = asyncio.get_event_loop().time()
                elapsed = now - self._last_refill
                # Refill tokens proportional to elapsed time
                self._tokens = min(
                    float(self._rate),
                    self._tokens + elapsed * self._rate
                )
                self._last_refill = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

            # Not enough tokens — sleep for one token's worth of time, then retry
            await asyncio.sleep(1.0 / self._rate)

# Global bucket — created in post_init once the event loop is running.
rate_bucket: TokenBucket | None = None

# Global FloodWait gate: when Telegram tells us to back off we pause
# ALL sends (not just the one that hit the error) until the window expires.
floodwait_until: float = 0.0   # loop.time() deadline

async def wait_if_flooded():
    """Pause the calling coroutine until any active FloodWait window clears."""
    loop = asyncio.get_event_loop()
    remaining = floodwait_until - loop.time()
    if remaining > 0:
        await asyncio.sleep(remaining)

# ================= Data persistence ==========================================
def load_normal_users() -> set:
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
            logger.error(f"載入用戶清單失敗: {e}")
    return set()

def save_normal_users(users_set: set):
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp_path = f"{USERS_JSON_PATH}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(sorted(users_set), f, ensure_ascii=False, indent=4)
        os.replace(tmp_path, USERS_JSON_PATH)
    except Exception as e:
        logger.error(f"儲存用戶清單失敗: {e}")

normal_users  = load_normal_users()
users_lock    = asyncio.Lock()

async def get_latest_users_snapshot() -> list:
    global normal_users
    async with users_lock:
        normal_users = load_normal_users()
        return list(normal_users)

async def add_normal_user(uid: int) -> bool:
    global normal_users
    async with users_lock:
        latest = load_normal_users()
        if uid in latest:
            normal_users = latest
            return False
        latest.add(uid)
        save_normal_users(latest)
        normal_users = latest
        return True

async def remove_blocked_user(uid: int):
    global normal_users
    async with users_lock:
        latest = load_normal_users()
        if uid in latest:
            latest.remove(uid)
            save_normal_users(latest)
            normal_users = latest
            logger.info(f"用戶 {uid} 已封鎖 Bot，已從接收名單移除。")

# ================= Core send helpers =========================================
async def _safe_send(coro_fn, uid: int):
    """
    Execute one Telegram API call through the token-bucket + FloodWait gate,
    with per-call retry logic.

    `coro_fn` is a zero-argument async callable that performs the actual API
    call, e.g.:  lambda: msg.copy(chat_id=uid)
    """
    global floodwait_until

    for attempt in range(1, MAX_SEND_RETRIES + 1):
        # 1. Honour any active global FloodWait window first
        await wait_if_flooded()
        # 2. Acquire a rate-limit token (blocks until budget allows)
        await rate_bucket.acquire()
        try:
            await coro_fn()
            return  # success
        except Forbidden:
            await remove_blocked_user(uid)
            return  # no point retrying
        except RetryAfter as e:
            wait_sec = float(getattr(e, "retry_after", 5)) + 1.0
            logger.warning(
                f"FloodWait {wait_sec:.1f}s triggered while sending to {uid} "
                f"— pausing ALL sends."
            )
            # Set the global gate so every other sender also waits
            floodwait_until = max(
                floodwait_until,
                asyncio.get_event_loop().time() + wait_sec
            )
            await asyncio.sleep(wait_sec)
        except (TimedOut, NetworkError) as e:
            if attempt >= MAX_SEND_RETRIES:
                logger.error(f"發送給 {uid} 失敗（網路/逾時），已放棄: {e}")
                return
            await asyncio.sleep(min(2 ** attempt, 8))
        except Exception as e:
            logger.error(f"發送給 {uid} 未預期錯誤: {e}")
            return

async def copy_single_with_retry(msg, uid: int):
    await _safe_send(lambda: msg.copy(chat_id=uid), uid)

async def copy_album_with_retry(bot, from_chat_id: int, msg_ids: list, uid: int):
    await _safe_send(
        lambda: bot.copy_messages(
            chat_id=uid, from_chat_id=from_chat_id, message_ids=msg_ids
        ),
        uid,
    )

# ================= Background broadcast worker (consumer) ====================
broadcast_queue: asyncio.Queue | None = None

# How many user-sends to run concurrently inside the worker.
# The token bucket already limits the *rate*; this caps memory / task count.
MAX_CONCURRENT_SENDS = int(os.getenv("MAX_CONCURRENT_SENDS", "50"))

async def broadcast_worker(bot):
    """
    Single background task that drains broadcast_queue.

    Architecture
    ────────────
    Each job contains a full snapshot of the user list.  We fan-out to
    MAX_CONCURRENT_SENDS parallel tasks, each of which goes through the
    shared token bucket, so the aggregate send rate stays ≤ GLOBAL_RATE_LIMIT
    regardless of how many jobs accumulate in the queue.

    Throughput estimate (conservative):
        25 sends/s × 30 s = 750 sends / broadcast window
        With albums (1 API call per user), 1 000 users ≈ 40 s at 25 sends/s.
        Tune GLOBAL_RATE_LIMIT up to 28–29 if you want tighter delivery.
    """
    logger.info("背景廣播工人已啟動！")
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_SENDS)

    async def guarded(coro):
        async with semaphore:
            await coro

    while True:
        try:
            job      = await broadcast_queue.get()
            job_type = job.get("type")
            users    = job.get("users", [])

            if job_type == "single":
                msg   = job["msg"]
                tasks = [
                    asyncio.create_task(
                        guarded(copy_single_with_retry(msg, uid))
                    )
                    for uid in users
                ]

            elif job_type == "album":
                from_chat_id = job["from_chat_id"]
                msg_ids      = job["msg_ids"]
                tasks = [
                    asyncio.create_task(
                        guarded(copy_album_with_retry(bot, from_chat_id, msg_ids, uid))
                    )
                    for uid in users
                ]

            else:
                broadcast_queue.task_done()
                continue

            # Wait for the entire fan-out to complete before pulling the next
            # job, so we never pile up unbounded tasks in memory.
            # If you want pipeline parallelism (overlap jobs), replace with
            # asyncio.gather(*tasks, return_exceptions=True) without await.
            await asyncio.gather(*tasks, return_exceptions=True)
            broadcast_queue.task_done()

        except Exception as e:
            logger.error(f"廣播隊列處理錯誤: {e}")

# ================= Bot command handlers ======================================
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
    user_id       = update.effective_user.id
    users_snapshot = await get_latest_users_snapshot()
    if user_id in USERBOT_LIST or user_id in users_snapshot:
        queue_size = broadcast_queue.qsize() if broadcast_queue else 0
        status_msg = (
            f"🤖 Bot 狀態：運行中\n"
            f"📅 上次轉發媒體：{LAST_FORWARD_TIME}\n"
            f"📦 等待發送的排隊任務數：{queue_size}\n"
            f"⚡ TG車速速限：{GLOBAL_RATE_LIMIT} 條/秒\n\n"
            f"📡 正在跟車的群組：\n"
        )
        if LISTENING_GROUPS_INFO:
            for gid, title in LISTENING_GROUPS_INFO.items():
                status_msg += f"- {title} ({gid})\n"
        else:
            status_msg += "目前沒有跟車中的群組或正在初始化中。\n"
        await update.message.reply_text(status_msg)
    else:
        await update.message.reply_text("請先輸入 /start 啟動機器人。")

# ================= Bot inbox handler (producer) ==============================
ptb_media_cache: dict = {}
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
        await broadcast_queue.put({
            "type":  "single",
            "msg":   msg,
            "users": current_users,
        })
    else:
        async with ptb_cache_lock:
            if gid not in ptb_media_cache:
                ptb_media_cache[gid] = []
                asyncio.create_task(
                    process_ptb_album(msg.chat_id, gid, context.bot)
                )
            ptb_media_cache[gid].append(msg.message_id)

async def process_ptb_album(from_chat_id: int, gid: str, bot):
    await asyncio.sleep(0.8)          # collect all parts of the album
    async with ptb_cache_lock:
        msg_ids = ptb_media_cache.pop(gid, [])
    if not msg_ids:
        return
    msg_ids.sort()
    current_users = await get_latest_users_snapshot()
    await broadcast_queue.put({
        "type":         "album",
        "from_chat_id": from_chat_id,
        "msg_ids":      msg_ids,
        "users":        current_users,
    })

# ================= Userbot listener ==========================================
media_groups_cache: dict = {}
cache_lock = asyncio.Lock()

async def run_userbot(bot_app: Application):
    os.chdir(DATA_DIR)
    session_string = ""
    if os.path.exists(SESSION_TXT_PATH):
        with open(SESSION_TXT_PATH, "r") as f:
            session_string = f.read().strip()

    client = TelegramClient(
        StringSession(session_string), USERBOT_KEY, USERBOT_HASH
    )
    await client.connect()

    if not await client.is_user_authorized():
        logger.error(
            "Userbot 未授權！請將有效 Session 字串存入 userbot_session.txt 後重啟。"
        )
        return

    # Persist refreshed session
    with open(SESSION_TXT_PATH, "w") as f:
        f.write(client.session.save())

    logger.info("Userbot 已成功啟動，正在監聽群組…")

    global LISTENING_GROUPS_INFO
    for group_id in LISTENING_GROUPS:
        try:
            entity = await client.get_entity(group_id)
            LISTENING_GROUPS_INFO[group_id] = getattr(entity, "title", str(group_id))
        except Exception as e:
            logger.error(f"獲取群組 {group_id} 資訊失敗: {e}")
            LISTENING_GROUPS_INFO[group_id] = "未知群組"

    @client.on(events.NewMessage(chats=LISTENING_GROUPS))
    async def handler(event):
        if not event.message.media or event.message.sticker:
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
                    asyncio.create_task(
                        process_media_group(client, media_group_id)
                    )
                media_groups_cache[media_group_id].append(event.message)

    async def process_media_group(client, media_group_id):
        await asyncio.sleep(0.8)
        async with cache_lock:
            messages = media_groups_cache.pop(media_group_id, [])
        if messages:
            messages.sort(key=lambda x: x.id)
            try:
                await client.forward_messages(BOT_USERNAME, messages)
            except Exception as e:
                logger.error(f"Userbot 相冊轉發至 Bot 失敗: {e}")

    await client.run_until_disconnected()

# ================= Application bootstrap =====================================
async def post_init(application: Application):
    global rate_bucket, broadcast_queue
    rate_bucket     = TokenBucket(GLOBAL_RATE_LIMIT)
    broadcast_queue = asyncio.Queue()

    # Single long-running broadcast consumer
    application.create_task(broadcast_worker(application.bot))
    # Userbot listener
    application.create_task(run_userbot(application))

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    bot_app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )
    bot_app.add_handler(CommandHandler("start", start_command))
    bot_app.add_handler(CommandHandler("check", check_command))
    bot_app.add_handler(
        MessageHandler(filters.User(USERBOT_LIST) & filters.ALL, bot_inbox_handler)
    )
    logger.info("Telegram Bot 正在啟動…")
    bot_app.run_polling()

if __name__ == "__main__":
    main()
