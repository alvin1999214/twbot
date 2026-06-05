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
from telethon.tl.types import DocumentAttributeVideo, InputMediaUploadedDocument, InputMediaUploadedPhoto

load_dotenv()

BOT_TOKEN        = os.getenv("BOT_TOKEN")
BOT_USERNAME     = os.getenv("BOT_USERNAME")
USERBOT_KEY      = int(os.getenv("USERBOT_KEY"))
USERBOT_HASH     = os.getenv("USERBOT_HASH")

USERBOT_LIST     = [int(x.strip()) for x in os.getenv("USERBOT_LIST",     "").split(",") if x.strip()]
LISTENING_GROUPS = [int(x.strip()) for x in os.getenv("LISTENING_GROUPS", "").split(",") if x.strip()]
WHITELIST_USERS  = [int(x.strip()) for x in os.getenv("WHITELIST_USERS", "").split(",") if x.strip()]

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
    """
    Remove uid from the persistent user list.
    Intentionally does NOT touch user_send_queues or user_sender_tasks:
    the worker that called us IS the task — it cannot await itself.
    Worker teardown is handled by _sender_worker after this returns.
    """
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
            return True   # success
        except Forbidden:
            # Remove from DB — but do NOT touch tasks/queues here.
            # The worker that called us will self-clean after we return False.
            await remove_blocked_user(uid)
            return False  # signal: user is gone, worker should exit
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
                return True
            await asyncio.sleep(min(2 ** attempt, 8))
        except Exception as e:
            logger.error(f"發送給 {uid} 未預期錯誤: {e}")
            return True

async def copy_single_with_retry(msg, uid: int) -> bool:
    return await _safe_send(lambda: msg.copy(chat_id=uid), uid)

async def copy_album_with_retry(bot, from_chat_id: int, msg_ids: list, uid: int) -> bool:
    return await _safe_send(
        lambda: bot.copy_messages(
            chat_id=uid, from_chat_id=from_chat_id, message_ids=msg_ids
        ),
        uid,
    )

# ================= Two-level broadcast pipeline ===============================
#
#  Level 1 – broadcast_queue  (job queue)
#  ───────────────────────────────────────
#  Each entry is a broadcast job: {type, users, msg | from_chat_id+msg_ids}.
#  The dispatcher pulls jobs instantly and fans them out into per-user slots.
#
#  Level 2 – user_send_queues  (per-user FIFO, size-1 active + unbounded pending)
#  ──────────────────────────────────────────────────────────────────────────────
#  Every user uid owns one asyncio.Queue.  A fixed pool of SENDER_POOL_SIZE
#  persistent coroutines ("sender workers") each own one uid and drain that
#  queue forever.  Because each sender is independent, a FloodWait or slow
#  send for uid-A never blocks uid-B.
#
#  The dispatcher never awaits individual sends — it just enqueues and moves on,
#  so broadcast_queue.get() is always ready for the next job immediately.
#
#  Memory contract
#  ───────────────
#  At most  SENDER_POOL_SIZE × (pending jobs)  send-items live in memory at
#  once.  With 1 000 users and 10 jobs queued that's 10 000 lightweight dicts —
#  well within budget.  The token-bucket still caps total API throughput.

broadcast_queue: asyncio.Queue | None = None

# One persistent coroutine per user.  Raising this above the user count wastes
# nothing; lowering it means some users share a slot and can head-of-line block.
# Default matches a typical deployment; override via env if user count grows.
SENDER_POOL_SIZE = int(os.getenv("SENDER_POOL_SIZE", "1000"))

# Per-user send queues: uid -> asyncio.Queue of send-items
# A send-item is a dict: {type, msg?} or {type, bot, from_chat_id, msg_ids}
user_send_queues: dict[int, asyncio.Queue] = {}

# Stores the Task object for each sender worker so it can be cancelled on cleanup.
user_sender_tasks: dict[int, asyncio.Task] = {}


async def _sender_worker(uid: int, bot):
    """
    One persistent coroutine per user.  Drains that user's send queue.

    Exit paths:
      1. Forbidden  — _safe_send returns False; worker cleans up and returns.
         This is the self-exit path.  We must NOT cancel/await ourselves here.
      2. CancelledError — triggered externally (e.g. future admin command).
         Drain the queue so task_done() counts stay balanced, then exit.
    """
    q = user_send_queues[uid]
    try:
        while True:
            item = await q.get()   # CancelledError surfaces here when idle
            try:
                if item["type"] == "single":
                    user_alive = await copy_single_with_retry(item["msg"], uid)
                elif item["type"] == "album":
                    user_alive = await copy_album_with_retry(
                        bot, item["from_chat_id"], item["msg_ids"], uid
                    )
                else:
                    user_alive = True
            except asyncio.CancelledError:
                raise   # finally: below calls q.task_done() exactly once
            except Exception as e:
                logger.error(f"sender_worker uid={uid} 未預期錯誤: {e}")
                user_alive = True   # unknown error — keep the worker alive
            finally:
                q.task_done()

            if not user_alive:
                # User blocked the bot.  DB already cleaned by remove_blocked_user.
                # Now clean up our own registry entries and exit — never await self.
                user_sender_tasks.pop(uid, None)
                user_send_queues.pop(uid, None)
                logger.info(f"sender_worker uid={uid} 自我終止（用戶已封鎖 Bot）。")
                return
    except asyncio.CancelledError:
        # External cancellation — drain pending items and exit cleanly.
        while not q.empty():
            try:
                q.get_nowait()
                q.task_done()
            except asyncio.QueueEmpty:
                break
        user_sender_tasks.pop(uid, None)
        user_send_queues.pop(uid, None)
        logger.info(f"sender_worker uid={uid} 已停止（外部取消）。")


async def broadcast_dispatcher(bot):
    """
    Level-1 consumer.  Pulls broadcast jobs and immediately distributes
    send-items to every user's per-user queue.  Never awaits a send — returns
    to queue.get() as fast as Python can iterate the user list.
    """
    logger.info("廣播調度器已啟動！")
    while True:
        try:
            job      = await broadcast_queue.get()
            job_type = job.get("type")
            users    = job.get("users", [])

            for uid in users:
                # Lazily create a queue + worker for first-seen users
                if uid not in user_send_queues:
                    user_send_queues[uid] = asyncio.Queue()
                    user_sender_tasks[uid] = asyncio.create_task(_sender_worker(uid, bot))

                if job_type == "single":
                    await user_send_queues[uid].put({
                        "type": "single",
                        "msg":  job["msg"],
                    })
                elif job_type == "album":
                    await user_send_queues[uid].put({
                        "type":         "album",
                        "from_chat_id": job["from_chat_id"],
                        "msg_ids":      job["msg_ids"],
                    })

            broadcast_queue.task_done()

        except Exception as e:
            logger.error(f"廣播調度器錯誤: {e}")

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
        
    if WHITELIST_USERS and user_id not in WHITELIST_USERS:
        await update.message.reply_text("抱歉，您不在白名單內，無法接收轉發的媒體。")
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
        job_queue_size  = broadcast_queue.qsize() if broadcast_queue else 0
        send_queue_size = sum(q.qsize() for q in user_send_queues.values())
        status_msg = (
            f"🤖 Bot 狀態：運行中\n"
            f"📅 上次轉發媒體：{LAST_FORWARD_TIME}\n"
            f"📦 待發送媒體數：{job_queue_size}\n"
#            f"📨 各用戶待發送總計：{send_queue_size}\n"
#            f"👥 已建立發送通道數：{len(user_send_queues)}\n"
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
async def get_video_metadata(video_path: str):
    width, height, duration = 0, 0, 0
    thumb_path = None
    try:
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height,duration",
            "-of", "json", video_path
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await process.communicate()
        if stdout:
            data = json.loads(stdout)
            streams = data.get("streams", [])
            if streams:
                stream = streams[0]
                width = int(stream.get("width", 0))
                height = int(stream.get("height", 0))
                duration = int(float(stream.get("duration", 0)))
        
        thumb_path = video_path + ".jpg"
        cmd2 = [
            "ffmpeg", "-y", "-i", video_path,
            "-ss", "00:00:00.000", "-vframes", "1",
            "-vf", "scale=320:-1",
            thumb_path
        ]
        process2 = await asyncio.create_subprocess_exec(
            *cmd2, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await process2.communicate()
        
        if not os.path.exists(thumb_path):
            thumb_path = None
            
    except Exception as e:
        logger.error(f"提取影片 metadata 失敗: {e}")
        
    return width, height, duration, thumb_path

async def get_custom_caption(message, is_edited: bool) -> str:
    original_text = message.text or ""
    try:
        sender = await message.get_sender()
        sender_id = sender.id if sender else "未知"
        if sender and getattr(sender, 'username', None):
            username = f"@{sender.username}"
        else:
            username = "用戶未設定username"
    except Exception:
        sender_id = "未知"
        username = "用戶未設定username"

    tag = "#已編輯" if is_edited else "#原始圖片"

    addition = f"\n\n發送者ID: {sender_id}\n發送者: {username}\n媒體信息: {tag}"
    return original_text + addition

async def process_restricted_message(client, message, custom_caption: str):
    path = await client.download_media(message, file=DATA_DIR)
    if not path:
        return
    files_to_delete = [path]
    try:
        if message.video or message.gif:
            width, height, duration, thumb_path = await get_video_metadata(path)
            if thumb_path:
                files_to_delete.append(thumb_path)
            
            await client.send_file(
                BOT_USERNAME,
                path,
                caption=custom_caption,
                attributes=[DocumentAttributeVideo(
                    duration=duration,
                    w=width,
                    h=height,
                    supports_streaming=True
                )],
                thumb=thumb_path
            )
        else:
            await client.send_file(BOT_USERNAME, path, caption=custom_caption)
    except Exception as e:
        logger.error(f"Userbot 單一媒體下載轉發失敗: {e}")
    finally:
        for f in files_to_delete:
            if f and os.path.exists(f):
                try:
                    os.remove(f)
                except Exception:
                    pass

async def process_restricted_album(client, messages, custom_caption: str):
    media_list = []
    files_to_delete = []
    
    try:
        for msg in messages:
            path = await client.download_media(msg, file=DATA_DIR)
            if not path:
                continue
            files_to_delete.append(path)
            
            uploaded_file = await client.upload_file(path)
            
            if msg.video or msg.gif:
                width, height, duration, thumb_path = await get_video_metadata(path)
                if thumb_path:
                    files_to_delete.append(thumb_path)
                    uploaded_thumb = await client.upload_file(thumb_path)
                else:
                    uploaded_thumb = None
                    
                mime_type = "video/mp4"
                media_list.append(
                    InputMediaUploadedDocument(
                        file=uploaded_file,
                        mime_type=mime_type,
                        attributes=[DocumentAttributeVideo(
                            duration=duration,
                            w=width,
                            h=height,
                            supports_streaming=True
                        )],
                        thumb=uploaded_thumb,
                        force_file=False
                    )
                )
            elif getattr(msg, 'photo', None):
                media_list.append(
                    InputMediaUploadedPhoto(
                        file=uploaded_file
                    )
                )
            else:
                media_list.append(
                    InputMediaUploadedDocument(
                        file=uploaded_file,
                        mime_type=msg.file.mime_type if getattr(msg, 'file', None) else "application/octet-stream",
                        force_file=True
                    )
                )
                
        if media_list:
            await client.send_file(BOT_USERNAME, media_list, caption=custom_caption)
    except Exception as e:
        logger.error(f"Userbot 相冊下載轉發失敗: {e}")
    finally:
        for f in files_to_delete:
            if f and os.path.exists(f):
                try:
                    os.remove(f)
                except Exception:
                    pass

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
        logger.info("Userbot 未授權！即將進入互動式登入流程...")
        
        async def async_input(prompt: str) -> str:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, input, prompt)
            
        try:
            await client.start(
                phone=lambda: async_input("📱 請輸入手機號碼 (包含國碼，如 +886...): "),
                password=lambda: async_input("🔑 請輸入兩步驗證密碼 (若無請直接 Enter): "),
                code_callback=lambda: async_input("💬 請輸入 Telegram 收到的驗證碼: ")
            )
        except Exception as e:
            logger.error(f"Userbot 登入失敗: {e}")
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

    async def handle_media_event(client, event, is_edited: bool):
        if not event.message.media or event.message.sticker:
            return

        media_group_id = event.message.grouped_id

        if media_group_id is None:
            try:
                chat = await event.get_chat()
                custom_caption = await get_custom_caption(event.message, is_edited)
                if getattr(chat, 'noforwards', False):
                    await process_restricted_message(client, event.message, custom_caption)
                else:
                    await client.send_file(BOT_USERNAME, event.message.media, caption=custom_caption)
            except Exception as e:
                logger.error(f"Userbot 單一媒體轉發至 Bot 失敗: {e}")
        else:
            cache_key = f"{media_group_id}_{'edit' if is_edited else 'new'}"
            async with cache_lock:
                if cache_key not in media_groups_cache:
                    media_groups_cache[cache_key] = []
                    asyncio.create_task(
                        process_media_group(client, cache_key, is_edited)
                    )
                media_groups_cache[cache_key].append(event.message)

    @client.on(events.NewMessage(chats=LISTENING_GROUPS))
    async def new_message_handler(event):
        await handle_media_event(client, event, is_edited=False)

    @client.on(events.MessageEdited(chats=LISTENING_GROUPS))
    async def edited_message_handler(event):
        await handle_media_event(client, event, is_edited=True)

    async def process_media_group(client, cache_key, is_edited):
        await asyncio.sleep(0.8)
        async with cache_lock:
            messages = media_groups_cache.pop(cache_key, [])
        if messages:
            messages.sort(key=lambda x: x.id)
            try:
                chat = await messages[0].get_chat()
                custom_caption = await get_custom_caption(messages[0], is_edited)
                if getattr(chat, 'noforwards', False):
                    await process_restricted_album(client, messages, custom_caption)
                else:
                    media_list = [m.media for m in messages]
                    await client.send_file(BOT_USERNAME, media_list, caption=custom_caption)
            except Exception as e:
                logger.error(f"Userbot 相冊轉發至 Bot 失敗: {e}")

    await client.run_until_disconnected()

# ================= Application bootstrap =====================================
async def post_init(application: Application):
    global rate_bucket, broadcast_queue
    rate_bucket     = TokenBucket(GLOBAL_RATE_LIMIT)
    broadcast_queue = asyncio.Queue()

    # Level-1 dispatcher (non-blocking fan-out to per-user queues)
    application.create_task(broadcast_dispatcher(application.bot))
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