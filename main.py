import os
import sys
import json
import uuid
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from telegram import Update, InputMediaPhoto, InputMediaVideo, InputMediaDocument
from telegram.error import Forbidden, NetworkError, RetryAfter, TimedOut
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import (
    DocumentAttributeVideo,
    DocumentAttributeFilename,
    InputMediaUploadedDocument,
    InputMediaUploadedPhoto
)

load_dotenv()

BOT_TOKEN        = os.getenv("BOT_TOKEN")
BOT_USERNAME     = os.getenv("BOT_USERNAME")
USERBOT_KEY      = int(os.getenv("USERBOT_KEY"))
USERBOT_HASH     = os.getenv("USERBOT_HASH")

USERBOT_LIST     = [int(x.strip()) for x in os.getenv("USERBOT_LIST",     "").split(",") if x.strip()]
LISTENING_GROUPS = [int(x.strip()) for x in os.getenv("LISTENING_GROUPS", "").split(",") if x.strip()]
WHITELIST_USERS  = set([int(x.strip()) for x in os.getenv("WHITELIST_USERS", "").split(",") if x.strip()] + USERBOT_LIST)

DATA_DIR          = "/app/data"
USERS_JSON_PATH   = os.path.join(DATA_DIR, "users.json")
SESSION_TXT_PATH  = os.path.join(DATA_DIR, "userbot_session.txt")
MAX_SEND_RETRIES  = int(os.getenv("MAX_SEND_RETRIES",  "3"))
GLOBAL_RATE_LIMIT = int(os.getenv("GLOBAL_RATE_LIMIT", "25"))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

LAST_FORWARD_TIME = "無記錄"
LISTENING_GROUPS_INFO: dict = {}

# ================= Utility Classes & Helpers ==================================
class BoundedDict(dict):
    """限制最大容量的字典，防止映射表造成記憶體外洩"""
    def __init__(self, maxlen=5000):
        self.maxlen = maxlen
        super().__init__()

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if len(self) > self.maxlen:
            self.pop(next(iter(self)))

userbot_msg_map = BoundedDict(5000)   # source_msg_id -> [bot_inbox_msg_id]
ptb_broadcast_map = BoundedDict(5000) # bot_inbox_msg_id -> [(user_id, sent_msg_id)]

class TokenBucket:
    def __init__(self, rate: int):
        self._rate      = rate
        self._tokens    = float(rate)
        self._last_refill = asyncio.get_event_loop().time()
        self._lock      = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now = asyncio.get_event_loop().time()
                elapsed = now - self._last_refill
                self._tokens = min(float(self._rate), self._tokens + elapsed * self._rate)
                self._last_refill = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
            await asyncio.sleep(1.0 / self._rate)

rate_bucket: TokenBucket | None = None
floodwait_until: float = 0.0

async def wait_if_flooded():
    loop = asyncio.get_event_loop()
    remaining = floodwait_until - loop.time()
    if remaining > 0:
        await asyncio.sleep(remaining)

# ================= Data persistence ==========================================
users_lock: asyncio.Lock | None = None

def load_normal_users() -> set:
    if os.path.exists(USERS_JSON_PATH):
        try:
            with open(USERS_JSON_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
            return set([int(uid) for uid in raw if str(uid).isdigit()])
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

normal_users = load_normal_users()

async def get_latest_users_snapshot() -> list:
    global normal_users
    async with users_lock:
        normal_users = load_normal_users()
        return [u for u in normal_users if u in WHITELIST_USERS]

async def add_normal_user(uid: int) -> bool:
    global normal_users
    async with users_lock:
        latest = load_normal_users()
        if uid in latest:
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
    global floodwait_until
    for attempt in range(1, MAX_SEND_RETRIES + 1):
        await wait_if_flooded()
        await rate_bucket.acquire()
        try:
            res = await coro_fn()
            return True, res
        except Forbidden:
            await remove_blocked_user(uid)
            return False, None
        except RetryAfter as e:
            wait_sec = float(getattr(e, "retry_after", 5)) + 1.0
            floodwait_until = max(floodwait_until, asyncio.get_event_loop().time() + wait_sec)
            await asyncio.sleep(wait_sec)
        except (TimedOut, NetworkError) as e:
            if attempt >= MAX_SEND_RETRIES:
                return True, None
            await asyncio.sleep(min(2 ** attempt, 8))
        except Exception as e:
            logger.error(f"發送給 {uid} 未預期錯誤: {e}")
            return True, None
    return True, None

async def copy_single_with_retry(msg, uid: int):
    return await _safe_send(lambda: msg.copy(chat_id=uid), uid)

async def copy_album_with_retry(bot, from_chat_id: int, msg_ids: list, uid: int):
    return await _safe_send(
        lambda: bot.copy_messages(chat_id=uid, from_chat_id=from_chat_id, message_ids=msg_ids),
        uid,
    )

# ================= Two-level broadcast pipeline ===============================
broadcast_queue: asyncio.Queue | None = None
SENDER_POOL_SIZE = int(os.getenv("SENDER_POOL_SIZE", "1000"))
user_send_queues: dict[int, asyncio.Queue] = {}
user_sender_tasks: dict[int, asyncio.Task] = {}

async def _sender_worker(uid: int, bot):
    q = user_send_queues[uid]
    try:
        while True:
            try:
                item = await q.get()
            except asyncio.CancelledError:
                raise
            except RuntimeError as e:
                if "event loop" in str(e): return
                await asyncio.sleep(1)
                continue

            try:
                if item["type"] == "single":
                    user_alive, res = await copy_single_with_retry(item["msg"], uid)
                    if res:
                        curr = ptb_broadcast_map.get(item["bot_msg_id"], [])
                        curr.append((uid, res.message_id))
                        ptb_broadcast_map[item["bot_msg_id"]] = curr

                elif item["type"] == "album":
                    user_alive, res = await copy_album_with_retry(bot, item["from_chat_id"], item["msg_ids"], uid)
                    if res:
                        for i, r in enumerate(res):
                            source_msg_id = item["msg_ids"][i]
                            curr = ptb_broadcast_map.get(source_msg_id, [])
                            curr.append((uid, r.message_id))
                            ptb_broadcast_map[source_msg_id] = curr
                else:
                    user_alive = True
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"sender_worker uid={uid} 未預期錯誤: {e}")
                user_alive = True
                await asyncio.sleep(1)
            finally:
                q.task_done()

            if not user_alive:
                user_sender_tasks.pop(uid, None)
                user_send_queues.pop(uid, None)
                return
    except asyncio.CancelledError:
        while not q.empty():
            try:
                q.get_nowait()
                q.task_done()
            except asyncio.QueueEmpty:
                break
        user_sender_tasks.pop(uid, None)
        user_send_queues.pop(uid, None)

async def broadcast_dispatcher(bot):
    logger.info("廣播調度器已啟動！")
    while True:
        try:
            job      = await broadcast_queue.get()
            job_type = job.get("type")
            users    = job.get("users", [])

            for uid in users:
                if uid not in user_send_queues:
                    user_send_queues[uid] = asyncio.Queue()
                    user_sender_tasks[uid] = asyncio.create_task(_sender_worker(uid, bot))

                if job_type == "single":
                    await user_send_queues[uid].put({"type": "single", "msg": job["msg"], "bot_msg_id": job["msg"].message_id})
                elif job_type == "album":
                    await user_send_queues[uid].put({"type": "album", "from_chat_id": job["from_chat_id"], "msg_ids": job["msg_ids"]})

            broadcast_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(1)

# ================= Bot command handlers ======================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in WHITELIST_USERS:
        await update.message.reply_text("⛔ 您不在授權的白名單內，無法使用此機器人。")
        return

    if user_id in USERBOT_LIST:
        await update.message.reply_text("歡迎回來，Userbot 管理員。\n可用指令：\n/start - 啟動 Bot\n/check - 檢查狀態")
        return
        
    await add_normal_user(user_id)
    await update.message.reply_text("歡迎使用閃圖司機！當有新媒體消息時，您將會同步收到。\n可用指令：\n/start - 啟動機器人\n/check - 檢查狀態")

async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in WHITELIST_USERS: return
        
    job_queue_size  = broadcast_queue.qsize() if broadcast_queue else 0
    status_msg = (
        f"🤖 Bot 狀態：運行中\n📅 上次轉發媒體：{LAST_FORWARD_TIME}\n"
        f"📦 待發送媒體數：{job_queue_size}\n⚡ TG車速速限：{GLOBAL_RATE_LIMIT} 條/秒\n\n📡 正在跟車的群組：\n"
    )
    if LISTENING_GROUPS_INFO:
        for gid, title in LISTENING_GROUPS_INFO.items(): status_msg += f"- {title} ({gid})\n"
    else:
        status_msg += "目前沒有跟車中的群組或正在初始化中。\n"
    await update.message.reply_text(status_msg)

# ================= Bot inbox handler (producer) ==============================
ptb_media_cache: dict = {}
ptb_cache_lock: asyncio.Lock | None = None

async def bot_inbox_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.effective_attachment: return

    global LAST_FORWARD_TIME
    LAST_FORWARD_TIME = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")

    current_users = await get_latest_users_snapshot()
    if not current_users: return

    gid = msg.media_group_id
    if not gid:
        await broadcast_queue.put({"type": "single", "msg": msg, "users": current_users})
    else:
        async with ptb_cache_lock:
            if gid not in ptb_media_cache:
                ptb_media_cache[gid] = []
                asyncio.create_task(process_ptb_album(msg.chat_id, gid, context.bot))
            ptb_media_cache[gid].append(msg.message_id)

async def process_ptb_album(from_chat_id: int, gid: str, bot):
    await asyncio.sleep(0.8)
    async with ptb_cache_lock:
        msg_ids = ptb_media_cache.pop(gid, [])
    if not msg_ids: return
    msg_ids.sort()
    
    current_users = await get_latest_users_snapshot()
    await broadcast_queue.put({"type": "album", "from_chat_id": from_chat_id, "msg_ids": msg_ids, "users": current_users})

# 攔截包含替換檔案的編輯事件並廣播
async def bot_edited_inbox_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.edited_message
    if not msg: return
    bot_msg_id = msg.message_id
    new_caption = msg.caption or msg.text or ""

    if bot_msg_id in ptb_broadcast_map:
        # 建立新的媒體物件 (利用 Telegram 伺服器上已有的 file_id，免除重新上傳)
        media = None
        if msg.photo:
            media = InputMediaPhoto(media=msg.photo[-1].file_id, caption=new_caption)
        elif msg.video:
            media = InputMediaVideo(media=msg.video.file_id, caption=new_caption)
        elif msg.document:
            media = InputMediaDocument(media=msg.document.file_id, caption=new_caption)

        for uid, sent_msg_id in ptb_broadcast_map[bot_msg_id]:
            try:
                if media:
                    # 抽換媒體與文字
                    await context.bot.edit_message_media(chat_id=uid, message_id=sent_msg_id, media=media)
                else:
                    # 僅修改文字
                    await context.bot.edit_message_caption(chat_id=uid, message_id=sent_msg_id, caption=new_caption)
            except Exception:
                pass 

# ================= Media Processing Helpers (ffmpeg) =========================
async def get_video_metadata(file_path: str):
    cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height,duration", "-of", "json", file_path]
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        info = json.loads(stdout.decode())
        stream = info.get('streams', [{}])[0]
        return int(stream.get('width', 0)), int(stream.get('height', 0)), int(float(stream.get('duration', 0)))
    except Exception as e:
        logger.error(f"ffprobe 解析失敗: {e}")
        return 0, 0, 0

async def generate_thumbnail(file_path: str, output_path: str):
    cmd = ["ffmpeg", "-y", "-i", file_path, "-ss", "00:00:00.000", "-vframes", "1", output_path]
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await proc.communicate()
        return os.path.exists(output_path)
    except Exception:
        return False

async def build_caption(msg, is_edit=False):
    """提取原始發送者資訊並建構包含 Hashtag 的 Caption"""
    sender = await msg.get_sender()
    sender_id = sender.id if sender else "未知"
    username = f"@{sender.username}" if sender and getattr(sender, 'username', None) else "無"
    status = "#已編輯" if is_edit else "#原始媒體"
    
    text = msg.text or ""
    info = f"\n\n👤 發送者 ID: {sender_id}\n🔗 發送者: {username}\n📌 狀態: {status}"
    return text + info

# ================= Userbot listener ==========================================
media_groups_cache: dict = {}
cache_lock: asyncio.Lock | None = None

async def run_userbot(bot_app: Application):
    os.chdir(DATA_DIR)
    session_string = ""
    if os.path.exists(SESSION_TXT_PATH):
        with open(SESSION_TXT_PATH, "r") as f: session_string = f.read().strip()

    client = TelegramClient(StringSession(session_string), USERBOT_KEY, USERBOT_HASH)
    await client.connect()

    if not await client.is_user_authorized():
        logger.error("Userbot 未授權！請先使用 docker compose run --rm -it tg_bot 進行登入。")
        await client.disconnect()
        return

    logger.info("Userbot 已成功啟動，正在監聽群組…")

    global LISTENING_GROUPS_INFO
    for group_id in LISTENING_GROUPS:
        try:
            entity = await client.get_entity(group_id)
            LISTENING_GROUPS_INFO[group_id] = getattr(entity, "title", str(group_id))
        except Exception:
            LISTENING_GROUPS_INFO[group_id] = "未知群組"

    @client.on(events.NewMessage(chats=LISTENING_GROUPS))
    async def handler(event):
        if not event.message.media or event.message.sticker: return
        media_group_id = event.message.grouped_id

        if media_group_id is None:
            asyncio.create_task(process_single_message(client, event.message))
        else:
            async with cache_lock:
                if media_group_id not in media_groups_cache:
                    media_groups_cache[media_group_id] = []
                    asyncio.create_task(process_media_group(client, media_group_id))
                media_groups_cache[media_group_id].append(event.message)

    # Userbot 編輯監聽器：判斷是否有新媒體，重新下載後再進行覆蓋
    @client.on(events.MessageEdited(chats=LISTENING_GROUPS))
    async def userbot_edit_handler(event):
        source_id = event.id
        if source_id in userbot_msg_map:
            sent_ids = userbot_msg_map[source_id]
            for s_id in sent_ids:
                new_caption = await build_caption(event.message, is_edit=True)
                
                # 若編輯後包含新的媒體內容，則重新下載
                if event.message.media and not event.message.sticker:
                    is_video = bool(event.message.video)
                    tmp_id = uuid.uuid4().hex
                    ext = ".mp4" if is_video else ".jpg"
                    file_path = f"/tmp/dl_edit_{tmp_id}{ext}"
                    thumb_path = f"/tmp/thumb_edit_{tmp_id}.jpg"
                    
                    try:
                        await client.download_media(event.message, file=file_path)
                        if is_video:
                            w, h, d = await get_video_metadata(file_path)
                            has_thumb = await generate_thumbnail(file_path, thumb_path)
                            await client.edit_message(
                                BOT_USERNAME, s_id, text=new_caption, file=file_path,
                                thumb=thumb_path if has_thumb else None,
                                attributes=[DocumentAttributeVideo(duration=d, w=w, h=h)]
                            )
                        else:
                            await client.edit_message(BOT_USERNAME, s_id, text=new_caption, file=file_path)
                    except Exception as e:
                        logger.error(f"Userbot 替換媒體失敗: {e}")
                    finally:
                        # 嚴格保證本地檔案刪除
                        if os.path.exists(file_path): os.remove(file_path)
                        if os.path.exists(thumb_path): os.remove(thumb_path)
                else:
                    try:
                        # 單純修改文字
                        await client.edit_message(BOT_USERNAME, s_id, text=new_caption)
                    except Exception as e:
                        logger.error(f"Userbot 修改文字失敗: {e}")

    async def process_single_message(client, msg):
        is_video, is_photo = bool(msg.video), bool(msg.photo)
        if not (is_video or is_photo): return

        tmp_id = uuid.uuid4().hex
        ext = ".mp4" if is_video else ".jpg"
        file_path, thumb_path = f"/tmp/dl_{tmp_id}{ext}", f"/tmp/thumb_{tmp_id}.jpg"

        try:
            await client.download_media(msg, file=file_path)
            caption = await build_caption(msg, is_edit=False)
            
            if is_video:
                w, h, d = await get_video_metadata(file_path)
                has_thumb = await generate_thumbnail(file_path, thumb_path)
                sent = await client.send_file(
                    BOT_USERNAME, file=file_path, thumb=thumb_path if has_thumb else None,
                    attributes=[DocumentAttributeVideo(duration=d, w=w, h=h)], caption=caption
                )
            else:
                sent = await client.send_file(BOT_USERNAME, file=file_path, caption=caption)
            
            userbot_msg_map[msg.id] = [sent.id]
        except Exception as e:
            logger.error(f"單一媒體處理失敗: {e}")
        finally:
            # 嚴格保證本地檔案刪除
            if os.path.exists(file_path): os.remove(file_path)
            if os.path.exists(thumb_path): os.remove(thumb_path)

    async def process_media_group(client, media_group_id):
        await asyncio.sleep(0.8)
        async with cache_lock:
            messages = media_groups_cache.pop(media_group_id, [])
        if not messages: return
        messages.sort(key=lambda x: x.id)
        
        media_files, caption = [], ""
        for msg in messages:
            if msg.text and not caption:
                caption = await build_caption(msg, is_edit=False)
            is_video, is_photo = bool(msg.video), bool(msg.photo)
            if not (is_video or is_photo): continue
            
            tmp_id = uuid.uuid4().hex
            ext = ".mp4" if is_video else ".jpg"
            file_path, thumb_path = f"/tmp/dl_{tmp_id}{ext}", f"/tmp/thumb_{tmp_id}.jpg"
            try:
                await client.download_media(msg, file=file_path)
                media_files.append({'is_video': is_video, 'file_path': file_path, 'thumb_path': thumb_path})
            except Exception as e:
                logger.error(f"相冊檔案下載失敗: {e}")

        if not caption and messages:
            caption = await build_caption(messages[0], is_edit=False)

        if not media_files: return
        try:
            input_media = []
            for item in media_files:
                up_file = await client.upload_file(item['file_path'])
                if item['is_video']:
                    w, h, d = await get_video_metadata(item['file_path'])
                    has_thumb = await generate_thumbnail(item['file_path'], item['thumb_path'])
                    up_thumb = await client.upload_file(item['thumb_path']) if has_thumb else None
                    input_media.append(InputMediaUploadedDocument(
                        file=up_file, thumb=up_thumb, mime_type="video/mp4",
                        attributes=[DocumentAttributeVideo(duration=d, w=w, h=h), DocumentAttributeFilename("video.mp4")]
                    ))
                else:
                    input_media.append(InputMediaUploadedPhoto(file=up_file))
                    
            if input_media:
                sent = await client.send_file(BOT_USERNAME, file=input_media, caption=caption)
                # 確保映射時每一張圖對應正確的單一發送 ID，否則編輯時會誤覆蓋整個相簿
                if isinstance(sent, list):
                    for idx, s in enumerate(sent):
                        if idx < len(messages):
                            userbot_msg_map[messages[idx].id] = [s.id]
                else:
                    for m in messages:
                        userbot_msg_map[m.id] = [sent.id]

        except Exception as e:
            logger.error(f"相冊轉發失敗: {e}")
        finally:
            # 嚴格保證本地檔案刪除
            for item in media_files:
                if os.path.exists(item['file_path']): os.remove(item['file_path'])
                if os.path.exists(item['thumb_path']): os.remove(item['thumb_path'])

    await client.run_until_disconnected()

# ================= Application bootstrap =====================================
def setup_userbot():
    os.makedirs(DATA_DIR, exist_ok=True)
    session_str = ""
    if os.path.exists(SESSION_TXT_PATH):
        with open(SESSION_TXT_PATH, "r") as f:
            session_str = f.read().strip()

    async def _do_auth():
        client = TelegramClient(StringSession(session_str), USERBOT_KEY, USERBOT_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            print("\n" + "="*50)
            print("⚠️  Userbot 尚未授權，進入互動登入模式  ⚠️")
            print("請依照提示輸入手機號碼與驗證碼")
            print("="*50 + "\n")
            await client.start()
            print("\n✅ Userbot 授權成功！正在儲存 Session...\n")
            with open(SESSION_TXT_PATH, "w") as f: f.write(client.session.save())
        await client.disconnect()

    try: asyncio.run(_do_auth())
    except EOFError: sys.exit(1)
    except Exception: sys.exit(1)

bg_tasks = set()

async def post_init(application: Application):
    global rate_bucket, broadcast_queue, users_lock, ptb_cache_lock, cache_lock, bg_tasks
    rate_bucket     = TokenBucket(GLOBAL_RATE_LIMIT)
    broadcast_queue = asyncio.Queue()
    users_lock      = asyncio.Lock()
    ptb_cache_lock  = asyncio.Lock()
    cache_lock      = asyncio.Lock()

    dispatcher_task = asyncio.create_task(broadcast_dispatcher(application.bot))
    userbot_task = asyncio.create_task(run_userbot(application))

    bg_tasks.add(dispatcher_task)
    bg_tasks.add(userbot_task)
    dispatcher_task.add_done_callback(bg_tasks.discard)
    userbot_task.add_done_callback(bg_tasks.discard)

def main():
    setup_userbot()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    bot_app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    bot_app.add_handler(CommandHandler("start", start_command))
    bot_app.add_handler(CommandHandler("check", check_command))
    bot_app.add_handler(MessageHandler(filters.User(USERBOT_LIST) & ~filters.UpdateType.EDITED, bot_inbox_handler))
    bot_app.add_handler(MessageHandler(filters.User(USERBOT_LIST) & filters.UpdateType.EDITED, bot_edited_inbox_handler))
    
    logger.info("Telegram Bot 正在啟動…")
    bot_app.run_polling()

if __name__ == "__main__":
    main()