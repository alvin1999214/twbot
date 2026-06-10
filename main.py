import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import Forbidden, NetworkError, RetryAfter, TimedOut
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
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

GLOBAL_RATE_LIMIT = int(os.getenv("GLOBAL_RATE_LIMIT", "25"))
USERBOT_RECONNECT_DELAY = int(os.getenv("USERBOT_RECONNECT_DELAY", "5"))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

LAST_FORWARD_TIME    = "無記錄"
LISTENING_GROUPS_INFO: dict = {}
FORWARDING_ENABLED   = True

# ================= Token-Bucket Rate Limiter ==================================
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
                self._tokens = min(
                    float(self._rate),
                    self._tokens + elapsed * self._rate
                )
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
# 移除全局直接實例化的 Lock，改為 None
users_lock: asyncio.Lock | None = None

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
    global floodwait_until
    for attempt in range(1, MAX_SEND_RETRIES + 1):
        await wait_if_flooded()
        await rate_bucket.acquire()
        try:
            await coro_fn()
            return True
        except Forbidden:
            await remove_blocked_user(uid)
            return False
        except RetryAfter as e:
            wait_sec = float(getattr(e, "retry_after", 5)) + 1.0
            logger.warning(
                f"FloodWait {wait_sec:.1f}s triggered while sending to {uid} — pausing ALL sends."
            )
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

async def send_text_with_retry(bot, text: str, uid: int) -> bool:
    return await _safe_send(lambda: bot.send_message(chat_id=uid, text=text), uid)

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
                    user_alive = await copy_single_with_retry(item["msg"], uid)
                elif item["type"] == "album":
                    user_alive = await copy_album_with_retry(
                        bot, item["from_chat_id"], item["msg_ids"], uid
                    )
                elif item["type"] == "text":
                    user_alive = await send_text_with_retry(bot, item["text"], uid)
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
                logger.info(f"sender_worker uid={uid} 自我終止（用戶已封鎖 Bot）。")
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
        logger.info(f"sender_worker uid={uid} 已停止（外部取消）。")

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
                    await user_send_queues[uid].put({"type": "single", "msg": job["msg"]})
                elif job_type == "album":
                    await user_send_queues[uid].put({
                        "type":         "album",
                        "from_chat_id": job["from_chat_id"],
                        "msg_ids":      job["msg_ids"],
                    })
                elif job_type == "text":
                    await user_send_queues[uid].put({"type": "text", "text": job["text"]})

            broadcast_queue.task_done()
        except asyncio.CancelledError:
            logger.info("廣播調度器已停止。")
            break
        except RuntimeError as e:
            if "event loop" in str(e):
                break
            logger.error(f"廣播調度器 RuntimeError: {e}")
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"廣播調度器錯誤: {e}")
            await asyncio.sleep(1)

# ================= Bot command handlers ======================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in USERBOT_LIST:
        await update.message.reply_text(
            "歡迎回來，Userbot 管理員。\n\n可用指令：\n/start - 啟動 Bot\n/check - 檢查狀態"
        )
        return
    added = await add_normal_user(user_id)
    if added:
        logger.info(f"新普通用戶加入: {user_id}")
    await update.message.reply_text(
        "歡迎使用閃圖司機！當有新媒體消息時，您將會同步收到，自動跟車。\n\n可用指令：\n/start - 啟動機器人\n/check - 檢查狀態"
    )

async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id       = update.effective_user.id
    users_snapshot = await get_latest_users_snapshot()
    if user_id in USERBOT_LIST or user_id in users_snapshot:
        job_queue_size  = broadcast_queue.qsize() if broadcast_queue else 0
        status_msg = (
            f"🤖 Bot 狀態：運行中\n"
            f"🔄 轉發狀態：{'🟢 開啟' if FORWARDING_ENABLED else '🔴 關閉'}\n"
            f"📅 上次轉發媒體：{LAST_FORWARD_TIME}\n"
            f"📦 待發送媒體數：{job_queue_size}\n"
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

async def tell_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    # 驗證是否為管理員
    if user_id not in USERBOT_LIST:
        await update.message.reply_text("無權限使用此指令。")
        return

    text = update.message.text or ""
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("請在指令後方輸入要廣播的文字，例如：/tell 早上好")
        return
        
    text_to_send = parts[1].strip()
    
    current_users = await get_latest_users_snapshot()
    if not current_users:
        await update.message.reply_text("目前沒有普通用戶可以發送。")
        return
        
    await broadcast_queue.put({
        "type": "text",
        "text": text_to_send,
        "users": current_users
    })
    
    await update.message.reply_text(f"✅ 已將訊息加入廣播佇列，預計發送給 {len(current_users)} 位用戶。")

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in USERBOT_LIST:
        await update.message.reply_text("無權限使用此指令。")
        return

    status_text = "🟢 開啟" if FORWARDING_ENABLED else "🔴 關閉"
    keyboard = [
        [InlineKeyboardButton(f"切換轉發狀態 (目前: {status_text})", callback_data="toggle_forward")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("🛠️ 管理員控制面板", reply_markup=reply_markup)

async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.from_user.id not in USERBOT_LIST:
        await query.answer("無權限操作。", show_alert=True)
        return

    if query.data == "toggle_forward":
        global FORWARDING_ENABLED
        FORWARDING_ENABLED = not FORWARDING_ENABLED
        
        status_text = "🟢 開啟" if FORWARDING_ENABLED else "🔴 關閉"
        keyboard = [
            [InlineKeyboardButton(f"切換轉發狀態 (目前: {status_text})", callback_data="toggle_forward")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_reply_markup(reply_markup=reply_markup)
        await query.answer(f"轉發狀態已更改為: {'開啟' if FORWARDING_ENABLED else '關閉'}")

# ================= Bot inbox handler (producer) ==============================
ptb_media_cache: dict = {}
ptb_cache_lock: asyncio.Lock | None = None

async def bot_inbox_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.effective_attachment:
        return

    global LAST_FORWARD_TIME, FORWARDING_ENABLED
    if not FORWARDING_ENABLED:
        return
        
    hkt_tz = timezone(timedelta(hours=8))
    LAST_FORWARD_TIME = datetime.now(hkt_tz).strftime("%Y-%m-%d %H:%M:%S")

    current_users = await get_latest_users_snapshot()
    if not current_users:
        return

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
cache_lock: asyncio.Lock | None = None

async def run_userbot(bot_app: Application):
    os.chdir(DATA_DIR)
    global LISTENING_GROUPS_INFO

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

    while True:
        session_string = ""
        if os.path.exists(SESSION_TXT_PATH):
            with open(SESSION_TXT_PATH, "r", encoding="utf-8") as f:
                session_string = f.read().strip()

        client = TelegramClient(
            StringSession(session_string),
            USERBOT_KEY,
            USERBOT_HASH,
            catch_up=True,
        )

        try:
            await client.connect()

            if not await client.is_user_authorized():
                logger.error("Userbot 失效或尚未授權！請先使用 docker compose run --rm -it tg_bot 進行互動登入。")
                return

            logger.info("Userbot 已連線，正在初始化監聽群組…")

            # 先同步常用對話與更新狀態，再註冊事件處理器並補抓離線更新。
            try:
                await client.get_dialogs(limit=10)
            except Exception as e:
                logger.warning(f"Userbot 同步對話列表失敗: {e}")

            valid_listening_groups = []
            for group_id in LISTENING_GROUPS:
                try:
                    entity = await client.get_entity(group_id)
                    LISTENING_GROUPS_INFO[group_id] = getattr(entity, "title", str(group_id))
                    valid_listening_groups.append(group_id)
                except Exception as e:
                    logger.error(f"獲取群組 {group_id} 資訊失敗: {e}")
                    LISTENING_GROUPS_INFO[group_id] = "未知群組"

            if valid_listening_groups:
                @client.on(events.NewMessage(chats=valid_listening_groups))
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
                                asyncio.create_task(process_media_group(client, media_group_id))
                            media_groups_cache[media_group_id].append(event.message)
            else:
                logger.warning("無有效的監聽群組，Userbot 暫時不會監聽任何新訊息。")

            try:
                await client.set_receive_updates(True)
            except Exception as e:
                logger.warning(f"Userbot 無法明確開啟更新接收: {e}")

            try:
                await client.catch_up()
            except Exception as e:
                logger.warning(f"Userbot 補抓離線更新失敗: {e}")

            logger.info("Userbot 已成功啟動，正在監聽群組…")
            
            async def keep_alive():
                """定期發送並刪除訊息至 Saved Messages ('me') 以維持帳號活躍度"""
                while client.is_connected():
                    try:
                        msg = await client.send_message("me", "keep-alive")
                        if msg:
                            await msg.delete()
                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        logger.debug(f"Userbot 活躍度維持任務發生錯誤: {e}")
                    await asyncio.sleep(300)  # 每 5 分鐘執行一次

            keep_alive_task = asyncio.create_task(keep_alive())
            try:
                await client.run_until_disconnected()
            finally:
                keep_alive_task.cancel()
                
            logger.warning("Userbot 連線已結束，%s 秒後嘗試重連…", USERBOT_RECONNECT_DELAY)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Userbot 發生錯誤，%s 秒後重連: %s", USERBOT_RECONNECT_DELAY, e)
        finally:
            try:
                if client.session is not None:
                    with open(SESSION_TXT_PATH, "w", encoding="utf-8") as f:
                        f.write(client.session.save())
            except Exception as e:
                logger.warning(f"儲存 Userbot session 失敗: {e}")
            try:
                await client.disconnect()
            except Exception:
                pass

        await asyncio.sleep(USERBOT_RECONNECT_DELAY)

# ================= Application bootstrap =====================================

def setup_userbot():
    """
    預先互動式授權流程，在進入 PTB 主程序前獨立完成。
    """
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
            print("請依照提示輸入手機號碼與驗證碼 (如果有 2FA 也會要求輸入)")
            print("="*50 + "\n")
            await client.start()
            print("\n✅ Userbot 授權成功！正在儲存 Session...\n")
            with open(SESSION_TXT_PATH, "w") as f:
                f.write(client.session.save())
        await client.disconnect()

    try:
        asyncio.run(_do_auth())
    except EOFError:
        print("\n❌ 錯誤：無法讀取終端機輸入。請確保執行時加上 -it 參數 (例如：docker compose run --rm -it tg_bot)")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Userbot 登入發生未預期錯誤: {e}")
        sys.exit(1)

bg_tasks = set()

async def post_init(application: Application):
    global rate_bucket, broadcast_queue, users_lock, ptb_cache_lock, cache_lock, bg_tasks
    rate_bucket     = TokenBucket(GLOBAL_RATE_LIMIT)
    broadcast_queue = asyncio.Queue()
    
    # 延遲到 Event Loop 確立後才初始化 Locks
    users_lock      = asyncio.Lock()
    ptb_cache_lock  = asyncio.Lock()
    cache_lock      = asyncio.Lock()

    # 使用 asyncio.create_task 建立任務，並保存到 bg_tasks 中
    dispatcher_task = asyncio.create_task(broadcast_dispatcher(application.bot))
    userbot_task = asyncio.create_task(run_userbot(application))

    bg_tasks.add(dispatcher_task)
    bg_tasks.add(userbot_task)

    # 當任務意外結束時，自動將它從集合中移除
    dispatcher_task.add_done_callback(bg_tasks.discard)
    userbot_task.add_done_callback(bg_tasks.discard)

def main():
    # 啟動前強制檢查並處理 Userbot 授權
    setup_userbot()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    bot_app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )
    bot_app.add_handler(CommandHandler("start", start_command))
    bot_app.add_handler(CommandHandler("check", check_command))
    bot_app.add_handler(CommandHandler("tell", tell_command))
    bot_app.add_handler(CommandHandler("admin", admin_command))
    bot_app.add_handler(CallbackQueryHandler(admin_callback))
    bot_app.add_handler(
        MessageHandler(filters.User(USERBOT_LIST) & filters.ALL, bot_inbox_handler)
    )
    logger.info("Telegram Bot 正在啟動…")
    bot_app.run_polling()

if __name__ == "__main__":
    main()