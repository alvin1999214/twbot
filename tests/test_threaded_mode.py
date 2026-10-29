"""Offline integration tests. Optional production inputs are copied, never mutated.

TWBOT_TEST_USERS=data/user.json TWBOT_TEST_COMPOSE=docker-compose.yml \
    .venv/bin/python -m unittest discover -s tests -v
"""
import asyncio
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

os.environ.update(BOT_TOKEN="123456:offline-test", USERBOT_KEY="12345",
                  USERBOT_HASH="offline", USERBOT_SESSION="",
                  BOT_USERNAME="test_bot", USERBOT_LIST="999", LISTENING_GROUPS="",
                  DATA_DIR="/tmp/twbot-unused-test-data")
import main as app
from topic_store import TopicStore
from telegram import Bot, Update, User
from telegram.error import BadRequest, Forbidden, RetryAfter, TimedOut
from telegram.request import BaseRequest
from telethon.errors import FloodWaitError

logging.getLogger("main").setLevel(logging.CRITICAL)


class MemoryRequest(BaseRequest):
    """Exercises the installed PTB serializers without any network connection."""
    def __init__(self):
        self.calls = []
        self.next_id = 100

    async def initialize(self):
        pass

    async def shutdown(self):
        pass

    async def do_request(self, url, method, request_data=None, **kwargs):
        method = url.rsplit("/", 1)[-1]
        params = request_data.parameters if request_data else {}
        self.calls.append((method, params))
        self.next_id += 1
        if method == "getMe":
            result = dict(id=123456, is_bot=True, first_name="Test", username="test_bot",
                          has_topics_enabled=True)
        elif method == "createForumTopic":
            result = dict(message_thread_id=self.next_id, name=params["name"], icon_color=7322096)
        elif method == "copyMessage":
            result = dict(message_id=self.next_id)
        elif method == "copyMessages":
            result = [dict(message_id=self.next_id + i) for i in range(len(params["message_ids"]))]
        else:
            raise AssertionError(f"Unexpected API method: {method}")
        return 200, json.dumps(dict(ok=True, result=result)).encode()


class ThreadedTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        app.DATA_DIR = self.tmp.name
        app.USERS_JSON_PATH = str(Path(self.tmp.name) / "user.json")
        self.original = None
        source = os.getenv("TWBOT_TEST_USERS")
        if source:
            self.original = Path(source)
            self.digest = hashlib.sha256(self.original.read_bytes()).digest()
            shutil.copyfile(source, app.USERS_JSON_PATH)
        else:
            Path(app.USERS_JSON_PATH).write_text("[101, 102, 103]")
        self.users = sorted(app.load_normal_users())
        self.groups = [-10011, -10022]
        compose_path = os.getenv("TWBOT_TEST_COMPOSE")
        if compose_path:
            import yaml
            config = yaml.safe_load(Path(compose_path).read_text())
            env = config["services"]["twbot"]["environment"]
            if isinstance(env, list):
                env = dict(item.split("=", 1) for item in env)
            # Deliberately do not load production tokens or sessions into the application.
            self.groups = [int(g.strip()) for g in str(env["LISTENING_GROUPS"]).split(",") if g.strip()]
        app.LISTENING_GROUPS = self.groups
        app.LISTENING_GROUPS_INFO = {gid: f"Group {i}" for i, gid in enumerate(self.groups)}
        app.USERBOT_LIST = [999]
        app.THREADED_MODE = True
        app.FORWARDING_ENABLED = True
        app.users_lock = asyncio.Lock()
        app.ptb_cache_lock = asyncio.Lock()
        app.cache_lock = asyncio.Lock()
        app.topic_locks.clear()
        app.ptb_media_cache.clear()
        app.completed_relays.clear()
        app.broadcast_queue = asyncio.Queue()
        app.rate_bucket = SimpleNamespace(acquire=AsyncMock())
        app.floodwait_until = 0
        app.topic_store = TopicStore(str(Path(self.tmp.name) / "topics.sqlite3"))
        await app.topic_store.initialize()
        self.request = MemoryRequest()
        self.bot = Bot("123456:offline-test", request=self.request, get_updates_request=MemoryRequest())
        await self.bot.initialize()

    async def asyncTearDown(self):
        await app.post_stop(None)
        await self.bot.shutdown()
        if self.original:
            self.assertEqual(self.digest, hashlib.sha256(self.original.read_bytes()).digest())

    def media(self, mid, album=None):
        data = dict(message_id=mid, date=1, chat=dict(id=999, type="private"),
                    photo=[dict(file_id="test", file_unique_id="unique", width=10, height=10)])
        data["from"] = dict(id=999, first_name="Relay", is_bot=False)
        if album:
            data["media_group_id"] = album
        return data

    async def incoming(self, data):
        update = Update.de_json(dict(update_id=1, message=data), self.bot)
        await app.bot_inbox_handler(update, SimpleNamespace(bot=self.bot))

    async def marker(self, media, group=None, count=1):
        control = dict(message_id=9000, date=1, chat=media["chat"],
                       text=f"{app.RELAY_PREFIX} {group or self.groups[0]} {count}",
                       reply_to_message=media)
        control["from"] = media["from"]
        await self.incoming(control)

    def item(self, gid=None, album=False):
        return dict(type="album" if album else "single", from_chat_id=999,
                    msg_ids=[1, 2] if album else [1], source_group_id=gid or self.groups[0])

    async def test_single_requires_source_and_deduplicates(self):
        media = self.media(42)
        await self.incoming(media)
        self.assertTrue(app.broadcast_queue.empty())
        await self.marker(media)
        item = app.broadcast_queue.get_nowait()
        self.assertEqual(item["msg_ids"], [42])
        self.assertEqual(item["source_group_id"], self.groups[0])
        self.assertEqual(set(item["users"]), set(self.users))
        await self.marker(media)
        self.assertTrue(app.broadcast_queue.empty())

    async def test_interleaved_albums_wait_for_all_items(self):
        a, b, c = self.media(10, "a"), self.media(11, "a"), self.media(12, "b")
        await self.marker(b, self.groups[0], count=2)
        await self.incoming(c)
        self.assertTrue(app.broadcast_queue.empty())
        await self.incoming(a)
        first = app.broadcast_queue.get_nowait()
        self.assertEqual(first["msg_ids"], [10, 11])
        self.assertEqual(first["source_group_id"], self.groups[0])
        await self.marker(c, self.groups[1])
        second = app.broadcast_queue.get_nowait()
        self.assertEqual(second["source_group_id"], self.groups[1])

    async def test_untrusted_and_unknown_sources_are_rejected(self):
        media = self.media(4)
        media["from"]["id"] = 998
        await self.marker(media)
        await self.marker(self.media(5), -999999)
        self.assertTrue(app.broadcast_queue.empty())

    async def test_forwarding_disabled(self):
        app.FORWARDING_ENABLED = False
        await self.marker(self.media(1))
        self.assertTrue(app.broadcast_queue.empty())

    async def test_topic_reuse_concurrency_and_restart(self):
        uid, gid = self.users[0], self.groups[0]
        topics = await asyncio.gather(*(app.get_user_topic(self.bot, uid, gid) for _ in range(12)))
        self.assertEqual(len(set(topics)), 1)
        app.topic_store = TopicStore(app.topic_store.path)
        app.topic_locks.clear()
        self.assertEqual(await app.get_user_topic(self.bot, uid, gid), topics[0])
        self.assertEqual(sum(method == "createForumTopic" for method, _ in self.request.calls), 1)

    async def test_actual_ptb_single_and_album_parameters(self):
        uid = self.users[0]
        await app.copy_media_with_retry(self.bot, self.item(), uid)
        await app.copy_media_with_retry(self.bot, self.item(album=True), uid)
        copies = [(m, p) for m, p in self.request.calls if m.startswith("copy")]
        self.assertEqual([m for m, _ in copies], ["copyMessage", "copyMessages"])
        self.assertEqual(copies[0][1]["message_thread_id"], copies[1][1]["message_thread_id"])
        self.assertEqual(copies[1][1]["message_ids"], [1, 2])

    async def test_deleted_topic_is_recreated(self):
        bot = SimpleNamespace(create_forum_topic=AsyncMock(side_effect=[
            SimpleNamespace(message_thread_id=1), SimpleNamespace(message_thread_id=2)]),
            copy_message=AsyncMock(side_effect=[BadRequest("Message thread not found"), object()]))
        await app.copy_media_with_retry(bot, self.item(), self.users[0])
        self.assertEqual(bot.create_forum_topic.await_count, 2)
        self.assertEqual(bot.copy_message.call_args.kwargs["message_thread_id"], 2)

    async def test_unrelated_bad_request_does_not_recreate_topic(self):
        bot = SimpleNamespace(create_forum_topic=AsyncMock(return_value=SimpleNamespace(message_thread_id=1)),
                              copy_message=AsyncMock(side_effect=BadRequest("Message to copy not found")))
        self.assertTrue(await app.copy_media_with_retry(bot, self.item(), self.users[0]))
        self.assertEqual(bot.create_forum_topic.await_count, 1)
        self.assertIn(self.users[0], app.load_normal_users())

    async def test_uncertain_creation_not_repeated_after_restart(self):
        bot = SimpleNamespace(create_forum_topic=AsyncMock(side_effect=TimedOut()))
        for _ in range(2):
            with self.assertRaises(RuntimeError):
                await app.get_user_topic(bot, self.users[0], self.groups[0])
            app.topic_store = TopicStore(app.topic_store.path)
        self.assertEqual(bot.create_forum_topic.await_count, 1)

    async def test_creation_floodwait_retries_and_keeps_user(self):
        bot = SimpleNamespace(create_forum_topic=AsyncMock(side_effect=[RetryAfter(0),
                              SimpleNamespace(message_thread_id=1)]), copy_message=AsyncMock())
        with patch.object(app.asyncio, "sleep", new=AsyncMock()):
            self.assertTrue(await app.copy_media_with_retry(bot, self.item(), self.users[0]))
        self.assertEqual(bot.create_forum_topic.await_count, 2)
        self.assertIn(self.users[0], app.load_normal_users())

    async def test_retry_exhaustion_does_not_remove_user(self):
        with patch.object(app.asyncio, "sleep", new=AsyncMock()):
            self.assertTrue(await app._safe_send(AsyncMock(side_effect=RetryAfter(0)), self.users[0]))
        self.assertIn(self.users[0], app.load_normal_users())

    async def test_forbidden_removes_only_from_test_copy(self):
        self.assertFalse(await app._safe_send(AsyncMock(side_effect=Forbidden("blocked")), self.users[0]))
        self.assertNotIn(self.users[0], app.load_normal_users())
        self.assertEqual(len(app.load_normal_users()), len(self.users) - 1)

    async def test_non_threaded_mode_skips_topic_creation(self):
        app.THREADED_MODE = False
        await app.copy_media_with_retry(self.bot, self.item(), self.users[0])
        self.assertFalse(any(m == "createForumTopic" for m, _ in self.request.calls))
        self.assertNotIn("message_thread_id", self.request.calls[-1][1])

    async def test_startup_rejects_disabled_botfather_mode(self):
        me = User.de_json(dict(id=123, first_name="Bot", is_bot=True, has_topics_enabled=False), self.bot)
        fake = SimpleNamespace(bot=SimpleNamespace(get_me=AsyncMock(return_value=me)))
        with self.assertRaisesRegex(RuntimeError, "BotFather"):
            await app.post_init(fake)
        self.assertFalse(app.bg_tasks)

    async def test_startup_and_shutdown(self):
        async def listener(_):
            await asyncio.Event().wait()
        with patch.object(app, "run_userbot", new=listener):
            await app.post_init(SimpleNamespace(bot=self.bot))
            self.assertEqual(len(app.bg_tasks), 2)
            await app.post_stop(None)
        self.assertFalse(app.bg_tasks)

    async def test_telethon_reply_marker_preserves_source(self):
        forwarded = [SimpleNamespace(id=800), SimpleNamespace(id=801)]
        client = SimpleNamespace(forward_messages=AsyncMock(return_value=forwarded),
                                 send_message=AsyncMock(side_effect=[FloodWaitError(None, 0), None]))
        with patch.object(app.asyncio, "sleep", new=AsyncMock()):
            await app.relay_media(client, [object(), object()], self.groups[0])
        self.assertEqual(client.forward_messages.await_count, 1)
        self.assertEqual(client.send_message.call_args.kwargs["reply_to"], 801)
        self.assertIsNone(client.send_message.call_args.kwargs["parse_mode"])
        self.assertEqual(client.send_message.call_args.args[1], f"{app.RELAY_PREFIX} {self.groups[0]} 2")

    async def test_legacy_user_filename_loads_on_import(self):
        env = dict(os.environ, DATA_DIR=self.tmp.name)
        result = await asyncio.to_thread(subprocess.run, [sys.executable, "-c",
            "import main; print(len(main.normal_users)); print(main.USERS_JSON_PATH.endswith('/user.json'))"],
            env=env, capture_output=True, text=True, check=True)
        self.assertEqual(result.stdout.strip().splitlines(), [str(len(self.users)), "True"])

    async def test_userbot_shutdown_cancels_active_poll(self):
        polling = asyncio.Event()
        reads = 0

        async def get_messages(*args, **kwargs):
            nonlocal reads
            reads += 1
            if reads > len(self.groups):
                polling.set()
                await asyncio.Event().wait()
            return [SimpleNamespace(id=1)]

        async def connected():
            await asyncio.Event().wait()

        client = SimpleNamespace(
            connect=AsyncMock(), disconnect=AsyncMock(),
            is_user_authorized=AsyncMock(return_value=True), get_dialogs=AsyncMock(),
            get_entity=AsyncMock(return_value=SimpleNamespace(title="Test")),
            get_messages=get_messages, send_read_acknowledge=AsyncMock(),
            on=lambda event: lambda handler: handler, is_connected=lambda: True,
            set_receive_updates=AsyncMock(), catch_up=AsyncMock(),
            send_message=AsyncMock(return_value=SimpleNamespace(delete=AsyncMock())),
            run_until_disconnected=connected,
        )
        with patch.object(app, "TelegramClient", return_value=client):
            task = asyncio.create_task(app.run_userbot(SimpleNamespace(bot=self.bot)))
            try:
                await asyncio.wait_for(polling.wait(), 2)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 2)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        client.disconnect.assert_awaited_once()

    async def test_all_users_all_groups_pipeline(self):
        """Full production-shaped fan-out, including real SQLite persistence."""
        task = asyncio.create_task(app.broadcast_dispatcher(self.bot))
        app.bg_tasks.add(task)
        for i, gid in enumerate(self.groups):
            item = self.item(gid, album=bool(i % 2))
            item["users"] = self.users
            await app.broadcast_queue.put(item)
        await asyncio.wait_for(app.broadcast_queue.join(), 10)
        await asyncio.wait_for(asyncio.gather(*(q.join() for q in app.user_send_queues.values())), 180)
        creates = [p for m, p in self.request.calls if m == "createForumTopic"]
        copies = [p for m, p in self.request.calls if m.startswith("copy")]
        expected = len(self.users) * len(self.groups)
        self.assertEqual(len(creates), expected)
        self.assertEqual(len(copies), expected)
        self.assertEqual({p["chat_id"] for p in copies}, set(self.users))
        self.assertEqual(len({(p["chat_id"], p["message_thread_id"]) for p in copies}), expected)
        print(f"Offline fan-out: {len(self.users)} users x {len(self.groups)} groups = {expected} routed deliveries")


if __name__ == "__main__":
    unittest.main()
