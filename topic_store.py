"""Durable per-user topic mappings; SQLite work stays off the event loop."""
import asyncio
import sqlite3
from contextlib import closing
from pathlib import Path


class TopicStore:
    def __init__(self, path):
        self.path = path

    def _execute(self, sql, params=()):
        with closing(sqlite3.connect(self.path, timeout=30)) as db:
            with db:
                return db.execute(sql, params).fetchone()

    async def initialize(self):
        await asyncio.to_thread(Path(self.path).parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(self._execute, """
            CREATE TABLE IF NOT EXISTS topics (
                user_id INTEGER NOT NULL,
                group_id INTEGER NOT NULL,
                topic_id INTEGER,
                PRIMARY KEY (user_id, group_id)
            )
        """)

    async def get(self, uid, gid):
        # None means absent; (None,) means a creation request may have succeeded.
        return await asyncio.to_thread(
            self._execute,
            "SELECT topic_id FROM topics WHERE user_id=? AND group_id=?", (uid, gid),
        )

    async def set(self, uid, gid, topic_id):
        await asyncio.to_thread(
            self._execute,
            "INSERT OR REPLACE INTO topics VALUES (?, ?, ?)", (uid, gid, topic_id),
        )

    async def delete(self, uid, gid):
        await asyncio.to_thread(
            self._execute,
            "DELETE FROM topics WHERE user_id=? AND group_id=?", (uid, gid),
        )
