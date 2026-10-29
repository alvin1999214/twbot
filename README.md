# twbot

Listen to Telegram groups through the existing Telethon user session and copy
media to subscribers. Each source group gets a separate topic in each user's
private chat with the bot.

## Deploy

1. Enable **Threaded Mode** for the bot in BotFather. Startup checks the
   `getMe.has_topics_enabled` flag and stops with an actionable error if disabled.
2. Keep the existing `./data:/app/data` volume and make a backup before deployment.
   Both `data/users.json` and the legacy `data/user.json` list format are supported;
   if both exist, `users.json` takes precedence. The application writes to the
   selected file; no subscriber migration is required.
3. Stop the old process before starting the new one, so only one instance uses
   the bot token, Telethon session and data directory.
4. Run `docker compose up -d --build`, then `docker compose logs -f twbot`.
   The container's UID is **1001**; the mounted data directory must be writable by
   that UID, including for creation of the SQLite journal.

`THREADED_MODE` defaults to `true`. Set it to `false` to deliver to the ordinary
private chat without creating topics. `DATA_DIR` defaults to `/app/data` and can
be overridden for local runs. Existing dependency versions are retained: PTB
21.1.1 supports the required methods and preserves the new `getMe` field in
`api_kwargs`.

## Routing and persistence

The userbot forwards media as before, then replies to the forwarded message with
an internal source marker. Telegram translates this reply into the bot's own
message ID namespace. For an album, the marker includes its expected size; the
bot waits for every item before dispatching. Only markers from `USERBOT_LIST`,
in private chats, for configured `LISTENING_GROUPS` are accepted. Unmarked manual
media is not broadcast because its source group is unknown. `/tell` remains an
ordinary text broadcast and does not target a group topic.

Topics are created lazily, named using the source group's title, and stored as
`(user_id, group_id, topic_id)` in `data/topics.sqlite3`. Preserve this file across
deployments to reuse topics. New groups get new topics; existing topics are not
automatically renamed or deleted when the configuration changes. A deleted
target topic is recreated on a definitive “message thread not found” response.
Other API errors are logged; content is never redirected into a different topic
as an error fallback.

Topic creation is not idempotent. If a network timeout or process crash leaves
the outcome unknown, a row with `topic_id IS NULL` blocks further creation for
that user/group, avoiding duplicate topics. Stop the process and reconcile the
row against the actual chat: set its confirmed topic ID, or delete that specific
row only after confirming no topic was created. Restart afterward. Do not delete
the entire database to retry one ambiguous request.

The media broadcast queues remain in memory, as in the existing implementation.
Interrupted relays or queued broadcasts are not guaranteed to replay after a
restart. Incomplete relay entries expire after ten minutes and are logged when
the next relay is processed. Topic persistence is not a durable message queue.

## Offline validation with production data

```sh
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -r requirements-test.txt
TWBOT_TEST_USERS=data/user.json TWBOT_TEST_COMPOSE=docker-compose.yml \
  .venv/bin/python -m unittest discover -s tests -v
```

These tests copy the subscriber file into temporary directories and read only
the group IDs from Compose. Production tokens and sessions are not loaded into
the test application. Bot API requests use an in-memory transport with the
installed PTB serializers. Tests exercise all subscribers and configured groups,
source correlation, album ordering, restart persistence, concurrent topic
creation, missing topics, rate limits, blocked users, startup and shutdown.
They also check that the original subscriber file's SHA-256 remains unchanged.

Without the two `TWBOT_TEST_*` variables, the suite uses synthetic fixtures.
An offline pass does not verify live MTProto connectivity, source-group access
or delivery in Telegram clients. Live delivery validation requires a dedicated
test bot/user session and a test recipient, or a deliberately scoped production
canary; do not run a second production polling process for a smoke test.
