import aiosqlite
from pathlib import Path
from typing import Iterable, Optional, Sequence, Any

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  username TEXT,
  first_name TEXT,
  last_name TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS players (
  player_id INTEGER PRIMARY KEY AUTOINCREMENT,
  display_name TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS player_aliases (
  alias TEXT PRIMARY KEY,
  player_id INTEGER NOT NULL,
  FOREIGN KEY(player_id) REFERENCES players(player_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS subscriptions (
  user_id INTEGER NOT NULL,
  player_id INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(user_id, player_id),
  FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE,
  FOREIGN KEY(player_id) REFERENCES players(player_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tracked_messages (
  chat_id INTEGER PRIMARY KEY,
  message_id INTEGER NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_players (
  chat_id INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  player_id INTEGER NOT NULL,
  PRIMARY KEY(chat_id, message_id, player_id),
  FOREIGN KEY(player_id) REFERENCES players(player_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS notify_log (
  chat_id INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  player_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  sent_at TEXT NOT NULL,
  PRIMARY KEY(chat_id, message_id, player_id, user_id)
);
"""

class Database:
    def __init__(self, path: str):
        self.path = path
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)

    # IMPORTANT: do NOT await connect() before "async with".
    # aiosqlite starts its worker thread when awaited/entered;
    # awaiting twice causes: "threads can only be started once".
    def connect(self) -> aiosqlite.Connection:
        return aiosqlite.connect(self.path)

    async def init(self) -> None:
        async with self.connect() as db:
            db.row_factory = aiosqlite.Row
            await db.executescript(SCHEMA_SQL)
            await db.commit()

    async def fetchone(self, sql: str, params: Sequence[Any] = ()) -> Optional[aiosqlite.Row]:
        async with self.connect() as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(sql, params)
            row = await cur.fetchone()
            await cur.close()
            return row

    async def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[aiosqlite.Row]:
        async with self.connect() as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(sql, params)
            rows = await cur.fetchall()
            await cur.close()
            return rows

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        async with self.connect() as db:
            await db.execute(sql, params)
            await db.commit()

    async def executemany(self, sql: str, seq_params: Iterable[Sequence[Any]]) -> None:
        async with self.connect() as db:
            await db.executemany(sql, seq_params)
            await db.commit()
