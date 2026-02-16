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
  player_id INTEGER,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS players (
  player_id INTEGER PRIMARY KEY AUTOINCREMENT,
  display_name TEXT NOT NULL,
  telegram_user_id INTEGER,
  mafiauniverse_url TEXT,
  visits_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS player_aliases (
  alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
  alias TEXT NOT NULL,
  alias_norm TEXT NOT NULL,
  player_id INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(player_id) REFERENCES players(player_id) ON DELETE CASCADE,
  UNIQUE(player_id, alias_norm)
);

CREATE INDEX IF NOT EXISTS idx_player_aliases_norm ON player_aliases(alias_norm);

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
  last_text TEXT,
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

-- Single-message UI state (private chat)
CREATE TABLE IF NOT EXISTS ui_state (
  user_id INTEGER PRIMARY KEY,
  main_message_id INTEGER,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

-- User registration requests (private -> admin approval)
CREATE TABLE IF NOT EXISTS registration_requests (
  request_id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  requested_nick TEXT NOT NULL,
  requested_nick_norm TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TEXT NOT NULL,
  decided_by INTEGER,
  decided_at TEXT,
  reason TEXT
);

-- Only one pending request per user
CREATE UNIQUE INDEX IF NOT EXISTS idx_reg_pending_user
  ON registration_requests(user_id)
  WHERE status='pending';
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
            # Lightweight migrations for older DBs
            try:
                await db.execute("ALTER TABLE tracked_messages ADD COLUMN last_text TEXT")
            except Exception:
                pass

            # players table migrations
            for stmt in (
                "ALTER TABLE players ADD COLUMN telegram_user_id INTEGER",
                "ALTER TABLE players ADD COLUMN mafiauniverse_url TEXT",
                "ALTER TABLE players ADD COLUMN visits_count INTEGER NOT NULL DEFAULT 0",
            ):
                try:
                    await db.execute(stmt)
                except Exception:
                    pass

            # users table migrations
            try:
                await db.execute("ALTER TABLE users ADD COLUMN player_id INTEGER")
            except Exception:
                pass

            # player_aliases schema migration (old: alias TEXT PRIMARY KEY, player_id)
            try:
                cur = await db.execute("PRAGMA table_info(player_aliases)")
                cols = await cur.fetchall()
                await cur.close()
                col_names = {c[1] for c in cols}  # (cid, name, type, notnull, dflt, pk)
                if "alias_id" not in col_names or "alias_norm" not in col_names:
                    # Migrate to new schema while keeping existing data.
                    await db.execute("ALTER TABLE player_aliases RENAME TO player_aliases_old")
                    await db.execute(
                        """CREATE TABLE player_aliases (
                               alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
                               alias TEXT NOT NULL,
                               alias_norm TEXT NOT NULL,
                               player_id INTEGER NOT NULL,
                               created_at TEXT NOT NULL,
                               FOREIGN KEY(player_id) REFERENCES players(player_id) ON DELETE CASCADE,
                               UNIQUE(player_id, alias_norm)
                             )"""
                    )
                    # old alias column already stored normalized names
                    await db.execute(
                        """INSERT OR IGNORE INTO player_aliases(alias, alias_norm, player_id, created_at)
                             SELECT alias, alias, player_id, COALESCE((SELECT created_at FROM players p WHERE p.player_id = player_aliases_old.player_id), datetime('now'))
                             FROM player_aliases_old"""
                    )
                    await db.execute("DROP TABLE player_aliases_old")
                    await db.execute("CREATE INDEX IF NOT EXISTS idx_player_aliases_norm ON player_aliases(alias_norm)")
            except Exception:
                # If migration fails, keep the bot running; worst case aliases remain old.
                pass
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
