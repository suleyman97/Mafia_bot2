from __future__ import annotations
from typing import Optional, Iterable
from datetime import datetime, timezone
import zoneinfo
import logging
from html import escape as html_escape

from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest

from db import Database
from parser import normalize_name, parse_players_from_post, extract_event_title

log = logging.getLogger(__name__)

def now_iso(tz_name: str) -> str:
    try:
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc
    return datetime.now(tz=tz).isoformat(timespec="seconds")

class BotService:
    def __init__(self, db: Database, tz_name: str, repeat_notify: bool = True):
        self.db = db
        self.tz_name = tz_name
        self.repeat_notify = repeat_notify

    async def upsert_user(self, user_id: int, username: Optional[str], first_name: Optional[str], last_name: Optional[str]) -> None:
        created_at = now_iso(self.tz_name)
        await self.db.execute(
            """INSERT INTO users(user_id, username, first_name, last_name, created_at)
               VALUES(?,?,?,?,?)
               ON CONFLICT(user_id) DO UPDATE SET
                 username=excluded.username,
                 first_name=excluded.first_name,
                 last_name=excluded.last_name,
                 is_active=1
            """,
            (user_id, username, first_name, last_name, created_at),
        )


    async def deactivate_user(self, user_id: int) -> None:
        """Mark user inactive when they block the bot (to stop repeated send attempts)."""
        await self.db.execute("UPDATE users SET is_active = 0 WHERE user_id = ?", (user_id,))

    async def get_or_create_player(self, display_name: str) -> int:
        alias = normalize_name(display_name)
        if not alias:
            raise ValueError("Empty alias after normalization")

        row = await self.db.fetchone("SELECT player_id FROM player_aliases WHERE alias = ?", (alias,))
        if row:
            return int(row["player_id"])

        created_at = now_iso(self.tz_name)

        async with self.db.connect() as conn:
            conn.row_factory = None
            await conn.execute("INSERT INTO players(display_name, created_at) VALUES(?,?)", (display_name, created_at))
            cur = await conn.execute("SELECT last_insert_rowid() as id")
            r = await cur.fetchone()
            player_id = int(r[0])
            await cur.close()

            await conn.execute("INSERT INTO player_aliases(alias, player_id) VALUES(?,?)", (alias, player_id))
            await conn.commit()

        return player_id

    async def list_players_page(self, user_id: int, page: int, page_size: int, query: str = "") -> tuple[list[tuple[int,str]], int]:
        """List players for /players, personalized:
        - subscribed players first
        - optional search by alias (case-insensitive via normalize_name)
        """
        if page < 1:
            page = 1

        q_norm = normalize_name(query) if query else ""
        needle = f"%{q_norm}%"

        row = await self.db.fetchone(
            """SELECT COUNT(DISTINCT p.player_id) as c
               FROM players p
               JOIN player_aliases a ON a.player_id = p.player_id
               WHERE (? = '' OR a.alias LIKE ?)
            """,
            (q_norm, needle),
        )
        total = int(row["c"]) if row else 0
        total_pages = max(1, (total + page_size - 1) // page_size)
        if page > total_pages:
            page = total_pages

        offset = (page - 1) * page_size
        rows = await self.db.fetchall(
            """SELECT p.player_id, p.display_name,
                      CASE WHEN s.user_id IS NULL THEN 0 ELSE 1 END AS is_sub
               FROM players p
               JOIN player_aliases a ON a.player_id = p.player_id
               LEFT JOIN subscriptions s ON s.player_id = p.player_id AND s.user_id = ?
               WHERE (? = '' OR a.alias LIKE ?)
               GROUP BY p.player_id
               ORDER BY is_sub DESC, p.display_name COLLATE NOCASE
               LIMIT ? OFFSET ?
            """,
            (user_id, q_norm, needle, page_size, offset),
        )
        data = [(int(r["player_id"]), str(r["display_name"])) for r in rows]
        return data, total_pages

    async def get_subscribed_player_ids(self, user_id: int) -> set[int]:
        rows = await self.db.fetchall("SELECT player_id FROM subscriptions WHERE user_id = ?", (user_id,))
        return {int(r["player_id"]) for r in rows}

    async def subscribe(self, user_id: int, player_id: int) -> None:
        created_at = now_iso(self.tz_name)
        await self.db.execute(
            "INSERT OR IGNORE INTO subscriptions(user_id, player_id, created_at) VALUES(?,?,?)",
            (user_id, player_id, created_at),
        )

    async def unsubscribe(self, user_id: int, player_id: int) -> None:
        await self.db.execute("DELETE FROM subscriptions WHERE user_id = ? AND player_id = ?", (user_id, player_id))

    async def list_subscriptions(self, user_id: int) -> list[tuple[int,str]]:
        rows = await self.db.fetchall(
            """SELECT p.player_id, p.display_name
               FROM subscriptions s
               JOIN players p ON p.player_id = s.player_id
               WHERE s.user_id = ?
               ORDER BY p.display_name COLLATE NOCASE
            """,
            (user_id,),
        )
        return [(int(r["player_id"]), str(r["display_name"])) for r in rows]

    async def set_tracked_message(self, chat_id: int, message_id: int) -> None:
        # If we switch to a new post in the same chat, clear old snapshot/logs
        row = await self.db.fetchone("SELECT message_id FROM tracked_messages WHERE chat_id = ?", (chat_id,))
        if row:
            old_mid = int(row["message_id"])
            if old_mid != message_id:
                await self.db.execute("DELETE FROM event_players WHERE chat_id = ? AND message_id = ?", (chat_id, old_mid))
                await self.db.execute("DELETE FROM notify_log WHERE chat_id = ? AND message_id = ?", (chat_id, old_mid))

        await self.db.execute(
            """INSERT INTO tracked_messages(chat_id, message_id, updated_at)

               VALUES(?,?,?)
               ON CONFLICT(chat_id) DO UPDATE SET
                 message_id=excluded.message_id,
                 updated_at=excluded.updated_at
            """,
            (chat_id, message_id, now_iso(self.tz_name)),
        )

    async def clear_tracked_message(self, chat_id: int) -> None:
        row = await self.db.fetchone("SELECT message_id FROM tracked_messages WHERE chat_id = ?", (chat_id,))
        if row:
            mid = int(row["message_id"])
            await self.db.execute("DELETE FROM event_players WHERE chat_id = ? AND message_id = ?", (chat_id, mid))
            await self.db.execute("DELETE FROM notify_log WHERE chat_id = ? AND message_id = ?", (chat_id, mid))
        await self.db.execute("DELETE FROM tracked_messages WHERE chat_id = ?", (chat_id,))

    async def get_tracked_message_id(self, chat_id: int) -> Optional[int]:
        row = await self.db.fetchone("SELECT message_id FROM tracked_messages WHERE chat_id = ?", (chat_id,))
        return int(row["message_id"]) if row else None

    async def get_event_player_ids(self, chat_id: int, message_id: int) -> set[int]:
        rows = await self.db.fetchall(
            "SELECT player_id FROM event_players WHERE chat_id = ? AND message_id = ?",
            (chat_id, message_id),
        )
        return {int(r["player_id"]) for r in rows}

    async def set_event_players(self, chat_id: int, message_id: int, player_ids: Iterable[int]) -> None:
        """Store a *snapshot* of players for this event post (chat_id+message_id)."""
        params = [(chat_id, message_id, int(pid)) for pid in player_ids]
        async with self.db.connect() as conn:
            await conn.execute("DELETE FROM event_players WHERE chat_id = ? AND message_id = ?", (chat_id, message_id))
            if params:
                await conn.executemany(
                    "INSERT OR IGNORE INTO event_players(chat_id, message_id, player_id) VALUES(?,?,?)",
                    params,
                )
            await conn.commit()


    async def subscribers_for_player(self, player_id: int) -> list[int]:
        rows = await self.db.fetchall(
            """SELECT s.user_id
               FROM subscriptions s
               JOIN users u ON u.user_id = s.user_id
               WHERE s.player_id = ? AND u.is_active = 1
            """,
            (player_id,),
        )
        return [int(r["user_id"]) for r in rows]

    async def was_notified(self, chat_id: int, message_id: int, player_id: int, user_id: int) -> bool:
        row = await self.db.fetchone(
            "SELECT 1 FROM notify_log WHERE chat_id=? AND message_id=? AND player_id=? AND user_id=?",
            (chat_id, message_id, player_id, user_id),
        )
        return bool(row)

    async def mark_notified(self, chat_id: int, message_id: int, player_id: int, user_id: int) -> None:
        await self.db.execute(
            "INSERT OR IGNORE INTO notify_log(chat_id, message_id, player_id, user_id, sent_at) VALUES(?,?,?,?,?)",
            (chat_id, message_id, player_id, user_id, now_iso(self.tz_name)),
        )

    async def update_from_post_and_notify(
        self,
        bot: Bot,
        chat_id: int,
        message_id: int,
        text: str,
        notify_existing: bool = False,
    ) -> tuple[int, int, str]:
        event_title = extract_event_title(text)
        parsed_names = parse_players_from_post(text)

        player_ids: list[int] = []
        for name in parsed_names:
            try:
                pid = await self.get_or_create_player(name)
                player_ids.append(pid)
            except Exception:
                log.exception("Failed to create/resolve player for %r", name)

        current = set(player_ids)
        previous = await self.get_event_player_ids(chat_id, message_id)

        new_ids = current if notify_existing else (current - previous)
        await self.set_event_players(chat_id, message_id, current)

        for pid in new_ids:
            prow = await self.db.fetchone("SELECT display_name FROM players WHERE player_id = ?", (pid,))
            player_name = str(prow["display_name"]) if prow else "Игрок"

            subs = await self.subscribers_for_player(pid)
            if not subs:
                continue

            for uid in subs:
                # In production (repeat_notify=False) we de-duplicate sends using notify_log
                # for BOTH edit-notifies and create-notifies.
                if (not self.repeat_notify) and await self.was_notified(chat_id, message_id, pid, uid):
                    continue

                text_msg = f"📌 <b>{html_escape(event_title)}</b>\nСегодня придёт: <b>{html_escape(player_name)}</b>"
                try:
                    await bot.send_message(uid, text_msg)
                except TelegramForbiddenError:
                    await self.deactivate_user(uid)
                    continue
                except TelegramBadRequest:
                    continue
                except Exception:
                    log.exception("Unexpected error while sending to %s", uid)
                    continue

                await self.mark_notified(chat_id, message_id, pid, uid)

        return (len(current), len(new_ids), event_title)

    async def sync_event_from_post(self, chat_id: int, message_id: int, text: str) -> tuple[int, str]:
        """Parse the post and store current players for this event WITHOUT sending notifications."""
        event_title = extract_event_title(text)
        parsed_names = parse_players_from_post(text)

        player_ids: list[int] = []
        for name in parsed_names:
            try:
                pid = await self.get_or_create_player(name)
                player_ids.append(pid)
            except Exception:
                log.exception("Failed to create/resolve player for %r", name)

        current = set(player_ids)
        await self.set_event_players(chat_id, message_id, current)
        return (len(current), event_title)
