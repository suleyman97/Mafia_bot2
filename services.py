from __future__ import annotations
from typing import Optional, Iterable
from datetime import datetime, timezone
import zoneinfo
import logging
from html import escape as html_escape

from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest

from db import Database
from parser import normalize_name, parse_players_from_post, extract_event_title, beautify_display_name

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

    # --- UI state (single-message UI in private chat) ---
    async def get_main_ui_message_id(self, user_id: int) -> int | None:
        row = await self.db.fetchone("SELECT main_message_id FROM ui_state WHERE user_id=?", (user_id,))
        if not row:
            return None
        mid = row["main_message_id"]
        return int(mid) if mid is not None else None

    async def set_main_ui_message_id(self, user_id: int, message_id: int) -> None:
        await self.db.execute(
            """INSERT INTO ui_state(user_id, main_message_id, updated_at)
               VALUES(?, ?, datetime('now'))
               ON CONFLICT(user_id) DO UPDATE SET
                 main_message_id=excluded.main_message_id,
                 updated_at=datetime('now')""",
            (user_id, message_id),
        )

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

    async def get_user_player_id(self, user_id: int) -> int | None:
        row = await self.db.fetchone("SELECT player_id FROM users WHERE user_id=?", (user_id,))
        if not row:
            return None
        pid = row["player_id"]
        return int(pid) if pid is not None else None

    async def set_user_player_id(self, user_id: int, player_id: int | None) -> None:
        await self.db.execute(
            "UPDATE users SET player_id = ? WHERE user_id = ?",
            (player_id, user_id),
        )

    async def get_pending_registration(self, user_id: int) -> dict | None:
        row = await self.db.fetchone(
            """SELECT request_id, user_id, requested_nick, status, created_at
               FROM registration_requests
               WHERE user_id=? AND status='pending'
               ORDER BY request_id DESC LIMIT 1""",
            (user_id,),
        )
        return dict(row) if row else None

    async def create_or_update_registration(self, user_id: int, nickname: str) -> int:
        nick = beautify_display_name(nickname)
        norm = normalize_name(nick)
        if not norm:
            raise ValueError("Empty nickname")
        created_at = now_iso(self.tz_name)

        # if there is an existing pending request, update it
        row = await self.db.fetchone(
            "SELECT request_id FROM registration_requests WHERE user_id=? AND status='pending'",
            (user_id,),
        )
        if row:
            rid = int(row["request_id"])
            await self.db.execute(
                """UPDATE registration_requests
                   SET requested_nick=?, requested_nick_norm=?, created_at=?
                   WHERE request_id=?""",
                (nick, norm, created_at, rid),
            )
            return rid

        # otherwise create (need same connection for last_insert_rowid)
        async with self.db.connect() as conn:
            await conn.execute(
                """INSERT INTO registration_requests(user_id, requested_nick, requested_nick_norm, status, created_at)
                   VALUES(?,?,?,?,?)""",
                (user_id, nick, norm, "pending", created_at),
            )
            cur = await conn.execute("SELECT last_insert_rowid()")
            rid = int((await cur.fetchone())[0])
            await cur.close()
            await conn.commit()
        return rid

    async def set_registration_status(self, request_id: int, *, status: str, decided_by: int | None = None, reason: str | None = None) -> None:
        await self.db.execute(
            """UPDATE registration_requests
               SET status=?, decided_by=?, decided_at=?, reason=?
               WHERE request_id=?""",
            (status, decided_by, now_iso(self.tz_name), reason, request_id),
        )

    async def get_registration_request(self, request_id: int) -> dict | None:
        row = await self.db.fetchone(
            """SELECT request_id, user_id, requested_nick, requested_nick_norm, status
               FROM registration_requests WHERE request_id=?""",
            (request_id,),
        )
        return dict(row) if row else None

    async def find_player_candidates_by_norm(self, nick_norm: str) -> list[dict]:
        rows = await self.db.fetchall(
            """SELECT p.player_id, p.display_name, p.mafiauniverse_url, p.telegram_user_id
               FROM player_aliases a
               JOIN players p ON p.player_id = a.player_id
               WHERE a.alias_norm = ?
               GROUP BY p.player_id
               ORDER BY p.player_id ASC""",
            (nick_norm,),
        )
        return [dict(r) for r in rows]

    async def ensure_player_alias(self, player_id: int, alias: str) -> None:
        alias = (alias or "").strip()
        if not alias:
            return
        norm = normalize_name(alias)
        if not norm:
            return
        await self.db.execute(
            """INSERT OR IGNORE INTO player_aliases(alias, alias_norm, player_id, created_at)
               VALUES(?,?,?,?)""",
            (alias, norm, player_id, now_iso(self.tz_name)),
        )

    async def link_user_to_player(self, user_id: int, player_id: int) -> None:
        """Link telegram user to a player.

        - sets users.player_id
        - sets players.telegram_user_id
        - clears telegram_user_id on other players previously linked to this user
        """
        # Clear previous player telegram links for this user (if any)
        await self.db.execute(
            "UPDATE players SET telegram_user_id = NULL WHERE telegram_user_id = ? AND player_id <> ?",
            (user_id, player_id),
        )
        await self.db.execute(
            "UPDATE players SET telegram_user_id = ? WHERE player_id = ?",
            (user_id, player_id),
        )
        await self.set_user_player_id(user_id, player_id)

    async def get_player_display_name(self, player_id: int) -> str | None:
        row = await self.db.fetchone("SELECT display_name FROM players WHERE player_id=?", (player_id,))
        return str(row["display_name"]) if row else None

    async def get_player_alias_norms(self, player_id: int) -> set[str]:
        rows = await self.db.fetchall("SELECT alias_norm FROM player_aliases WHERE player_id=?", (player_id,))
        return {str(r["alias_norm"]) for r in rows}

    async def resolve_name_for_post(self, raw_name: str) -> str:
        """Best-effort: map user-typed name to canonical display_name for inserts."""
        norm = normalize_name(raw_name)
        if not norm:
            return beautify_display_name(raw_name)
        row = await self.db.fetchone(
            """SELECT p.display_name
               FROM player_aliases a
               JOIN players p ON p.player_id = a.player_id
               WHERE a.alias_norm = ?
               ORDER BY p.player_id ASC LIMIT 1""",
            (norm,),
        )
        if row and row["display_name"]:
            return str(row["display_name"])
        return beautify_display_name(raw_name)

    async def get_latest_tracked_event(self) -> tuple[int, int, str | None] | None:
        row = await self.db.fetchone(
            """SELECT chat_id, message_id, last_text
               FROM tracked_messages
               ORDER BY updated_at DESC
               LIMIT 1"""
        )
        if not row:
            return None
        return int(row["chat_id"]), int(row["message_id"]), (str(row["last_text"]) if row["last_text"] is not None else None)


    async def deactivate_user(self, user_id: int) -> None:
        """Mark user inactive when they block the bot (to stop repeated send attempts)."""
        await self.db.execute("UPDATE users SET is_active = 0 WHERE user_id = ?", (user_id,))

    async def get_or_create_player(self, display_name: str) -> int:
        alias_norm = normalize_name(display_name)
        if not alias_norm:
            raise ValueError("Empty alias after normalization")

        row = await self.db.fetchone(
            "SELECT player_id FROM player_aliases WHERE alias_norm = ? ORDER BY player_id ASC LIMIT 1",
            (alias_norm,),
        )
        if row:
            return int(row["player_id"])

        created_at = now_iso(self.tz_name)

        async with self.db.connect() as conn:
            conn.row_factory = None
            await conn.execute(
                "INSERT INTO players(display_name, created_at) VALUES(?,?)",
                (display_name, created_at),
            )
            cur = await conn.execute("SELECT last_insert_rowid() as id")
            r = await cur.fetchone()
            player_id = int(r[0])
            await cur.close()

            await conn.execute(
                "INSERT OR IGNORE INTO player_aliases(alias, alias_norm, player_id, created_at) VALUES(?,?,?,?)",
                (display_name, alias_norm, player_id, created_at),
            )
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
               WHERE (? = '' OR a.alias_norm LIKE ?)
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
               WHERE (? = '' OR a.alias_norm LIKE ?)
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

    async def set_tracked_message(self, chat_id: int, message_id: int, *, last_text: str | None = None, cleanup_old: bool = True) -> None:
        """Set (or replace) the tracked message for a chat.

        If cleanup_old=True, old snapshots/logs for the previous message_id are removed.
        """
        row = await self.db.fetchone("SELECT message_id FROM tracked_messages WHERE chat_id = ?", (chat_id,))
        old_mid = int(row["message_id"]) if row else None

        await self.db.execute(
            """INSERT INTO tracked_messages(chat_id, message_id, last_text, updated_at)
               VALUES(?,?,?,?)
               ON CONFLICT(chat_id) DO UPDATE SET
                 message_id=excluded.message_id,
                 last_text=excluded.last_text,
                 updated_at=excluded.updated_at
            """,
            (chat_id, message_id, last_text, now_iso(self.tz_name)),
        )

        if cleanup_old and old_mid and old_mid != message_id:
            await self.db.execute("DELETE FROM event_players WHERE chat_id = ? AND message_id = ?", (chat_id, old_mid))
            await self.db.execute("DELETE FROM notify_log WHERE chat_id = ? AND message_id = ?", (chat_id, old_mid))

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

    async def get_tracked_message(self, chat_id: int) -> tuple[Optional[int], Optional[str]]:
        row = await self.db.fetchone("SELECT message_id, last_text FROM tracked_messages WHERE chat_id = ?", (chat_id,))
        if not row:
            return None, None
        return int(row["message_id"]), (str(row["last_text"]) if row["last_text"] is not None else None)

    async def rollover_tracked_event_to_new_message(self, chat_id: int, new_message_id: int, *, new_last_text: str | None) -> Optional[int]:
        """Switch tracking to a new message while preserving the event snapshot.

        We copy:
        - event_players from old_message_id -> new_message_id
        - notify_log (to keep deduplication stable when repeat_notify=False)
        Then we delete old rows to avoid DB growth.

        Returns old_message_id (if any).
        """
        old_message_id = await self.get_tracked_message_id(chat_id)
        if old_message_id and old_message_id != new_message_id:
            # Copy snapshot
            await self.db.execute(
                """INSERT OR IGNORE INTO event_players(chat_id, message_id, player_id)
                   SELECT chat_id, ?, player_id
                   FROM event_players
                   WHERE chat_id = ? AND message_id = ?
                """,
                (new_message_id, chat_id, old_message_id),
            )

            await self.db.execute(
                """INSERT OR IGNORE INTO notify_log(chat_id, message_id, player_id, user_id, sent_at)
                   SELECT chat_id, ?, player_id, user_id, sent_at
                   FROM notify_log
                   WHERE chat_id = ? AND message_id = ?
                """,
                (new_message_id, chat_id, old_message_id),
            )

        # Switch tracked message WITHOUT deleting old first
        await self.set_tracked_message(chat_id, new_message_id, last_text=new_last_text, cleanup_old=False)

        if old_message_id and old_message_id != new_message_id:
            await self.db.execute("DELETE FROM event_players WHERE chat_id = ? AND message_id = ?", (chat_id, old_message_id))
            await self.db.execute("DELETE FROM notify_log WHERE chat_id = ? AND message_id = ?", (chat_id, old_message_id))

        return old_message_id

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
