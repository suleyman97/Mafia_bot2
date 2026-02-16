import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

import aiosqlite
from dotenv import load_dotenv


# Allow running as: python scripts/import_players_patch.py
PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from parser import normalize_name  # noqa: E402
from services import now_iso  # noqa: E402
from players_sync import PATCH_FILENAME  # noqa: E402


def _project_dir() -> Path:
    return Path(__file__).resolve().parent.parent


def _resolve_db_path(project_dir: Path, db_path_raw: str) -> str:
    raw = (db_path_raw or "").strip() or "data/bot.db"
    p = Path(raw)
    if not p.is_absolute() and p.parent == Path('.'):
        p = Path('data') / p
    return str((project_dir / p).resolve()) if not p.is_absolute() else str(p)


async def _upsert_player(
    conn: aiosqlite.Connection,
    player: dict[str, Any],
    *,
    tz: str,
    missing_only: bool,
    allow_rename: bool,
    force: bool,
    report: dict[str, int],
) -> None:
    now = now_iso(tz)
    pid = player.get("id")
    nickname = (player.get("nickname") or "").strip()
    tg_id = player.get("telegram_user_id")
    url = (player.get("mafiauniverse_url") or "").strip() or None
    aliases = player.get("aliases") or []
    if not nickname and not pid and not tg_id and not url:
        report["skipped"] += 1
        return

    # Find existing player
    row = None
    if isinstance(pid, int):
        cur = await conn.execute("SELECT * FROM players WHERE player_id = ?", (pid,))
        row = await cur.fetchone()
        await cur.close()
    if row is None and url:
        cur = await conn.execute("SELECT * FROM players WHERE mafiauniverse_url = ?", (url,))
        row = await cur.fetchone()
        await cur.close()
    if row is None and isinstance(tg_id, int):
        cur = await conn.execute("SELECT * FROM players WHERE telegram_user_id = ?", (tg_id,))
        row = await cur.fetchone()
        await cur.close()
    if row is None and nickname:
        cur = await conn.execute("SELECT * FROM players WHERE display_name = ? COLLATE NOCASE", (nickname,))
        row = await cur.fetchone()
        await cur.close()

    if row is None:
        # Create
        if isinstance(pid, int):
            await conn.execute(
                """INSERT INTO players(player_id, display_name, telegram_user_id, mafiauniverse_url, visits_count, created_at)
                   VALUES(?,?,?,?,0,?)""",
                (pid, nickname or f"Player {pid}", tg_id if isinstance(tg_id, int) else None, url, now),
            )
        else:
            await conn.execute(
                """INSERT INTO players(display_name, telegram_user_id, mafiauniverse_url, visits_count, created_at)
                   VALUES(?,?,?,0,?)""",
                (nickname or "Unknown", tg_id if isinstance(tg_id, int) else None, url, now),
            )
            cur = await conn.execute("SELECT last_insert_rowid()")
            pid = int((await cur.fetchone())[0])
            await cur.close()
        report["created"] += 1
        # Ensure main nickname also an alias
        if nickname:
            aliases = list({nickname, *aliases})
    else:
        pid = int(row["player_id"])
        updates: list[str] = []
        params: list[Any] = []

        # telegram_user_id
        if isinstance(tg_id, int):
            cur_val = row["telegram_user_id"]
            if cur_val is None or not missing_only or force:
                if cur_val is None or int(cur_val) == tg_id or force:
                    updates.append("telegram_user_id = ?")
                    params.append(tg_id)
                else:
                    report["conflicts"] += 1

        # mafiauniverse_url
        if url:
            cur_val = row["mafiauniverse_url"]
            if cur_val is None or not missing_only or force:
                if (cur_val is None) or (str(cur_val) == url) or force:
                    updates.append("mafiauniverse_url = ?")
                    params.append(url)
                else:
                    report["conflicts"] += 1

        # nickname rename
        if nickname and allow_rename:
            cur_name = str(row["display_name"]) if row["display_name"] else ""
            if cur_name != nickname and (force or not missing_only):
                updates.append("display_name = ?")
                params.append(nickname)

        if updates:
            params.append(pid)
            await conn.execute(f"UPDATE players SET {', '.join(updates)} WHERE player_id = ?", params)
            report["updated"] += 1

    # Upsert aliases
    if not isinstance(aliases, list):
        aliases = []
    for a in aliases:
        if not isinstance(a, str):
            continue
        a = a.strip()
        if not a:
            continue
        norm = normalize_name(a)
        if not norm:
            continue
        await conn.execute(
            "INSERT OR IGNORE INTO player_aliases(alias, alias_norm, player_id, created_at) VALUES(?,?,?,?)",
            (a, norm, pid, now),
        )
        cur = await conn.execute("SELECT changes()")
        chg = await cur.fetchone()
        await cur.close()
        if chg and int(chg[0]) == 1:
            report["aliases_added"] += 1


async def _run(args: argparse.Namespace) -> None:
    project_dir = _project_dir()
    env_path = project_dir / "data" / ".env"
    load_dotenv(dotenv_path=env_path, override=False)

    db_path = args.db
    if not db_path:
        db_path = _resolve_db_path(project_dir, os.getenv("DB_PATH", "data/bot.db"))

    patch_path = Path(args.patch)
    if not patch_path.is_absolute():
        patch_path = (project_dir / patch_path).resolve()

    raw = patch_path.read_text(encoding="utf-8")
    data = json.loads(raw)
    players = data.get("players") or []

    report = {"created": 0, "updated": 0, "aliases_added": 0, "conflicts": 0, "skipped": 0}

    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA foreign_keys=ON")
        await conn.execute("BEGIN")
        for p in players:
            if not isinstance(p, dict):
                report["skipped"] += 1
                continue
            await _upsert_player(
                conn,
                p,
                tz=args.tz,
                missing_only=not args.merge_all,
                allow_rename=args.allow_rename,
                force=args.force,
                report=report,
            )
        await conn.commit()

    print("Import finished")
    for k, v in report.items():
        print(f"{k}: {v}")


def main() -> None:
    project_dir = _project_dir()
    parser = argparse.ArgumentParser(description="Import players patch JSON into SQLite (merge)")
    parser.add_argument("--patch", default=str((project_dir / "data" / PATCH_FILENAME).resolve()), help="Patch JSON path")
    parser.add_argument("--db", default="", help="SQLite DB path (optional, defaults to DB_PATH in data/.env)")
    parser.add_argument("--tz", default=os.getenv("TZ", "Europe/Moscow"), help="Timezone for timestamps")

    parser.add_argument("--merge-all", action="store_true", help="Update existing fields too (not only missing)")
    parser.add_argument("--allow-rename", action="store_true", help="Allow changing player's nickname")
    parser.add_argument("--force", action="store_true", help="Force overwrite on conflicts")
    args = parser.parse_args()

    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
