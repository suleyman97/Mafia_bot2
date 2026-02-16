from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from db import Database
from services import now_iso


SNAPSHOT_FILENAME = "players.snapshot.json"
PATCH_FILENAME = "players.patch.json"


def atomic_write_json(path: Path, data: Any) -> None:
    """Write JSON atomically: write to temp file and replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


def ensure_patch_file(path: Path) -> None:
    """Create an empty patch file if it doesn't exist (safe to edit by hand)."""
    if path.exists():
        return
    atomic_write_json(
        path,
        {
            "version": 1,
            "players": [],
        },
    )


async def export_players_snapshot(db: Database, *, tz_name: str, out_path: Path) -> None:
    """Export players registry to a human-readable JSON snapshot.

    This snapshot is intended for reading/review only.
    For manual edits use PATCH_FILENAME.
    """
    players_rows = await db.fetchall(
        """SELECT player_id, display_name, telegram_user_id, mafiauniverse_url, visits_count
           FROM players
           ORDER BY display_name COLLATE NOCASE"""
    )
    alias_rows = await db.fetchall(
        """SELECT player_id, alias
           FROM player_aliases
           ORDER BY player_id ASC, alias_id ASC"""
    )

    aliases_by_pid: dict[int, list[str]] = {}
    for r in alias_rows:
        pid = int(r["player_id"])
        aliases_by_pid.setdefault(pid, []).append(str(r["alias"]))

    payload_players: list[dict[str, Any]] = []
    for r in players_rows:
        pid = int(r["player_id"])
        item: dict[str, Any] = {
            "id": pid,
            "nickname": str(r["display_name"]),
            "visits_count": int(r["visits_count"] or 0),
            "aliases": aliases_by_pid.get(pid, []),
        }
        if r["telegram_user_id"] is not None:
            item["telegram_user_id"] = int(r["telegram_user_id"])
        if r["mafiauniverse_url"]:
            item["mafiauniverse_url"] = str(r["mafiauniverse_url"])
        payload_players.append(item)

    data = {
        "version": 1,
        "exported_at": now_iso(tz_name),
        "players": payload_players,
    }
    atomic_write_json(out_path, data)
