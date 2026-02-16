import argparse
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Allow running as: python scripts/export_players_snapshot.py
PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from db import Database  # noqa: E402
from players_sync import export_players_snapshot, SNAPSHOT_FILENAME  # noqa: E402


def _project_dir() -> Path:
    return Path(__file__).resolve().parent.parent


def _resolve_db_path(project_dir: Path, db_path_raw: str) -> str:
    raw = (db_path_raw or "").strip() or "data/bot.db"
    p = Path(raw)
    if not p.is_absolute() and p.parent == Path('.'):
        p = Path('data') / p
    return str((project_dir / p).resolve()) if not p.is_absolute() else str(p)


async def _run(db_path: str, out_path: str, tz: str) -> None:
    db = Database(db_path)
    await db.init()
    await export_players_snapshot(db, tz_name=tz, out_path=Path(out_path))


def main() -> None:
    project_dir = _project_dir()
    env_path = project_dir / "data" / ".env"
    load_dotenv(dotenv_path=env_path, override=False)

    default_db = _resolve_db_path(project_dir, os.getenv("DB_PATH", "data/bot.db"))
    default_out = str((project_dir / "data" / SNAPSHOT_FILENAME).resolve())
    default_tz = os.getenv("TZ", "Europe/Moscow")

    parser = argparse.ArgumentParser(description="Export players registry snapshot (SQLite -> JSON)")
    parser.add_argument("--db", default=default_db, help="Path to SQLite DB")
    parser.add_argument("--out", default=default_out, help="Output JSON path")
    parser.add_argument("--tz", default=default_tz, help="Timezone name")
    args = parser.parse_args()

    asyncio.run(_run(args.db, args.out, args.tz))


if __name__ == "__main__":
    main()
