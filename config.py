import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def _as_bool(value: str, default: bool = False) -> bool:
    v = (value or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "on", "y")

@dataclass(frozen=True)
class Config:
    bot_token: str
    admin_ids: set[int]
    db_path: str
    tz: str

    # Behavior flags (useful during тестирование)
    repeat_notify: bool
    notify_on_create: bool
    announce_autotrack: bool

def load_config() -> Config:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN is missing in environment (.env)")

    admin_raw = os.getenv("ADMIN_IDS", "").strip()
    admin_ids: set[int] = set()
    if admin_raw:
        for x in admin_raw.split(","):
            x = x.strip()
            if x:
                admin_ids.add(int(x))
    # DB path: default is ./data/bot.db relative to the project directory (not CWD).
    db_path_raw = (os.getenv("DB_PATH") or "").strip()
    if not db_path_raw:
        db_path_raw = "data/bot.db"

    # If user provided just a filename (no folder), keep DB inside ./data/
    p_raw = Path(db_path_raw)
    if not p_raw.is_absolute() and p_raw.parent == Path('.'):
        p_raw = Path('data') / p_raw

    project_dir = Path(__file__).resolve().parent
    db_path = str((project_dir / p_raw).resolve()) if not p_raw.is_absolute() else str(p_raw)

    tz = os.getenv("TZ", "Europe/Moscow").strip() or "Europe/Moscow"

    repeat_notify = _as_bool(os.getenv("REPEAT_NOTIFY", "1"), default=True)

    # NEW:
    # When a new sign-up post is detected (creation/copy-paste), should we immediately
    # notify subscribers about players already listed in the post?
    notify_on_create = _as_bool(os.getenv("NOTIFY_ON_CREATE", "1"), default=True)

    # For debugging during tests: send a short message in the group when a post is auto-tracked.
    announce_autotrack = _as_bool(os.getenv("ANNOUNCE_AUTOTRACK", "0"), default=False)

    return Config(
        bot_token=token,
        admin_ids=admin_ids,
        db_path=db_path,
        tz=tz,
        repeat_notify=repeat_notify,
        notify_on_create=notify_on_create,
        announce_autotrack=announce_autotrack,
    )
