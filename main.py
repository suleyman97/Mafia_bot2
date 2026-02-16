import asyncio
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import zoneinfo
from html import escape as html_escape

from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.enums import ChatType

from config import load_config
from db import Database
from services import BotService
from keyboards import players_keyboard, subs_keyboard, home_keyboard, PAGE_SIZE
from parser import parse_players_from_post, normalize_name
from parser import beautify_display_name
from players_sync import export_players_snapshot, ensure_patch_file, SNAPSHOT_FILENAME, PATCH_FILENAME

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("main")

router = Router()

# Per-user state for /players search (OK for test stage; resets on restart)
USER_PLAYERS_QUERY: dict[int, str] = {}
USER_EXPECTING_QUERY: set[int] = set()
USER_EXPECTING_REG_NICK: set[int] = set()

HELP_PRIVATE = """Я помогу тебе не пропускать игры в BASE 🎭

Подписывайся на любимых игроков — и я напишу в личку, когда они появятся в записи.

Кнопки:
• 👥 Игроки — список игроков и подписки
• ⭐ Подписки — твой список подписок
• ✅ Записаться — добавить себя в актуальную запись (после регистрации)
"""


async def _send_or_edit_main_ui(
    *,
    bot: Bot,
    service: BotService,
    chat_id: int,
    user_id: int,
    text: str,
    reply_markup,
):
    """Keep the private chat clean: render everything into a single message."""
    mid = await service.get_main_ui_message_id(user_id)
    if mid:
        try:
            await bot.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=mid,
                reply_markup=reply_markup,
                disable_web_page_preview=True,
            )
            return
        except Exception:
            pass

    msg = await bot.send_message(chat_id, text, reply_markup=reply_markup, disable_web_page_preview=True)
    await service.set_main_ui_message_id(user_id, msg.message_id)


async def render_home_ui(bot: Bot, service: BotService, message: Message) -> None:
    u = message.from_user
    if not u:
        return
    await service.upsert_user(u.id, u.username, u.first_name, u.last_name)
    pid = await service.get_user_player_id(u.id)
    pending = await service.get_pending_registration(u.id)

    status_lines: list[str] = []
    is_registered = False
    is_pending = False

    if pid:
        is_registered = True
        pname = await service.get_player_display_name(pid)
        if pname:
            status_lines.append(f"👤 Ты зарегистрирован как: <b>{html_escape(pname)}</b>")
            status_lines.append("Нажми <b>✅ Записаться</b>, чтобы добавить себя в актуальную запись в группе.")
    elif pending:
        is_pending = True
        status_lines.append("⏳ Заявка на регистрацию отправлена. Ждём подтверждения админа.")
        status_lines.append("Если ошибся в нике — нажми <b>📝 Регистрация</b> ещё раз и отправь правильный ник.")
    else:
        status_lines.append("📝 Чтобы записываться на игры прямо из этого чата — пройди регистрацию.")

    text = "<b>Base bot</b> 🎭\n\n" + HELP_PRIVATE
    if status_lines:
        text += "\n" + "\n".join(status_lines)

    await _send_or_edit_main_ui(
        bot=bot,
        service=service,
        chat_id=message.chat.id,
        user_id=u.id,
        text=text,
        reply_markup=home_keyboard(is_registered=is_registered, is_pending=is_pending),
    )


async def render_players_ui(
    *,
    bot: Bot,
    service: BotService,
    chat_id: int,
    user_id: int,
    page: int = 1,
    query: str = "",
    status: str | None = None,
):
    players, total_pages = await service.list_players_page(user_id, page, PAGE_SIZE, query=query)
    subscribed = await service.get_subscribed_player_ids(user_id)

    header = "<b>👥 Игроки клуба</b>"
    if query:
        header += f"\n🔎 <code>{html_escape(query)}</code>"
    if status:
        header += f"\n\n{status}"

    if not players and not query:
        await _send_or_edit_main_ui(
            bot=bot,
            service=service,
            chat_id=chat_id,
            user_id=user_id,
            text=(
                "<b>👥 Игроки</b>\n\n"
                "Пока нет игроков в базе. Они появятся, когда бот начнёт отслеживать пост записи и увидит список игроков."
            ),
            reply_markup=home_keyboard(is_registered=bool(await service.get_user_player_id(user_id)), is_pending=bool(await service.get_pending_registration(user_id))),
        )
        return

    if not players and query:
        header += "\n\nНичего не найдено. Попробуй другой запрос или нажми ✖️ Сброс."

    await _send_or_edit_main_ui(
        bot=bot,
        service=service,
        chat_id=chat_id,
        user_id=user_id,
        text=header,
        reply_markup=players_keyboard(players, subscribed, page, total_pages, query=query),
    )


async def render_subs_ui(*, bot: Bot, service: BotService, chat_id: int, user_id: int) -> None:
    subs = await service.list_subscriptions(user_id)
    if not subs:
        text = "<b>⭐ Мои подписки</b>\n\nУ тебя пока нет подписок. Открой 👥 Игроки и выбери кого отслеживать." 
        await _send_or_edit_main_ui(
            bot=bot,
            service=service,
            chat_id=chat_id,
            user_id=user_id,
            text=text,
            reply_markup=home_keyboard(is_registered=bool(await service.get_user_player_id(user_id)), is_pending=bool(await service.get_pending_registration(user_id))),
        )
        return
    lines = "\n".join([f"• {html_escape(name)}" for _, name in subs])
    text = "<b>⭐ Мои подписки</b>\n\n" + lines
    await _send_or_edit_main_ui(bot=bot, service=service, chat_id=chat_id, user_id=user_id, text=text, reply_markup=subs_keyboard(subs))


HELP_GROUP = (
    "Команды (для админов):\n"
    "• /untrack — остановить отслеживание (если нужно)\n"
    "• /event — статус отслеживания\n"
)

_QMARK_RE = re.compile(r"[❓❔]")  # U+2753 or U+2754

_PLUS_LINE_RE = re.compile(r"^\s*\+\s*(.+?)\s*$")
_PLUS_HAS_NAME_RE = re.compile(r"[A-Za-zА-Яа-яЁё@]")
_QUESTION_ONLY_RE = re.compile(r"^\s*[❓❔]+\s*$")
_STOP_RE = re.compile(r"\b(стоимость|цена|оплат|локац|адрес|место|парковк|ставь\s*\+)\b", flags=re.IGNORECASE)

# Organizer posts in this project always contain a stable title line like:
# (any emoji) МАФИЯ В СОЧИ (В день недели)
_MAFIA_TITLE_RE = re.compile(r"(?im)^\s*\W*мафия\s+в\s+сочи\b")


def _parse_hhmm(value: str) -> tuple[int, int]:
    s = (value or "").strip()
    if not s:
        return (4, 0)
    parts = s.split(":")
    if len(parts) != 2:
        return (4, 0)
    try:
        h = int(parts[0])
        m = int(parts[1])
        if 0 <= h <= 23 and 0 <= m <= 59:
            return (h, m)
    except Exception:
        pass
    return (4, 0)


async def _players_snapshot_loop(service: BotService, *, out_path: Path, tz_name: str, hhmm: str) -> None:
    """Run daily SQLite->JSON snapshot export."""
    h, m = _parse_hhmm(hhmm)
    try:
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception as e:
        # On Windows (and some minimal containers) IANA tz database may be missing.
        # Installing PyPI package `tzdata` fixes ZoneInfo for names like Europe/Moscow.
        log.warning("Timezone %s is not available (%s). Falling back to UTC. Install 'tzdata' to fix.", tz_name, e)
        tz = timezone.utc

    while True:
        now = datetime.now(tz=tz)
        target = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if target <= now:
            target = target + timedelta(days=1)
        sleep_sec = max(1.0, (target - now).total_seconds())
        await asyncio.sleep(sleep_sec)
        try:
            await export_players_snapshot(service.db, tz_name=service.tz_name, out_path=out_path)
            log.info("Players snapshot exported: %s", out_path)
        except Exception:
            log.exception("Failed to export players snapshot")


def parse_plus_players(text: str) -> list[str]:
    """Parse messages like:
    +Альфач\n+Старк

    Returns [] if the message is not a pure "+players" message.
    """
    if not text:
        return []
    lines = [ln.strip() for ln in text.splitlines()]
    names: list[str] = []
    for ln in lines:
        if not ln:
            continue
        # ignore commands / regular chat
        if ln.startswith("/"):
            return []
        m = _PLUS_LINE_RE.match(ln)
        if not m:
            return []
        name = m.group(1).strip()
        if not name or not _PLUS_HAS_NAME_RE.search(name):
            continue
        names.append(name)

    # dedupe (preserve order)
    out: list[str] = []
    seen: set[str] = set()
    for n in names:
        key = normalize_name(n)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(n)
    return out


def _find_players_block(lines: list[str]) -> tuple[int | None, int, int]:
    """Return (header_idx, start_idx, end_idx_for_insertion).

    end_idx points to the first line *after* the players block (i.e. where we should
    insert new player lines). We try to stop before placeholders (❓) and before next
    sections like "Цена:" etc.
    """
    header_idx = None
    for i, ln in enumerate(lines):
        if re.search(r"\bсписок\s+игрок", ln, flags=re.IGNORECASE):
            header_idx = i
            break
    if header_idx is None:
        return None, 0, len(lines)

    start = header_idx + 1
    end = len(lines)
    for j in range(start, len(lines)):
        raw = lines[j].strip()
        if not raw:
            continue
        # Insert BEFORE placeholders
        if _QUESTION_ONLY_RE.match(raw):
            end = j
            break
        # Stop at obvious next section markers
        if _STOP_RE.search(raw):
            end = j
            break
        if re.fullmatch(r"[A-Za-zА-Яа-яЁё0-9 _-]{1,60}:\s*", raw):
            end = j
            break
    return header_idx, start, end


def _derive_line_prefix(sample_line: str) -> str:
    """Try to preserve list style (emoji/bullet/indent) from an existing player line."""
    m = _PLUS_HAS_NAME_RE.search(sample_line)
    if not m:
        return ""
    return sample_line[: m.start()]


def add_players_to_post_text(old_text: str | None, added_players: list[str]) -> str:
    """Return updated post text by inserting added_players into the existing post.

    Goal: preserve the original formatting максимально:
    - keep all lines outside the players block unchanged
    - keep existing player lines unchanged
    - insert new player lines right after the last existing player line (or after header)
      using the same prefix style (emoji/bullet/indent) when possible
    """
    if not old_text:
        # Fallback: minimal template
        if not added_players:
            return "Список игроков:\n"
        return "Список игроков:\n" + "\n".join([f"• {p}" for p in added_players])

    if not added_players:
        return old_text

    lines = old_text.splitlines()
    header_idx, start, end = _find_players_block(lines)
    if header_idx is None:
        # No header → fallback minimal
        return old_text + "\n\nСписок игроков:\n" + "\n".join([f"• {p}" for p in added_players])

    block = lines[start:end]

    def is_player_line(line: str) -> bool:
        s = line.strip()
        if not s:
            return False
        if _QUESTION_ONLY_RE.match(s):
            return False
        if _STOP_RE.search(s):
            return False
        if re.fullmatch(r"[A-Za-zА-Яа-яЁё0-9 _-]{1,60}:\s*", s):
            return False
        return _PLUS_HAS_NAME_RE.search(s) is not None

    player_idxs = [i for i, ln in enumerate(block) if is_player_line(ln)]

    if player_idxs:
        insert_at = player_idxs[-1] + 1
        prefix = _derive_line_prefix(block[player_idxs[-1]])
    else:
        # No existing players: keep leading empty lines, insert after them
        insert_at = 0
        while insert_at < len(block) and not block[insert_at].strip():
            insert_at += 1
        prefix = ""

    new_lines = [prefix + p for p in added_players]

    new_block = block[:insert_at] + new_lines + block[insert_at:]
    updated = lines[:start] + new_block + lines[end:]
    return "\n".join(updated)

def is_candidate_event_post(text: str) -> bool:
    """Heuristic to detect the organizer sign-up post among normal chat messages.

    We must avoid tracking random chat messages, so we rely on STABLE markers:
    - phrase like 'Список игроков'
    - question-mark block (❓/❔) OR lines that are just '?' (fallback)

    During creation the post can be "empty" (only placeholders). In that case we still
    want to start tracking, so we also accept:
    - 'Список игроков' + at least a couple of short lines right after it (placeholders),
      even if there are no names yet.
    """
    if not text:
        return False

    # Project-specific stable title marker
    if not _MAFIA_TITLE_RE.search(text):
        return False

    if not re.search(r"\bсписок\s+игрок", text, flags=re.IGNORECASE):
        return False

    # 1) Preferred: emoji question marks
    if _QMARK_RE.search(text):
        return True

    # 2) Fallback: plain '?' lines (some keyboards replace emoji with '?')
    q_plain = 0
    for ln in text.splitlines():
        if re.fullmatch(r"\s*\?+\s*", ln):
            q_plain += 1
            if q_plain >= 2:
                return True

    # 3) Fallback: detect a "placeholder" block right after the header
    lines = text.splitlines()
    for i, ln in enumerate(lines):
        if re.search(r"\bсписок\s+игрок", ln, flags=re.IGNORECASE):
            after = [x.strip() for x in lines[i+1:i+25]]
            # count non-empty short lines (placeholders like 🔑, •, etc.)
            short = [x for x in after if x and len(x) <= 6]
            if len(short) >= 2:
                return True
            break

    # 4) Last resort: if at least one player name parses, treat as candidate
    try:
        from parser import parse_players_from_post
        return len(parse_players_from_post(text)) > 0
    except Exception:
        return False


_ADMIN_CACHE: dict[tuple[int, int], tuple[bool, float]] = {}
_ADMIN_CACHE_TTL_SEC = 60.0

async def is_authorized_source(message: Message, admin_ids: set[int], bot: Bot) -> bool:
    """Allow organizer/admin messages.

    Cases:
    - allow-list: from_user.id in ADMIN_IDS
    - anonymous admin / "send as chat" / "send as channel": sender_chat is set -> authorized
    - fallback: check chat member status (administrator/creator) with a small TTL cache
    """
    if message.sender_chat is not None:
        return True

    if message.from_user is None:
        return False

    if message.from_user.id in admin_ids:
        return True

    key = (message.chat.id, message.from_user.id)
    cached = _ADMIN_CACHE.get(key)
    now = time.time()
    if cached and cached[1] > now:
        return cached[0]

    is_admin = False
    try:
        member = await bot.get_chat_member(message.chat.id, message.from_user.id)
        is_admin = member.status in ("administrator", "creator")
    except Exception:
        is_admin = False

    _ADMIN_CACHE[key] = (is_admin, now + _ADMIN_CACHE_TTL_SEC)
    return is_admin

@router.message(Command("start"), F.chat.type == ChatType.PRIVATE)
async def cmd_start(message: Message, service: BotService, bot: Bot):
    await render_home_ui(bot, service, message)


@router.message(Command("menu"), F.chat.type == ChatType.PRIVATE)
async def cmd_menu(message: Message, service: BotService, bot: Bot):
    """Alias for going back to the main menu."""
    await render_home_ui(bot, service, message)

@router.message(Command("help"), F.chat.type == ChatType.PRIVATE)
async def cmd_help(message: Message, service: BotService, bot: Bot):
    # show help inside the single UI message
    u = message.from_user
    if not u:
        return
    await service.upsert_user(u.id, u.username, u.first_name, u.last_name)
    text = "<b>ℹ️ Как это работает</b>\n\n" + HELP_PRIVATE
    await _send_or_edit_main_ui(
        bot=bot,
        service=service,
        chat_id=message.chat.id,
        user_id=u.id,
        text=text,
        reply_markup=home_keyboard(is_registered=bool(await service.get_user_player_id(u.id)), is_pending=bool(await service.get_pending_registration(u.id))),
    )

@router.message(Command("players"), F.chat.type == ChatType.PRIVATE)
async def cmd_players(message: Message, service: BotService, bot: Bot):
    u = message.from_user
    await service.upsert_user(u.id, u.username, u.first_name, u.last_name)
    page = 1
    USER_EXPECTING_QUERY.discard(u.id)
    # /players should always show the full list (reset any previous search)
    USER_PLAYERS_QUERY[u.id] = ""
    await render_players_ui(bot=bot, service=service, chat_id=message.chat.id, user_id=u.id, page=page, query="")

@router.message(Command("subs"), F.chat.type == ChatType.PRIVATE)
async def cmd_subs(message: Message, service: BotService, bot: Bot):
    u = message.from_user
    await service.upsert_user(u.id, u.username, u.first_name, u.last_name)
    await render_subs_ui(bot=bot, service=service, chat_id=message.chat.id, user_id=u.id)

@router.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer()


@router.callback_query(F.data == "home")
async def cb_home(call: CallbackQuery, service: BotService, bot: Bot):
    # Remember the message we use as the main UI message
    await service.set_main_ui_message_id(call.from_user.id, call.message.message_id)
    await service.upsert_user(call.from_user.id, call.from_user.username, call.from_user.first_name, call.from_user.last_name)

    pid = await service.get_user_player_id(call.from_user.id)
    pending = await service.get_pending_registration(call.from_user.id)

    status_lines: list[str] = []
    is_registered = False
    is_pending = False
    if pid:
        is_registered = True
        pname = await service.get_player_display_name(pid)
        if pname:
            status_lines.append(f"👤 Ты зарегистрирован как: <b>{html_escape(pname)}</b>")
            status_lines.append("Нажми <b>✅ Записаться</b>, чтобы добавить себя в актуальную запись в группе.")
    elif pending:
        is_pending = True
        status_lines.append("⏳ Заявка на регистрацию отправлена. Ждём подтверждения админа.")
        status_lines.append("Если ошибся в нике — нажми <b>📝 Регистрация</b> ещё раз и отправь правильный ник.")
    else:
        status_lines.append("📝 Чтобы записываться на игры прямо из этого чата — пройди регистрацию.")

    text = "<b>Mafia Notify Bot</b> 🎭\n\n" + HELP_PRIVATE
    text += "\n" + "\n".join(status_lines)

    await _send_or_edit_main_ui(
        bot=bot,
        service=service,
        chat_id=call.message.chat.id,
        user_id=call.from_user.id,
        text=text,
        reply_markup=home_keyboard(is_registered=is_registered, is_pending=is_pending),
    )
    await call.answer()


@router.callback_query(F.data.startswith("home:"))
async def cb_home_actions(call: CallbackQuery, service: BotService, bot: Bot):
    await service.set_main_ui_message_id(call.from_user.id, call.message.message_id)
    action = call.data.split(":", 1)[1]
    if action == "players":
        USER_EXPECTING_QUERY.discard(call.from_user.id)
        USER_PLAYERS_QUERY[call.from_user.id] = ""
        await render_players_ui(bot=bot, service=service, chat_id=call.message.chat.id, user_id=call.from_user.id, page=1, query="")
    elif action == "subs":
        await render_subs_ui(bot=bot, service=service, chat_id=call.message.chat.id, user_id=call.from_user.id)
    elif action == "register":
        uid = call.from_user.id
        USER_EXPECTING_QUERY.discard(uid)
        USER_EXPECTING_REG_NICK.add(uid)
        await call.answer()
        await call.message.edit_text(
            "<b>📝 Регистрация</b>\n\n"
            "Отправь следующим сообщением свой ник (как тебя знают в клубе/на MafiaUniverse).\n"
            "Пример: <code>Stark</code>",
            reply_markup=call.message.reply_markup,
            disable_web_page_preview=True,
        )
        return
    elif action == "signup":
        await call.answer()
        await _handle_private_signup(call=call, service=service, bot=bot)
        return
    elif action == "help":
        # Backward compatibility: old UI messages might still have the button.
        text = "<b>ℹ️ Справка</b>\n\n" + HELP_PRIVATE
        await _send_or_edit_main_ui(
            bot=bot,
            service=service,
            chat_id=call.message.chat.id,
            user_id=call.from_user.id,
            text=text,
            reply_markup=home_keyboard(
                is_registered=bool(await service.get_user_player_id(call.from_user.id)),
                is_pending=bool(await service.get_pending_registration(call.from_user.id)),
            ),
        )
    else:
        # Unknown action -> just go home
        await render_home_ui(bot=bot, service=service, message=call.message)
    await call.answer()


@router.callback_query(F.data == "find")
async def cb_find(call: CallbackQuery):
    uid = call.from_user.id
    USER_EXPECTING_QUERY.add(uid)
    # Keep chat clean: show prompt inside the same UI message (no new bot messages)
    await call.answer()
    await call.message.edit_text(
        "<b>🔎 Поиск</b>\n\nОтправь следующим сообщением часть ника игрока.",
        reply_markup=call.message.reply_markup,
        disable_web_page_preview=True,
    )

@router.callback_query(F.data == "clearq")
async def cb_clearq(call: CallbackQuery, service: BotService):
    uid = call.from_user.id
    USER_PLAYERS_QUERY[uid] = ""
    USER_EXPECTING_QUERY.discard(uid)

    await render_players_ui(bot=call.bot, service=service, chat_id=call.message.chat.id, user_id=uid, page=1, query="")
    await call.answer("Сброшено ✅")


@router.message((F.chat.type == ChatType.PRIVATE) & F.text)
async def on_private_text_router(message: Message, service: BotService, bot: Bot, admin_ids: set[int]):
    """Handle "next message" flows in private chat (search / registration).

    We keep the chat clean: user sends one message, we update the main UI message.
    """
    if not message.text:
        return
    if message.text.startswith("/"):
        return
    if not message.from_user:
        return

    uid = message.from_user.id

    # --- Registration nickname input ---
    if uid in USER_EXPECTING_REG_NICK:
        USER_EXPECTING_REG_NICK.discard(uid)
        nick_raw = message.text.strip()
        try:
            rid = await service.create_or_update_registration(uid, nick_raw)
        except Exception:
            await render_home_ui(bot, service, message)
            return

        # Notify admins in private
        username = message.from_user.username
        who = f"@{username}" if username else (message.from_user.full_name or "")
        nick_pretty = beautify_display_name(nick_raw)
        text_admin = (
            "📝 <b>Запрос на регистрацию</b>\n"
            f"Ник: <b>{html_escape(nick_pretty)}</b>\n"
            f"Telegram ID: <code>{uid}</code>\n"
            + (f"Пользователь: {html_escape(who)}\n" if who else "")
        )

        from aiogram.utils.keyboard import InlineKeyboardBuilder
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Подтвердить", callback_data=f"reg:approve:{rid}")
        kb.button(text="❌ Отказать", callback_data=f"reg:reject:{rid}")
        kb.adjust(2)
        markup = kb.as_markup()

        for aid in admin_ids:
            try:
                await bot.send_message(aid, text_admin, reply_markup=markup, disable_web_page_preview=True)
            except Exception:
                continue

        # Update user's home UI with pending state
        await render_home_ui(bot, service, message)
        return

    # --- Search query input ---
    if uid in USER_EXPECTING_QUERY:
        query = message.text.strip()
        USER_EXPECTING_QUERY.discard(uid)
        USER_PLAYERS_QUERY[uid] = query
        await render_players_ui(bot=bot, service=service, chat_id=message.chat.id, user_id=uid, page=1, query=query)
        return

@router.callback_query(F.data.startswith("page:"))
async def cb_page(call: CallbackQuery, service: BotService):
    user_id = call.from_user.id
    page = int(call.data.split(":")[1])
    query = USER_PLAYERS_QUERY.get(user_id, "")
    await render_players_ui(bot=call.bot, service=service, chat_id=call.message.chat.id, user_id=user_id, page=page, query=query)
    await call.answer()

@router.callback_query(F.data.startswith("sub:") | F.data.startswith("unsub:"))
async def cb_sub(call: CallbackQuery, service: BotService):
    user_id = call.from_user.id
    action, player_id_s, page_s = call.data.split(":")
    player_id = int(player_id_s)
    page = int(page_s)

    if action == "sub":
        await service.subscribe(user_id, player_id)
        await call.answer("Подписка добавлена ✅")
    else:
        await service.unsubscribe(user_id, player_id)
        await call.answer("Отписался ❌")

    query = USER_PLAYERS_QUERY.get(user_id, "")
    await render_players_ui(bot=call.bot, service=service, chat_id=call.message.chat.id, user_id=user_id, page=page, query=query)


@router.callback_query(F.data.startswith("unsub_s:"))
async def cb_unsub_from_subs(call: CallbackQuery, service: BotService):
    user_id = call.from_user.id
    player_id = int(call.data.split(":")[1])
    await service.unsubscribe(user_id, player_id)
    await call.answer("Отписался ❌")
    await render_subs_ui(bot=call.bot, service=service, chat_id=call.message.chat.id, user_id=user_id)


async def _handle_private_signup(*, call: CallbackQuery, service: BotService, bot: Bot) -> None:
    """Add the registered user's nickname into the currently tracked post in the group."""
    uid = call.from_user.id
    chat_id = call.message.chat.id

    pid = await service.get_user_player_id(uid)
    if not pid:
        # Not registered
        await _send_or_edit_main_ui(
            bot=bot,
            service=service,
            chat_id=chat_id,
            user_id=uid,
            text=(
                "<b>✅ Записаться</b>\n\n"
                "Сначала нужно пройти регистрацию: нажми <b>📝 Регистрация</b> и отправь свой ник."
            ),
            reply_markup=home_keyboard(is_registered=False, is_pending=bool(await service.get_pending_registration(uid))),
        )
        return

    pname = await service.get_player_display_name(pid)
    if not pname:
        await _send_or_edit_main_ui(
            bot=bot,
            service=service,
            chat_id=chat_id,
            user_id=uid,
            text="<b>✅ Записаться</b>\n\nНе нашёл твой профиль игрока в базе. Попробуй зарегистрироваться ещё раз.",
            reply_markup=home_keyboard(is_registered=False, is_pending=bool(await service.get_pending_registration(uid))),
        )
        return

    latest = await service.get_latest_tracked_event()
    if not latest:
        await _send_or_edit_main_ui(
            bot=bot,
            service=service,
            chat_id=chat_id,
            user_id=uid,
            text=(
                "<b>✅ Записаться</b>\n\n"
                "Сейчас нет активного поста записи в группе.\n"
                "Как только организатор опубликует пост (МАФИЯ В СОЧИ + Список игроков), я смогу добавить тебя."
            ),
            reply_markup=home_keyboard(is_registered=True, is_pending=False),
        )
        return

    group_chat_id, tracked_mid, tracked_text = latest
    if not tracked_text:
        await _send_or_edit_main_ui(
            bot=bot,
            service=service,
            chat_id=chat_id,
            user_id=uid,
            text="<b>✅ Записаться</b>\n\nНе могу прочитать текущий текст записи (last_text пустой).",
            reply_markup=home_keyboard(is_registered=True, is_pending=False),
        )
        return

    current_names = parse_players_from_post(tracked_text)
    seen = {normalize_name(x) for x in current_names}
    # Consider all known aliases for the player
    alias_norms = await service.get_player_alias_norms(pid)
    if seen.intersection(alias_norms or {normalize_name(pname)}):
        await _send_or_edit_main_ui(
            bot=bot,
            service=service,
            chat_id=chat_id,
            user_id=uid,
            text=f"<b>✅ Записаться</b>\n\nТы уже есть в записи: <b>{html_escape(pname)}</b> ✅",
            reply_markup=home_keyboard(is_registered=True, is_pending=False),
        )
        return

    # Insert name preserving original formatting
    new_post_text = add_players_to_post_text(tracked_text, [pname])

    # Send updated post in the group (no extra 'service' messages)
    sent = await bot.send_message(
        chat_id=group_chat_id,
        text=new_post_text,
        reply_to_message_id=tracked_mid,
        disable_web_page_preview=True,
    )

    await service.rollover_tracked_event_to_new_message(group_chat_id, sent.message_id, new_last_text=new_post_text)
    await service.update_from_post_and_notify(
        bot=bot,
        chat_id=group_chat_id,
        message_id=sent.message_id,
        text=new_post_text,
        notify_existing=False,
    )

    await _send_or_edit_main_ui(
        bot=bot,
        service=service,
        chat_id=chat_id,
        user_id=uid,
        text=f"<b>✅ Записаться</b>\n\nГотово! Добавил тебя в запись: <b>{html_escape(pname)}</b> 🎭",
        reply_markup=home_keyboard(is_registered=True, is_pending=False),
    )


@router.callback_query(F.data.startswith("reg:"))
async def cb_registration_admin(call: CallbackQuery, service: BotService, bot: Bot, admin_ids: set[int]):
    """Admin approval flow for user registration."""
    if call.from_user.id not in admin_ids:
        await call.answer("Недостаточно прав", show_alert=True)
        return

    parts = call.data.split(":")
    if len(parts) < 3:
        await call.answer()
        return

    action = parts[1]
    if action in ("approve", "reject"):
        request_id = int(parts[2])
        req = await service.get_registration_request(request_id)
        if not req:
            await call.answer("Запрос не найден", show_alert=True)
            return
        if req.get("status") != "pending":
            await call.answer("Уже обработано", show_alert=True)
            return

        user_id = int(req["user_id"])
        nick = str(req["requested_nick"])
        nick_norm = str(req["requested_nick_norm"])

        if action == "reject":
            await service.set_registration_status(request_id, status="rejected", decided_by=call.from_user.id)
            try:
                await _send_or_edit_main_ui(
                    bot=bot,
                    service=service,
                    chat_id=user_id,
                    user_id=user_id,
                    text=(
                        "<b>📝 Регистрация</b>\n\n"
                        "❌ Регистрация отклонена админом.\n"
                        "Если это ошибка — нажми <b>📝 Регистрация</b> и отправь ник ещё раз."
                    ),
                    reply_markup=home_keyboard(is_registered=False, is_pending=False),
                )
            except Exception:
                pass
            await call.message.edit_text(call.message.text + "\n\n❌ <b>Отклонено</b>")
            await call.answer("Отклонено")
            return

        # approve
        candidates = await service.find_player_candidates_by_norm(nick_norm)

        if len(candidates) > 1:
            # Ask admin to pick exact player_id
            from aiogram.utils.keyboard import InlineKeyboardBuilder
            kb = InlineKeyboardBuilder()
            shown = 0
            lines = [
                "⚠️ <b>Найдено несколько игроков с таким ником.</b>",
                f"Ник: <b>{html_escape(nick)}</b>",
                "Выбери, к кому привязать Telegram ID:",
                f"<code>{user_id}</code>",
                "",
            ]
            for c in candidates[:10]:
                pid = int(c["player_id"])
                dn = str(c["display_name"])
                url = c.get("mafiauniverse_url")
                lines.append(f"• #{pid} — {html_escape(dn)}" + (f" ({html_escape(str(url))})" if url else ""))
                kb.button(text=f"#{pid}: {dn}", callback_data=f"reg:choose:{request_id}:{pid}")
                shown += 1
            kb.button(text="➕ Создать нового", callback_data=f"reg:create:{request_id}")
            kb.adjust(1)
            await call.message.answer("\n".join(lines), reply_markup=kb.as_markup())
            await call.answer("Нужно выбрать игрока")
            return

        if len(candidates) == 1:
            pid = int(candidates[0]["player_id"])
            await _approve_registration_link(call=call, service=service, bot=bot, request_id=request_id, user_id=user_id, player_id=pid, nick=nick)
            return

        # No candidates -> create a new player
        pid = await service.get_or_create_player(nick)
        await _approve_registration_link(call=call, service=service, bot=bot, request_id=request_id, user_id=user_id, player_id=pid, nick=nick)
        return

    if action == "choose" and len(parts) >= 4:
        request_id = int(parts[2])
        player_id = int(parts[3])
        req = await service.get_registration_request(request_id)
        if not req or req.get("status") != "pending":
            await call.answer("Запрос не найден/уже обработан", show_alert=True)
            return
        user_id = int(req["user_id"])
        nick = str(req["requested_nick"])
        await _approve_registration_link(call=call, service=service, bot=bot, request_id=request_id, user_id=user_id, player_id=player_id, nick=nick)
        return

    if action == "create" and len(parts) >= 3:
        request_id = int(parts[2])
        req = await service.get_registration_request(request_id)
        if not req or req.get("status") != "pending":
            await call.answer("Запрос не найден/уже обработан", show_alert=True)
            return
        user_id = int(req["user_id"])
        nick = str(req["requested_nick"])
        pid = await service.get_or_create_player(nick)
        await _approve_registration_link(call=call, service=service, bot=bot, request_id=request_id, user_id=user_id, player_id=pid, nick=nick)
        return

    await call.answer()


async def _approve_registration_link(*, call: CallbackQuery, service: BotService, bot: Bot, request_id: int, user_id: int, player_id: int, nick: str) -> None:
    # Conflict check: player already linked to another telegram id
    prow = await service.db.fetchone("SELECT telegram_user_id, display_name FROM players WHERE player_id=?", (player_id,))
    if prow and prow["telegram_user_id"] is not None and int(prow["telegram_user_id"]) not in (0, user_id):
        await call.answer("Этот игрок уже привязан к другому Telegram", show_alert=True)
        return

    await service.link_user_to_player(user_id, player_id)
    await service.ensure_player_alias(player_id, nick)
    await service.set_registration_status(request_id, status="approved", decided_by=call.from_user.id)

    pname = str(prow["display_name"]) if prow and prow["display_name"] else nick

    # Notify user (keep chat clean: update the main UI message)
    try:
        await _send_or_edit_main_ui(
            bot=bot,
            service=service,
            chat_id=user_id,
            user_id=user_id,
            text=(
                f"<b>✅ Регистрация подтверждена</b>\n\n"
                f"Ты привязан как: <b>{html_escape(pname)}</b>\n"
                "Теперь можешь нажать <b>✅ Записаться</b>."
            ),
            reply_markup=home_keyboard(is_registered=True, is_pending=False),
        )
    except Exception:
        pass

    # Mark admin message
    try:
        await call.message.edit_text(call.message.text + f"\n\n✅ <b>Подтверждено</b> → #{player_id} ({html_escape(pname)})")
    except Exception:
        pass
    await call.answer("Подтверждено")

@router.message((F.chat.type == ChatType.GROUP) | (F.chat.type == ChatType.SUPERGROUP))
async def auto_track_post(
    message: Message,
    service: BotService,
    bot: Bot,
    admin_ids: set[int],
    notify_on_create: bool,
    announce_autotrack: bool,
):
    """Auto-enable tracking when an admin/organizer posts the sign-up template.

    This handler runs on NEW messages. Previously, we only synced the snapshot (no notifications),
    which is why copy-paste / creation looked like "nothing happened".
    Now we can optionally notify subscribers immediately (NOTIFY_ON_CREATE=1).
    """
    # Ignore bot's own messages to avoid loops
    if message.from_user is not None and message.from_user.is_bot:
        return

    text = message.text or message.caption or ""

    # --- NEW: "+player" quick adds ---
    plus_names = parse_plus_players(text)
    if plus_names:
        tracked_id, tracked_text = await service.get_tracked_message(message.chat.id)
        if not tracked_id:
            # No active event post → do nothing (no extra messages in the group)
            return

        # Current list (prefer preserving order from the last tracked text)
        current_names: list[str] = []
        if tracked_text:
            current_names = parse_players_from_post(tracked_text)
        else:
            # Fallback: load from DB snapshot (order will be alphabetical)
            ids = await service.get_event_player_ids(message.chat.id, tracked_id)
            if ids:
                rows = await service.db.fetchall(
                    """SELECT display_name FROM players WHERE player_id IN ({}) ORDER BY display_name COLLATE NOCASE""".format(
                        ",".join(["?"] * len(ids))
                    ),
                    tuple(ids),
                )
                current_names = [str(r["display_name"]) for r in rows]

        seen = {normalize_name(x) for x in current_names}
        actually_added: list[str] = []
        merged = list(current_names)
        for n in plus_names:
            resolved = await service.resolve_name_for_post(n)
            key = normalize_name(resolved)
            if not key or key in seen:
                continue
            seen.add(key)
            merged.append(resolved)
            actually_added.append(resolved)

        if not actually_added:
            # Nothing new → do nothing (no extra messages in the group)
            return

        # Preserve original formatting: insert only newly added players into the tracked post text
        new_post_text = add_players_to_post_text(tracked_text, actually_added)

        sent = await bot.send_message(
            chat_id=message.chat.id,
            text=new_post_text,
            reply_to_message_id=tracked_id,
            disable_web_page_preview=True,
        )

        # Switch tracking to the new bot post (preserve snapshot from the previous one)
        await service.rollover_tracked_event_to_new_message(
            message.chat.id,
            sent.message_id,
            new_last_text=new_post_text,
        )

        # Notify only for newly added players
        players_total, new_players, title = await service.update_from_post_and_notify(
            bot=bot,
            chat_id=message.chat.id,
            message_id=sent.message_id,
            text=new_post_text,
            notify_existing=False,
        )
        return

    # --- Auto-track of organizer posts ---
    if not await is_authorized_source(message, admin_ids, bot):
        return

    if not is_candidate_event_post(text):
        return

    tracked_id = await service.get_tracked_message_id(message.chat.id)
    if tracked_id == message.message_id:
        return

    # Switch tracking to the new organizer post (cleanup old snapshot/logs)
    await service.set_tracked_message(message.chat.id, message.message_id, last_text=text, cleanup_old=True)

    if notify_on_create:
        players_total, new_players, title = await service.update_from_post_and_notify(
            bot=bot,
            chat_id=message.chat.id,
            message_id=message.message_id,
            text=text,
            notify_existing=True,
        )
    else:
        players_total, title = await service.sync_event_from_post(message.chat.id, message.message_id, text)
        new_players = 0

    log.info(
        "Auto-tracking enabled: chat=%s message=%s title=%r players=%s notified_now=%s",
        message.chat.id,
        message.message_id,
        title,
        players_total,
        new_players,
    )

    if announce_autotrack:
        await message.reply(
            f"📌 Отслеживаю запись: <b>{html_escape(title)}</b>\n"
            f"Игроков сейчас: {players_total}",
        )

@router.channel_post()
async def auto_track_channel_post(message: Message, service: BotService, bot: Bot, notify_on_create: bool):
    """Auto-enable tracking for channel posts (if you use a channel for announcements)."""
    text = message.text or message.caption or ""
    if not is_candidate_event_post(text):
        return

    tracked_id = await service.get_tracked_message_id(message.chat.id)
    if tracked_id == message.message_id:
        return

    await service.set_tracked_message(message.chat.id, message.message_id, last_text=text, cleanup_old=True)

    if notify_on_create:
        players_total, new_players, title = await service.update_from_post_and_notify(
            bot=bot,
            chat_id=message.chat.id,
            message_id=message.message_id,
            text=text,
            notify_existing=True,
        )
    else:
        players_total, title = await service.sync_event_from_post(message.chat.id, message.message_id, text)
        new_players = 0

    log.info(
        "Auto-tracking enabled (channel): chat=%s message=%s title=%r players=%s notified_now=%s",
        message.chat.id,
        message.message_id,
        title,
        players_total,
        new_players,
    )

@router.edited_channel_post()
async def on_edited_channel_post(message: Message, service: BotService, bot: Bot, notify_on_create: bool):
    """Handle edits for tracked channel posts."""
    text = message.text or message.caption or ""
    if not text:
        return

    tracked_id = await service.get_tracked_message_id(message.chat.id)
    if not tracked_id:
        # If nothing is tracked yet, auto-track (and optionally notify).
        if is_candidate_event_post(text):
            await service.set_tracked_message(message.chat.id, message.message_id, last_text=text, cleanup_old=True)

            if notify_on_create:
                players_total, new_players, title = await service.update_from_post_and_notify(
                    bot=bot,
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                    text=text,
                    notify_existing=True,
                )
            else:
                players_total, title = await service.sync_event_from_post(message.chat.id, message.message_id, text)
                new_players = 0

            log.info(
                "Auto-tracking enabled (channel via edit): chat=%s message=%s title=%r players=%s notified_now=%s",
                message.chat.id,
                message.message_id,
                title,
                players_total,
                new_players,
            )
        return

    if message.message_id != tracked_id:
        return

    # keep last_text updated
    await service.set_tracked_message(message.chat.id, message.message_id, last_text=text, cleanup_old=False)

    await service.update_from_post_and_notify(
        bot=bot,
        chat_id=message.chat.id,
        message_id=message.message_id,
        text=text,
        notify_existing=False,
    )

## NOTE: We intentionally avoid slash-commands in group chats to keep them clean.
## Tracking is fully automatic; group admin commands were removed.

@router.edited_message()
async def on_edited_message(message: Message, service: BotService, bot: Bot, admin_ids: set[int], notify_on_create: bool):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    text = message.text or message.caption or ""
    if not text:
        return

    tracked_id = await service.get_tracked_message_id(message.chat.id)

    # If nothing is tracked yet, try to auto-track on first edit of the organizer post.
    if not tracked_id:
        if (await is_authorized_source(message, admin_ids, bot)) and is_candidate_event_post(text):
            await service.set_tracked_message(message.chat.id, message.message_id, last_text=text, cleanup_old=True)

            if notify_on_create:
                players_total, new_players, title = await service.update_from_post_and_notify(
                    bot=bot,
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                    text=text,
                    notify_existing=True,
                )
            else:
                players_total, title = await service.sync_event_from_post(message.chat.id, message.message_id, text)
                new_players = 0

            log.info(
                "Auto-tracking enabled (via edit): chat=%s message=%s title=%r players=%s notified_now=%s",
                message.chat.id,
                message.message_id,
                title,
                players_total,
                new_players,
            )
        return

    if message.message_id != tracked_id:
        return

    # keep last_text updated
    await service.set_tracked_message(message.chat.id, message.message_id, last_text=text, cleanup_old=False)

    players_total, new_players, _ = await service.update_from_post_and_notify(
        bot=bot,
        chat_id=message.chat.id,
        message_id=message.message_id,
        text=text,
        notify_existing=False,
    )
    if new_players > 0:
        await message.reply(f"🔔 Обновление записи: новых игроков: {new_players}. Всего сейчас: {players_total}.")


async def main():
    cfg = load_config()
    db = Database(cfg.db_path)
    await db.init()
    service = BotService(db, tz_name=cfg.tz, repeat_notify=cfg.repeat_notify)

    # Players registry JSON files live next to the DB (./data by default)
    data_dir = Path(cfg.db_path).resolve().parent
    snapshot_path = data_dir / SNAPSHOT_FILENAME
    patch_path = data_dir / PATCH_FILENAME
    ensure_patch_file(patch_path)
    # Export snapshot once on startup (safe, read-only)
    try:
        await export_players_snapshot(service.db, tz_name=service.tz_name, out_path=snapshot_path)
    except Exception:
        log.exception("Failed to export initial players snapshot")

    bot = Bot(cfg.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    dp.workflow_data.update({"service": service, "bot": bot, "admin_ids": cfg.admin_ids, "notify_on_create": cfg.notify_on_create, "announce_autotrack": cfg.announce_autotrack})
    log.info("Bot started. Admin IDs: %s | DB_PATH: %s", sorted(cfg.admin_ids), cfg.db_path)

    if cfg.players_snapshot_enabled:
        asyncio.create_task(
            _players_snapshot_loop(
                service,
                out_path=snapshot_path,
                tz_name=cfg.tz,
                hhmm=cfg.players_snapshot_time,
            )
        )

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
