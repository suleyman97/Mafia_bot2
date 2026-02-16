import asyncio
import logging
import re
import time
from html import escape as html_escape

from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, CommandObject
from aiogram.types import Message, CallbackQuery, ForceReply
from aiogram.enums import ChatType

from config import load_config
from db import Database
from services import BotService
from keyboards import players_keyboard, subs_keyboard, PAGE_SIZE

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("main")

router = Router()

# Per-user state for /players search (OK for test stage; resets on restart)
USER_PLAYERS_QUERY: dict[int, str] = {}
USER_EXPECTING_QUERY: set[int] = set()

HELP_PRIVATE = """Хочешь чаще играть с любимыми игроками? 🎭
Подпишись на них — и я не дам пропустить вечер, когда они придут.

Быстрый старт:
• /players — выбрать игроков и подписаться
• /subs — мои подписки

Я отправляю уведомления в личку, когда организатор обновляет «Список игроков».
"""


HELP_GROUP = """Команды (для админов):
• /event — статус отслеживания
• /untrack — остановить отслеживание (если нужно)
"""

_QMARK_RE = re.compile(r"[❓❔]")  # U+2753 or U+2754

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

    Why not only ADMIN_IDS?
    - In real groups the organizer can change, and tests often happen from accounts
      not in the allow-list. If they are an actual group admin, we should treat them
      as authorized.

    Cases:
    - allow-list: from_user.id in ADMIN_IDS
    - anonymous admin / "send as chat" / "send as channel": sender_chat is set
      (this is only possible for admins) -> authorized
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

@router.message(Command("start"))
async def cmd_start(message: Message, service: BotService):
    if message.chat.type != ChatType.PRIVATE:
        await message.answer("Напиши мне в личку: открой бота и нажми /start.")
        return
    u = message.from_user
    await service.upsert_user(u.id, u.username, u.first_name, u.last_name)
    await message.answer("Привет! ✅\n\n" + HELP_PRIVATE)

@router.message(Command("help"))

async def cmd_help(message: Message):
    await message.answer(HELP_PRIVATE if message.chat.type == ChatType.PRIVATE else HELP_GROUP)

@router.message(Command("players"))
async def cmd_players(message: Message, service: BotService):
    if message.chat.type != ChatType.PRIVATE:
        await message.answer("Список игроков доступен в личке с ботом. Напиши мне /start.")
        return
    u = message.from_user
    await service.upsert_user(u.id, u.username, u.first_name, u.last_name)
    page = 1
    USER_EXPECTING_QUERY.discard(u.id)
    query = USER_PLAYERS_QUERY.get(u.id, "")
    players, total_pages = await service.list_players_page(u.id, page, PAGE_SIZE, query=query)
    subscribed = await service.get_subscribed_player_ids(u.id)
    if not players:
        await message.answer("Пока нет игроков в базе. Они появятся, когда бот начнёт отслеживать пост записи и увидит список игроков.")
        return
    await message.answer("Выбери, на кого подписаться:", reply_markup=players_keyboard(players, subscribed, page, total_pages, query=query))

@router.message(Command("subs"))
async def cmd_subs(message: Message, service: BotService):
    if message.chat.type != ChatType.PRIVATE:
        await message.answer("Команда доступна только в личке с ботом.")
        return
    u = message.from_user
    subs = await service.list_subscriptions(u.id)
    if not subs:
        await message.answer("У тебя пока нет подписок. Используй /players.")
        return
    lines = "\n".join([f"• {html_escape(name)}" for _, name in subs])
    await message.answer("Твои подписки:\n" + lines, reply_markup=subs_keyboard(subs))

@router.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer()



@router.callback_query(F.data == "find")
async def cb_find(call: CallbackQuery):
    # Ask the user for a search query (Telegram has no real "search bar" for keyboards)
    uid = call.from_user.id
    USER_EXPECTING_QUERY.add(uid)
    await call.message.answer("🔎 Напиши часть имени игрока для поиска:", reply_markup=ForceReply(selective=True))
    await call.answer()

@router.callback_query(F.data == "clearq")
async def cb_clearq(call: CallbackQuery, service: BotService):
    uid = call.from_user.id
    USER_PLAYERS_QUERY[uid] = ""
    USER_EXPECTING_QUERY.discard(uid)

    page = 1
    players, total_pages = await service.list_players_page(uid, page, PAGE_SIZE, query="")
    subscribed = await service.get_subscribed_player_ids(uid)

    try:
        await call.message.edit_reply_markup(reply_markup=players_keyboard(players, subscribed, page, total_pages, query=""))
    except Exception:
        pass
    await call.answer("Сброшено ✅")


@router.message((F.chat.type == ChatType.PRIVATE) & F.text)
async def on_private_text_for_search(message: Message, service: BotService):
    # If user is in "search" mode, treat the next non-command message as query
    if not message.text:
        return
    if message.text.startswith("/"):
        return

    uid = message.from_user.id
    if uid not in USER_EXPECTING_QUERY:
        return

    query = message.text.strip()
    USER_EXPECTING_QUERY.discard(uid)
    USER_PLAYERS_QUERY[uid] = query

    page = 1
    players, total_pages = await service.list_players_page(uid, page, PAGE_SIZE, query=query)
    subscribed = await service.get_subscribed_player_ids(uid)

    if not players:
        await message.answer(f"Ничего не нашёл по запросу: <b>{html_escape(query)}</b>\nПопробуй другой запрос или нажми /players.", parse_mode="HTML")
        return

    await message.answer(
        f"Результаты поиска: <b>{html_escape(query)}</b>",
        reply_markup=players_keyboard(players, subscribed, page, total_pages, query=query),
        parse_mode="HTML",
    )

@router.callback_query(F.data.startswith("page:"))
async def cb_page(call: CallbackQuery, service: BotService):
    user_id = call.from_user.id
    page = int(call.data.split(":")[1])
    query = USER_PLAYERS_QUERY.get(user_id, "")
    players, total_pages = await service.list_players_page(user_id, page, PAGE_SIZE, query=query)
    subscribed = await service.get_subscribed_player_ids(user_id)
    await call.message.edit_reply_markup(reply_markup=players_keyboard(players, subscribed, page, total_pages, query=query))
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
    players, total_pages = await service.list_players_page(user_id, page, PAGE_SIZE, query=query)
    subscribed = await service.get_subscribed_player_ids(user_id)
    try:
        await call.message.edit_reply_markup(reply_markup=players_keyboard(players, subscribed, page, total_pages, query=query))
    except Exception:
        pass

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
    text = message.text or message.caption or ""
    if not is_candidate_event_post(text):
        return

    if not await is_authorized_source(message, admin_ids, bot):
        return

    tracked_id = await service.get_tracked_message_id(message.chat.id)
    if tracked_id == message.message_id:
        return

    # Switch tracking to the new organizer post (cleanup old snapshot/logs)
    if tracked_id and tracked_id != message.message_id:
        await service.clear_tracked_message(message.chat.id)

    await service.set_tracked_message(message.chat.id, message.message_id)

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

    # Switch tracking to the new organizer post (cleanup old snapshot/logs)
    if tracked_id and tracked_id != message.message_id:
        await service.clear_tracked_message(message.chat.id)

    await service.set_tracked_message(message.chat.id, message.message_id)

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
            await service.set_tracked_message(message.chat.id, message.message_id)

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

    await service.update_from_post_and_notify(
        bot=bot,
        chat_id=message.chat.id,
        message_id=message.message_id,
        text=text,
        notify_existing=False,
    )

@router.message(Command("untrack"))
async def cmd_untrack(message: Message, service: BotService, admin_ids: set[int], bot: Bot):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Эта команда работает только в группе.")
        return
    if not await is_authorized_source(message, admin_ids, bot):
        await message.reply("Только админы могут отключать отслеживание.")
        return
    await service.clear_tracked_message(message.chat.id)
    await message.reply("🛑 Отслеживание отключено.")

@router.message(Command("event"))
async def cmd_event(message: Message, service: BotService):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Эта команда работает только в группе.")
        return
    mid = await service.get_tracked_message_id(message.chat.id)
    if not mid:
        await message.reply("Сейчас ничего не отслеживается. Когда появится пост записи со «Список игроков», бот возьмёт его под отслеживание автоматически.")
        return
    current = await service.get_event_player_ids(message.chat.id, mid)
    await message.reply(f"📌 Сейчас отслеживается message_id={mid}. Игроков в базе для этого поста: {len(current)}")

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
        if is_candidate_event_post(text) and await is_authorized_source(message, admin_ids, bot):
            await service.set_tracked_message(message.chat.id, message.message_id)

            # If we missed the original creation update (privacy mode, permissions, etc.),
            # the first time we see the post might be an edit. Behave like "create":
            # optionally notify about current players.
            if notify_on_create:
                players_total, new_players, title = await service.update_from_post_and_notify(
                    bot=bot,
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                    text=text,
                    notify_existing=True,
                )
                log.info(
                    "Auto-tracking enabled (via edit): chat=%s message=%s title=%r players=%s notified_now=%s",
                    message.chat.id,
                    message.message_id,
                    title,
                    players_total,
                    new_players,
                )
            else:
                players_total, title = await service.sync_event_from_post(message.chat.id, message.message_id, text)
                log.info(
                    "Auto-tracking enabled (via edit): chat=%s message=%s title=%r players=%s",
                    message.chat.id,
                    message.message_id,
                    title,
                    players_total,
                )
        return

    if message.message_id != tracked_id:
        return

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
    bot = Bot(cfg.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    dp.workflow_data.update({"service": service, "bot": bot, "admin_ids": cfg.admin_ids, "notify_on_create": cfg.notify_on_create, "announce_autotrack": cfg.announce_autotrack})
    log.info("Bot started. Admin IDs: %s | DB_PATH: %s", sorted(cfg.admin_ids), cfg.db_path)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
