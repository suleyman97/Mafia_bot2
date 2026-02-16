from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

PAGE_SIZE = 15

def players_keyboard(
    players: list[tuple[int, str]],
    subscribed: set[int],
    page: int,
    total_pages: int,
    query: str = "",
) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for player_id, name in players:
        if player_id in subscribed:
            kb.button(text=f"✅ {name}", callback_data=f"unsub:{player_id}:{page}")
        else:
            kb.button(text=f"➕ {name}", callback_data=f"sub:{player_id}:{page}")
    kb.adjust(1)

    tools = InlineKeyboardBuilder()
    tools.button(text="🔎 Поиск", callback_data="find")
    if query:
        tools.button(text="✖️ Сброс", callback_data="clearq")
    tools.adjust(2)
    kb.attach(tools)

    nav = InlineKeyboardBuilder()
    if page > 1:
        nav.button(text="⬅️ Назад", callback_data=f"page:{page-1}")
    nav.button(text=f"Стр. {page}/{max(total_pages,1)}", callback_data="noop")
    if page < total_pages:
        nav.button(text="➡️ Вперёд", callback_data=f"page:{page+1}")
    nav.adjust(3)

    kb.attach(nav)
    return kb.as_markup()

def subs_keyboard(subs: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for player_id, name in subs[:30]:
        kb.button(text=f"❌ {name}", callback_data=f"unsub:{player_id}:1")
    kb.adjust(1)
    return kb.as_markup()
