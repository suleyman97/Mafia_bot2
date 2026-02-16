from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

PAGE_SIZE = 15

# Telegram clients sometimes render 2-column inline keyboards too narrow when labels are short.
# We pad labels with NBSP to make columns visually fill the width (especially on mobile).
_NBSP = "\u00A0"
_MIN_PLAYER_BTN_LEN = 18


def _pad_btn(label: str, width: int) -> str:
    if len(label) >= width:
        return label
    return label + (_NBSP * (width - len(label)))


def home_keyboard(*, is_registered: bool = False, is_pending: bool = False) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="👥 Игроки", callback_data="home:players")
    kb.button(text="⭐ Подписки", callback_data="home:subs")
    if is_registered:
        kb.button(text="✅ Записаться", callback_data="home:signup")
    else:
        kb.button(text="📝 Регистрация", callback_data="home:register")

    # Layout: row1 (2) + row2 (1)
    kb.adjust(2, 1)
    return kb.as_markup()

def players_keyboard(
    players: list[tuple[int, str]],
    subscribed: set[int],
    page: int,
    total_pages: int,
    query: str = "",
) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()

    # Choose a target width for this page to keep 2 columns wide and aligned.
    # We base it on the longest label on the page but enforce a minimum.
    max_len = 0
    for _, name in players:
        max_len = max(max_len, len(f"✅ {name}"), len(f"➕ {name}"))
    target_len = max(_MIN_PLAYER_BTN_LEN, min(28, max_len))

    for player_id, name in players:
        if player_id in subscribed:
            kb.button(text=_pad_btn(f"✅ {name}", target_len), callback_data=f"unsub:{player_id}:{page}")
        else:
            kb.button(text=_pad_btn(f"➕ {name}", target_len), callback_data=f"sub:{player_id}:{page}")
    # 2 columns for mobile-friendly layout
    kb.adjust(2)

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

    footer = InlineKeyboardBuilder()
    footer.button(text="🏠 Меню", callback_data="home")
    kb.attach(footer)
    return kb.as_markup()

def subs_keyboard(subs: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for player_id, name in subs[:30]:
        kb.button(text=f"❌ {name}", callback_data=f"unsub_s:{player_id}")
    kb.adjust(1)

    footer = InlineKeyboardBuilder()
    footer.button(text="🏠 Меню", callback_data="home")
    kb.attach(footer)
    return kb.as_markup()
