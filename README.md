# Mafia Notify Bot (Telegram) — Py3.13 fixed

## Important
- BotFather: /setprivacy -> Disable
- Add bot to the group.

## Install
```powershell
python -m venv .venv
.\.venv\Scriptsctivate
python -m pip install -U pip setuptools wheel
pip install -r requirements.txt
# при первом запуске бот автоматически создаст файл настроек: ./data/.env
# открой ./data/.env и заполни BOT_TOKEN (и при желании ADMIN_IDS)
python main.py
```

## Commands
Private:
- /start
- /players
- /subs

Group (admins only):
- /event — статус
- /untrack — остановить отслеживание (если нужно)

## Auto-tracking
- Бот автоматически начинает отслеживать пост записи, если его отправил админ группы (или пользователь из ADMIN_IDS) и в тексте есть блок "Список игроков" + шаблонные маркеры (❓/?/короткие плейсхолдеры).
- Ручной /track удалён: всё работает автоматически.

## .env flags (тестирование)
- Файл настроек находится в `./data/.env` (создаётся автоматически при первом запуске).
- По умолчанию база хранится в `./data/bot.db` (путь считается относительно папки проекта, а не текущей рабочей директории).
- REPEAT_NOTIFY=1 — повторные уведомления (игнорировать notify_log)
- NOTIFY_ON_CREATE=1 — уведомлять сразу при создании/копировании новой записи
- ANNOUNCE_AUTOTRACK=1 — писать в группу короткое сообщение, что запись взята под отслеживание


## Troubleshooting: база не наполняется
- Убедись, что в BotFather выключен privacy mode (`/setprivacy -> Disable`).
- Проверь, что бот добавлен в группу/канал и имеет права читать сообщения и edited messages.
- В `.env` можно задать путь к БД. Если путь относительный, он считается от папки проекта, например: `DB_PATH=data/bot.db`.
- После старта проверь лог: бот выводит `DB_PATH`, по этому пути должна лежать актуальная sqlite база.
