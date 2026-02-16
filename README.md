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
- /event
- /untrack

## Auto-tracking
- Бот автоматически начинает отслеживать пост записи, если его отправил админ (или пользователь из ADMIN_IDS),
  в тексте есть блок "Список игроков" и заголовок вида "МАФИЯ В СОЧИ ...".

## .env flags (тестирование)
- Файл настроек находится в `./data/.env` (создаётся автоматически при первом запуске).
- По умолчанию база хранится в `./data/bot.db` (путь считается относительно папки проекта).
- REPEAT_NOTIFY=1 — повторные уведомления (игнорировать notify_log)
- NOTIFY_ON_CREATE=1 — уведомлять сразу при создании/копировании новой записи
- ANNOUNCE_AUTOTRACK=1 — писать в группу короткое сообщение, что запись взята под отслеживание

## Реестр игроков: JSON snapshot + ручной patch
Бот ведёт реестр игроков в SQLite (таблица `players` + `player_aliases`).

### Snapshot (только чтение)
Каждый день бот выгружает реестр в `./data/players.snapshot.json`.

Настройки:
- PLAYERS_SNAPSHOT_ENABLED=1 — включить/выключить ежедневный экспорт
- PLAYERS_SNAPSHOT_TIME=04:00 — время экспорта (локальное по TZ)

### Patch (ручные правки)
Для ручного редактирования используй `./data/players.patch.json`.
Бот этот файл НЕ перезаписывает.
Импорт patch в SQLite делается скриптом:

```bash
python scripts/import_players_patch.py
```

Экспорт snapshot вручную:

```bash
python scripts/export_players_snapshot.py
```


## Troubleshooting: база не наполняется
- Убедись, что в BotFather выключен privacy mode (`/setprivacy -> Disable`).
- Проверь, что бот добавлен в группу/канал и имеет права читать сообщения и edited messages.
- В `./data/.env` задай постоянный путь к БД, например: `DB_PATH=data/bot.db`.
- После старта проверь лог: бот выводит `DB_PATH`, по этому пути должна лежать актуальная sqlite база.
