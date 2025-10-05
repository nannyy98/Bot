
# Деплой Telegram-бота (Render)

## 1) Переменные окружения
Обязательно задайте **в панели Render** (или через `.env` локально):
- `TELEGRAM_BOT_TOKEN` — токен бота (после ревока старого)
- `POST_CHANNEL_ID` — ID канала (в формате `-100...`), если нужен
- `DATABASE_URL` — единая база **Postgres** для бота и веба, например:  
  `postgresql://webbot_user:***@dpg-d3h61kjuibrs73amgt40-a.frankfurt-postgres.render.com/webbot`
- (опционально) `LOG_DIR` — папка для логов (по умолчанию `logs`)

## 2) Procfile (worker)
Создайте Procfile в корне проекта (если у вас только бот):
```
worker: python main.py
```
На Render создайте **Background Worker** и укажите команду запуска `python main.py`.

Если у вас есть и сайт (Flask) и бот — используйте два сервиса:
- `Web Service` (например, `gunicorn app:app` для Flask)
- `Background Worker` (`python main.py` для бота)

## 3) Зависимости
В `requirements.txt` должны быть:
- `python-telegram-bot==20.7` (или новее 20.x, если код на async)
- `psycopg2-binary` (если работаете с Postgres напрямую)
- `python-dotenv`, `Flask` — по необходимости

## 4) База данных
Код читает URL из `config.DATABASE_URL`.  
Если переменная не задана — будет использован локальный SQLite `shop_bot.db` (для разработки).

## 5) Логи
Файл `logger.py` совместим со стандартным `logging`.  
Методы `logger.info/debug/error(...)` принимают параметры как в `logging`:
```py
logger.info("ok=%s; info=%s", ok, info)
```
### Где искать логи на Render
- **View Logs** в панели сервиса
- или файлы в каталоге `logs/` (если включён файловый лог)

## 6) Проверка перед деплоем
Локально выполните:
```
python -m compileall .
python main.py
```
Если запускается — деплойте на Render.
