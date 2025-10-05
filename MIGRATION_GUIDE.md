
# Миграция на Postgres (единая БД для бота и веба)

## Что сделано
- `database.py` теперь умеет работать с **Postgres** через `DATABASE_URL`.
- При `postgresql://` автоматически выполняется `schema_postgres.sql` (создание таблиц `IF NOT EXISTS`).
- Адаптированы SQL-запросы: заменены плейсхолдеры и `INSERT OR IGNORE` → `ON CONFLICT DO NOTHING` (автоматически в рантайме).
- Добавлен `reset_postgres.py` для полного очистки схемы `public` (опасно, удаляет данные).

## Как включить Postgres
1. Задайте переменную окружения `DATABASE_URL` в обоих сервисах (бот и веб), например:  
   `postgresql://webbot_user:***@dpg-d3h61kjuibrs73amgt40-a.frankfurt-postgres.render.com/webbot`
2. Установите зависимости: `pip install -r requirements.txt`

## Первичная инициализация схемы
При старте, если `DATABASE_URL` указывает на Postgres, выполнится `schema_postgres.sql` (создаст таблицы, если их нет).

## Полный сброс БД (ОПАСНО)
```bash
export DATABASE_URL="postgresql://..."
python Bot-main/reset_postgres.py
```

## Совместимость
Код по-прежнему работает с SQLite, если `DATABASE_URL` не задан (для локальной разработки).
