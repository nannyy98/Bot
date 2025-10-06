
#!/usr/bin/env python3
"""
Простой самотест БД и основных таблиц для бота (Postgres).
"""
from __future__ import annotations
import logging
from dbx import scalar as db_scalar, all as db_all

def test_database() -> bool:
    logging.info("🔍 Проверка базы данных...")
    try:
        # Простые проверки на существование и доступность ключевых таблиц
        must_tables = ["users", "categories", "products", "orders", "order_items", "cart"]
        for t in must_tables:
            db_scalar(f'SELECT COUNT(*) FROM "{t}"')
        logging.info("✅ Таблицы доступны")

        # Проверка данных
        users = db_scalar("SELECT COUNT(*) FROM users")
        cats = db_scalar("SELECT COUNT(*) FROM categories")
        logging.info(f"👥 users={users}, 🗂 categories={cats}")

        return True
    except Exception as e:
        logging.info(f"❌ Ошибка проверки базы: {e}", exc_info=True)
        return False

def main():
    ok = test_database()
    if ok:
        logging.info("\n✅ Всё ок, можно запускать бота")
    else:
        logging.info("\n❌ Исправьте ошибки перед запуском")

if __name__ == "__main__":
    main()
