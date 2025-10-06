
#!/usr/bin/env python3
"""
Скрипт для исправления базовых проблем с БД (Postgres).
Создаёт категории/админа при отсутствии, чинит простые несоответствия.
"""
from __future__ import annotations
import logging
from dbx import run as db_run, all as db_all, one as db_one, executemany as db_many, scalar as db_scalar

def fix_database_issues() -> bool:
    logging.info("🔧 Исправление базы данных...")

    try:
        # 1) категории
        cnt = db_scalar("SELECT COUNT(*) FROM categories")
        if not cnt or cnt == 0:
            logging.info("   Создаю базовые категории...")
            rows = [
                ('Электроника', 'Смартфоны, ноутбуки, гаджеты', '📱'),
                ('Одежда', 'Мужская и женская одежда', '👕'),
                ('Дом и сад', 'Товары для дома и дачи', '🏠'),
                ('Спорт', 'Спортивные товары и инвентарь', '⚽'),
                ('Красота', 'Косметика и парфюмерия', '💄'),
                ('Книги', 'Художественная и техническая литература', '📚')
            ]
            db_many("INSERT INTO categories (name, description, emoji) VALUES (?, ?, ?)", rows)
            logging.info("   ✅ Категории добавлены")

        # 2) админ-пользователь из конфигурации
        from config import BOT_CONFIG
        admin_telegram_id = BOT_CONFIG.get('admin_telegram_id')
        admin_name = BOT_CONFIG.get('admin_name', 'Admin')
        if admin_telegram_id:
            try:
                admin_telegram_id = int(admin_telegram_id)
            except ValueError:
                logging.info(f"   ❌ Неверный ADMIN_TELEGRAM_ID: {admin_telegram_id}")
                admin_telegram_id = None

        if admin_telegram_id:
            exists = db_scalar("SELECT id FROM users WHERE telegram_id = ?", (admin_telegram_id,))
            if not exists:
                db_run("INSERT INTO users (telegram_id, name, is_admin, language, created_at) VALUES (?, ?, 1, 'ru', CURRENT_TIMESTAMP)", (admin_telegram_id, admin_name))
                logging.info(f"   ✅ Создан админ: {admin_name} ({admin_telegram_id})")
            else:
                logging.info(f"   ✅ Админ уже существует: {admin_name}")
        else:
            # запасной вариант — создадим дефолтного
            db_run("INSERT INTO users (telegram_id, name, is_admin, language, created_at) VALUES (?, ?, 1, 'ru', CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING",
                   (5720497431, 'Admin'))
            logging.info("   ⚠️ ADMIN_TELEGRAM_ID не задан — создан дефолтный админ")

        # 3) убедимся, что товары активны и имеют категорию
        db_run("UPDATE products SET is_active = 1 WHERE is_active IS NULL OR is_active = 0")
        db_run("UPDATE products SET category_id = COALESCE(category_id, 1)")

        logging.info("✅ Исправление завершено")
        return True
    except Exception as e:
        logging.info(f"❌ Ошибка исправления БД: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    ok = fix_database_issues()
    if not ok:
        logging.info("❌ Исправьте ошибки перед запуском")
