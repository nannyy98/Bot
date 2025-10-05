"""
Сброс БД Postgres: удаление и пересоздание схемы public.
ВНИМАНИЕ: уничтожает все таблицы/данные в текущей БД!
"""
import os
import sys

DATABASE_URL = os.getenv("DATABASE_URL", "")

if not DATABASE_URL or not DATABASE_URL.startswith(("postgres://","postgresql://")):
    print("DATABASE_URL не задан или не Postgres — отмена.")
    sys.exit(2)

# psycopg2 is expected to be installed
import psycopg2

def reset_schema(url: str):
    conn = psycopg2.connect(url)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS public CASCADE;")
    cur.execute("CREATE SCHEMA public;")
    cur.execute("GRANT ALL ON SCHEMA public TO public;")
    cur.close()
    conn.close()

if __name__ == "__main__":
    print("⚠️ ВНИМАНИЕ: будет сброшена схема public в БД из DATABASE_URL")
    reset_schema(DATABASE_URL)
    print("Готово: схема public пересоздана.")
