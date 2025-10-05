"""
Резервное копирование БД.
- Если используется SQLite (DATABASE_URL начинается с "sqlite:///"), делаем копию файла.
- Если используется Postgres — оставляем заглушку (бэкап делать средствами БД/провайдера).
"""
import os
import shutil
from datetime import datetime
from config import DATABASE_URL

def is_sqlite(url: str) -> bool:
    return str(url).startswith("sqlite:///")

def backup_sqlite(db_file: str, backup_dir: str = "backups"):
    os.makedirs(backup_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.basename(db_file)
    dst = os.path.join(backup_dir, f"{base}.{ts}.bak")
    shutil.copy2(db_file, dst)
    return dst

def backup_database():
    if is_sqlite(DATABASE_URL):
        # db file path after "sqlite:///"
        db_file = DATABASE_URL.replace("sqlite:///", "", 1)
        if os.path.exists(db_file):
            return backup_sqlite(db_file)
        else:
            return None
    else:
        # On Postgres use provider-level backups or pg_dump
        return None

if __name__ == "__main__":
    path = backup_database()
    if path:
        print("SQLite backup created:", path)
    else:
        print("No backup action (Postgres or missing SQLite file).")
