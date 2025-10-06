
"""
Резервное копирование БД (Postgres): логический дамп в CSV + ZIP.
"""
from __future__ import annotations
import os, csv, io, shutil, zipfile, logging
from datetime import datetime
from sqlalchemy import text
from dbx import get_engine

DEFAULT_BACKUP_DIR = os.getenv("BACKUP_DIR", "backups")

class DatabaseBackup:
    def __init__(self, backup_dir: str | None = None):
        self.backup_dir = backup_dir or DEFAULT_BACKUP_DIR
        os.makedirs(self.backup_dir, exist_ok=True)

    def _list_tables(self):
        sql = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        with get_engine().begin() as con:
            return [row[0] for row in con.execute(sql).fetchall()]

    def _dump_table_to_csv_bytes(self, table: str) -> bytes:
        buf = io.StringIO()
        with get_engine().begin() as con:
            rows = con.execute(text(f'SELECT * FROM "{table}"')).fetchall()
            cols = rows[0].keys() if rows else [c["column_name"] for c in con.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=:t
                ORDER BY ordinal_position
            """), {"t": table}).mappings().all()]
        writer = csv.writer(buf)
        writer.writerow(cols)
        for r in rows:
            writer.writerow([r[c] if hasattr(r, "__getitem__") else getattr(r, c) for c in cols])
        return buf.getvalue().encode("utf-8")

    def create_backup(self) -> str | None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_path = os.path.join(self.backup_dir, f"pg_backup_{ts}.zip")
        try:
            tables = self._list_tables()
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
                # метаданные
                meta = f"created_at={ts}\nengine=postgresql\ntables={len(tables)}\n"
                z.writestr("meta.txt", meta)
                for t in tables:
                    csv_bytes = self._dump_table_to_csv_bytes(t)
                    z.writestr(f"{t}.csv", csv_bytes)
            logging.info(f"✅ Резервная копия создана: {zip_path}")
            return zip_path
        except Exception as e:
            logging.info(f"❌ Ошибка резервного копирования: {e}", exc_info=True)
            return None

    def verify_backup(self, archive_path: str) -> bool:
        try:
            return os.path.exists(archive_path) and os.path.getsize(archive_path) > 0
        except Exception as e:
            logging.info(f"❌ Ошибка проверки бэкапа: {e}", exc_info=True)
            return False
