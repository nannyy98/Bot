"""
Система резервного копирования БД (совместима с:
`from database_backup import DatabaseBackup`).

Поведение:
- SQLite (`DATABASE_URL` начинается с sqlite:///): делает файловый бэкап.
- Postgres: no-op (используйте pg_dump / бэкапы провайдера), но код не падает.
"""

from __future__ import annotations
import os, shutil, gzip, time, threading, logging
from datetime import datetime

# Берём DATABASE_URL из config или ENV
try:
    from config import DATABASE_URL as _CFG_DATABASE_URL  # type: ignore
except Exception:
    _CFG_DATABASE_URL = os.getenv("DATABASE_URL", "")

try:
    from config import DATABASE_CONFIG  # type: ignore
except Exception:
    DATABASE_CONFIG = {
        "path": "shop_bot.db",
        "backup_interval": 3600,  # сек
        "backup_dir": "backups",
        "gzip": True,
    }

try:
    from logger import logger
except Exception:
    logger = logging.getLogger("database_backup_fallback")
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
        logger.addHandler(h)
    logger.setLevel(logging.INFO)


def _is_sqlite(url: str) -> bool:
    return str(url or "").startswith("sqlite:///")


def _sqlite_db_path(url: str, fallback_path: str) -> str:
    return url.replace("sqlite:///", "", 1) if _is_sqlite(url) else fallback_path


class DatabaseBackup:
    """
    API:
      - DatabaseBackup(database_url: str|None=None, backup_interval: int|None=None,
                       backup_dir: str|None=None, gzip_backup: bool|None=None)
      - start(), stop()
      - backup_now() -> str|None
      - restore_latest() -> bool
    """
    def __init__(self,
                 database_url: str | None = None,
                 backup_interval: int | None = None,
                 backup_dir: str | None = None,
                 gzip_backup: bool | None = None) -> None:
        self.database_url = database_url if database_url is not None else _CFG_DATABASE_URL
        self.is_sqlite = _is_sqlite(self.database_url)

        cfg_interval = int(DATABASE_CONFIG.get("backup_interval", 3600))
        cfg_dir = str(DATABASE_CONFIG.get("backup_dir", "backups"))
        cfg_gzip = bool(DATABASE_CONFIG.get("gzip", True))
        cfg_sqlite_path = str(DATABASE_CONFIG.get("path", "shop_bot.db"))

        self.backup_interval = int(backup_interval or cfg_interval)
        self.backup_dir = backup_dir or cfg_dir
        self.gzip_backup = cfg_gzip if gzip_backup is None else bool(gzip_backup)
        self.sqlite_path = _sqlite_db_path(self.database_url, cfg_sqlite_path)

        self._stop = False
        self._thread: threading.Thread | None = None

        logger.info("DatabaseBackup init: is_sqlite=%s; dir=%s; interval=%ss",
                    self.is_sqlite, self.backup_dir, self.backup_interval)

    # ---------- файловые операции ----------
    def _ensure_dir(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)

    def _backup_sqlite_file(self) -> str | None:
        if not os.path.exists(self.sqlite_path):
            logger.warning("SQLite-файл не найден: %s", self.sqlite_path)
            return None
        self._ensure_dir(self.backup_dir)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = os.path.basename(self.sqlite_path)
        backup_path = os.path.join(self.backup_dir, f"{base}.{ts}.bak")
        shutil.copy2(self.sqlite_path, backup_path)
        logger.info("Создан бэкап SQLite: %s", backup_path)

        if self.gzip_backup:
            gz_path = f"{backup_path}.gz"
            with open(backup_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            try: os.remove(backup_path)
            except Exception: pass
            logger.info("Сжатие бэкапа: %s", gz_path)
            return gz_path
        return backup_path

    def backup_now(self) -> str | None:
        if self.is_sqlite:
            try:
                return self._backup_sqlite_file()
            except Exception as e:
                logger.error("Ошибка бэкапа SQLite: %s", e, exc_info=True)
                return None
        logger.info("Postgres: файловый бэкап не выполняется (no-op).")
        return None

    def _list_backups(self) -> list[str]:
        if not os.path.isdir(self.backup_dir):
            return []
        return sorted(
            [os.path.join(self.backup_dir, f) for f in os.listdir(self.backup_dir)
             if f.endswith(".bak") or f.endswith(".bak.gz")],
            reverse=True,
        )

    def restore_latest(self) -> bool:
        if not self.is_sqlite:
            logger.info("Postgres: restore файлового бэкапа не поддерживается.")
            return False
        files = self._list_backups()
        if not files:
            logger.warning("Нет доступных бэкапов для восстановления.")
            return False
        latest = files[0]
        try:
            if latest.endswith(".gz"):
                unpacked = latest[:-3]
                with gzip.open(latest, "rb") as f_in, open(unpacked, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
                latest = unpacked
            shutil.copy2(latest, self.sqlite_path)
            logger.info("База восстановлена из бэкапа: %s", latest)
            return True
        except Exception as e:
            logger.error("Ошибка восстановления: %s", e, exc_info=True)
            return False

    # ---------- планировщик ----------
    def _worker(self):
        logger.info("Планировщик бэкапов запущен (интервал %s сек)", self.backup_interval)
        while not self._stop:
            try:
                self.backup_now()
            except Exception as e:
                logger.error("Ошибка планового бэкапа: %s", e, exc_info=True)
            for _ in range(self.backup_interval):
                if self._stop: break
                time.sleep(1)
        logger.info("Планировщик бэкапов остановлен.")

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop = False
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop = True
        if self._thread:
            try: self._thread.join(timeout=timeout)
            except Exception: pass
