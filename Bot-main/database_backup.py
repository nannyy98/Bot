"""
Система резервного копирования БД (совместима с: `from database_backup import DatabaseBackup`).
SQLite — файловые бэкапы; Postgres — no-op (используйте pg_dump/снапшоты провайдера).
"""
from __future__ import annotations
import os, shutil, gzip, time, threading, logging
from datetime import datetime

try:
    from config import DATABASE_URL as _CFG_DATABASE_URL  # type: ignore
except Exception:
    _CFG_DATABASE_URL = os.getenv("DATABASE_URL", "")

try:
    from config import DATABASE_CONFIG  # type: ignore
except Exception:
    DATABASE_CONFIG = {"path": "shop_bot.db", "backup_interval": 3600, "backup_dir": "backups", "gzip": True}

try:
    from logger import logger
except Exception:
    logger = logging.getLogger("database_backup_fallback")
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
        logger.addHandler(h)
    logger.setLevel(logging.INFO)

def _is_sqlite(url: str) -> bool: return str(url or "").startswith("sqlite:///")
def _sqlite_path(url: str, fallback: str) -> str: return url.replace("sqlite:///", "", 1) if _is_sqlite(url) else fallback

class DatabaseBackup:
    def __init__(self, database_url: str|None=None, backup_interval: int|None=None,
                 backup_dir: str|None=None, gzip_backup: bool|None=None) -> None:
        self.database_url = database_url if database_url is not None else _CFG_DATABASE_URL
        self.is_sqlite = _is_sqlite(self.database_url)
        cfg_interval = int(DATABASE_CONFIG.get("backup_interval", 3600))
        cfg_dir = str(DATABASE_CONFIG.get("backup_dir", "backups"))
        cfg_gzip = bool(DATABASE_CONFIG.get("gzip", True))
        cfg_sqlite = str(DATABASE_CONFIG.get("path", "shop_bot.db"))
        self.backup_interval = int(backup_interval or cfg_interval)
        self.backup_dir = backup_dir or cfg_dir
        self.gzip_backup = cfg_gzip if gzip_backup is None else bool(gzip_backup)
        self.sqlite_path = _sqlite_path(self.database_url, cfg_sqlite)
        self._stop = False
        self._t = None
        logger.info("DatabaseBackup init: is_sqlite=%s; dir=%s; interval=%ss", self.is_sqlite, self.backup_dir, self.backup_interval)

    def _ensure_dir(self, p: str): os.makedirs(p, exist_ok=True)

    def _backup_sqlite(self) -> str|None:
        if not os.path.exists(self.sqlite_path):
            logger.warning("SQLite-файл не найден: %s", self.sqlite_path); return None
        self._ensure_dir(self.backup_dir)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = os.path.basename(self.sqlite_path)
        dst = os.path.join(self.backup_dir, f"{base}.{ts}.bak")
        shutil.copy2(self.sqlite_path, dst); logger.info("Создан бэкап SQLite: %s", dst)
        if self.gzip_backup:
            gz = f"{dst}.gz"
            with open(dst, "rb") as fi, gzip.open(gz, "wb") as fo: shutil.copyfileobj(fi, fo)
            try: os.remove(dst)
            except Exception: pass
            logger.info("Сжатие бэкапа: %s", gz)
            return gz
        return dst

    def backup_now(self) -> str|None:
        if self.is_sqlite:
            try: return self._backup_sqlite()
            except Exception as e: logger.error("Ошибка бэкапа: %s", e, exc_info=True); return None
        logger.info("Postgres: файловый бэкап не выполняется (no-op)."); return None

    def _list_backups(self) -> list[str]:
        if not os.path.isdir(self.backup_dir): return []
        return sorted([os.path.join(self.backup_dir, f)
                       for f in os.listdir(self.backup_dir)
                       if f.endswith(".bak") or f.endswith(".bak.gz")], reverse=True)

    def restore_latest(self) -> bool:
        if not self.is_sqlite:
            logger.info("Postgres: restore файлового бэкапа не поддерживается."); return False
        files = self._list_backups()
        if not files: logger.warning("Нет доступных бэкапов для восстановления."); return False
        latest = files[0]
        try:
            if latest.endswith(".gz"):
                unpack = latest[:-3]
                with gzip.open(latest, "rb") as fi, open(unpack, "wb") as fo: shutil.copyfileobj(fi, fo)
                latest = unpack
            shutil.copy2(latest, self.sqlite_path); logger.info("База восстановлена из бэкапа: %s", latest); return True
        except Exception as e:
            logger.error("Ошибка восстановления: %s", e, exc_info=True); return False

    def _worker(self):
        logger.info("Планировщик бэкапов запущен (интервал %s сек)", self.backup_interval)
        while not self._stop:
            try: self.backup_now()
            except Exception as e: logger.error("Ошибка планового бэкапа: %s", e, exc_info=True)
            for _ in range(self.backup_interval):
                if self._stop: break
                time.sleep(1)
        logger.info("Планировщик бэкапов остановлен.")

    def start(self):
        if self._t and self._t.is_alive(): return
        self._stop = False
        import threading as _th
        self._t = _th.Thread(target=self._worker, daemon=True); self._t.start()

    def stop(self, timeout: float = 5.0):
        self._stop = True
        if self._t:
            try: self._t.join(timeout=timeout)
            except Exception: pass
