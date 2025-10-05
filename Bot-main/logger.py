"""
Система логирования для продакшена (совместимая со стандартным logging).

Особенности:
- Поддерживает те же сигнатуры методов, что и logging.Logger: debug/info/warning/error/critical/exception(msg, *args, **kwargs).
- Если вызывается как `logger.info("x=%s", 1)`, форматирование делается самим logging (совместимость).
- Доп. каналы: security_logger и perf_logger.
- Настройки берутся из config.LOGGING_CONFIG при наличии; либо применяются дефолты.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
from datetime import datetime

try:
    from config import LOGGING_CONFIG
except Exception:
    LOGGING_CONFIG = {}

_DEFAULT_LEVEL = LOGGING_CONFIG.get("level", "INFO")
_DEFAULT_FMT = LOGGING_CONFIG.get("format", "%(asctime)s %(levelname)s %(name)s: %(message)s")
_DEFAULT_DATEFMT = LOGGING_CONFIG.get("datefmt", "%Y-%m-%d %H:%M:%S")
_DEFAULT_LOG_DIR = LOGGING_CONFIG.get("dir", os.environ.get("LOG_DIR", "logs"))
_DEFAULT_FILE = LOGGING_CONFIG.get("file", "app.log")
_DEFAULT_MAX_BYTES = int(LOGGING_CONFIG.get("max_bytes", 1_000_000))
_DEFAULT_BACKUP_COUNT = int(LOGGING_CONFIG.get("backup_count", 5))

_LEVEL = getattr(logging, str(_DEFAULT_LEVEL).upper(), logging.INFO)

def _ensure_dir(path: str) -> None:
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

class ProductionLogger:
    """Обёртка над logging.Logger с безопасными сигнатурами."""
    def __init__(self, name: str = "shop_bot") -> None:
        self.name = name
        self.logger = logging.getLogger(name)
        if not self.logger.handlers:
            self._setup_logging()
        self.security_logger = logging.getLogger(f"{name}.security")
        self.perf_logger = logging.getLogger(f"{name}.perf")

    def _setup_logging(self) -> None:
        self.logger.setLevel(_LEVEL)

        fmt = logging.Formatter(_DEFAULT_FMT, datefmt=_DEFAULT_DATEFMT)

        # Console
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        sh.setLevel(_LEVEL)
        self.logger.addHandler(sh)

        # File (rotating)
        try:
            _ensure_dir(_DEFAULT_LOG_DIR)
            file_path = os.path.join(_DEFAULT_LOG_DIR, _DEFAULT_FILE)
            fh = logging.handlers.RotatingFileHandler(
                file_path, maxBytes=_DEFAULT_MAX_BYTES, backupCount=_DEFAULT_BACKUP_COUNT, encoding="utf-8"
            )
            fh.setFormatter(fmt)
            fh.setLevel(_LEVEL)
            self.logger.addHandler(fh)
        except Exception:
            # Файл-лог необязателен — не валимся при ошибке
            pass

        # Child loggers
        for child_name in ("security", "perf"):
            child = logging.getLogger(f"{self.name}.{child_name}")
            if not child.handlers:
                ch = logging.StreamHandler()
                ch.setFormatter(fmt)
                ch.setLevel(_LEVEL)
                child.addHandler(ch)
                child.setLevel(_LEVEL)

    # === Совместимые методы ===
    def debug(self, msg, *args, **kwargs):
        return self.logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        return self.logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        return self.logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        return self.logger.error(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        # Ставит exc_info=True по умолчанию, если не указан
        kwargs.setdefault("exc_info", True)
        return self.logger.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        return self.logger.critical(msg, *args, **kwargs)

    # === Дополнительные, безопасные вспомогательные методы ===
    def security(self, user_id, action, message="", **extra):
        """Логирование событий безопасности."""
        record = {
            "user_id": user_id,
            "action": action,
            "message": message,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        }
        # Передаём словарь в сообщение, чтобы не ломать форматтеры
        return self.security_logger.info("SECURITY %s", record, **extra)

    def performance(self, operation, duration, details=None, **extra):
        """Логирование производительности."""
        msg = {"operation": operation, "duration": float(duration), "details": details}
        return self.perf_logger.info("PERF %s", msg, **extra)

    def get_logger(self) -> logging.Logger:
        """При необходимости получить базовый logging.Logger"""
        return self.logger


# Экспорт совместимого экземпляра
logger = ProductionLogger()
