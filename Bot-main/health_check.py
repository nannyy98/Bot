"""
Система мониторинга здоровья бота (без жёсткой зависимости от psutil)
"""

from __future__ import annotations

import os
import time
import threading
import shutil
from datetime import datetime

# psutil опционален — если нет, используем фоллбеки
try:
    import psutil  # type: ignore
except Exception:
    psutil = None  # noqa: N816

# Конфиг мониторинга — берём из config, но безопасно подставляем дефолты
try:
    from config import MONITORING_CONFIG  # type: ignore
except Exception:
    MONITORING_CONFIG = {
        "health_check_interval": 30,  # сек
        "mem_mb_warn": 500,           # MB
        "cpu_percent_warn": 80,       # %
        "health_port": 8080,          # порт HTTP health-сервера
    }

from logger import logger


class HealthMonitor:
    def __init__(self, db, bot):
        self.db = db
        self.bot = bot
        self.metrics = {
            "start_time": time.time(),
            "messages_processed": 0,
            "errors_count": 0,
            "last_error": None,
            "database_status": "unknown",
            "memory_usage": 0.0,
            "cpu_usage": 0.0,
            "uptime_hours": 0.0,
        }
        self._stop = False
        self.start_monitoring()

    # ---------- системные метрики (с фоллбеками) ----------

    def _cpu_usage_percent(self) -> float:
        # Точный путь — через psutil
        if psutil:
            try:
                # небольшая пауза для усреднения
                return float(psutil.cpu_percent(interval=0.25))
            except Exception:
                pass
        # Фоллбек: loadavg -> приблизительный % загрузки
        try:
            load1, _, _ = os.getloadavg()
            cpus = os.cpu_count() or 1
            return max(0.0, min(100.0, (load1 / cpus) * 100.0))
        except Exception:
            return 0.0

    def _memory_usage_mb(self) -> float:
        # Точный путь — через psutil
        if psutil:
            try:
                p = psutil.Process(os.getpid())
                return float(p.memory_info().rss) / (1024 * 1024)
            except Exception:
                pass
        # Фоллбек: через resource (Linux)
        try:
            import resource  # type: ignore
            rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # На Linux ru_maxrss уже в килобайтах
            return float(rss_kb) / 1024.0 if rss_kb else 0.0
        except Exception:
            return 0.0

    # ---------- основной цикл мониторинга ----------

    def start_monitoring(self):
        """Запуск фонового мониторинга."""
        interval = int(MONITORING_CONFIG.get("health_check_interval", 30))

        def monitor_worker():
            while not self._stop:
                try:
                    self.update_metrics()
                    self.check_health()
                    time.sleep(interval)
                except Exception as e:
                    logger.error("Ошибка мониторинга: %s", e, exc_info=True)
                    time.sleep(60)

        monitor_thread = threading.Thread(target=monitor_worker, daemon=True)
        monitor_thread.start()
        logger.info("Система мониторинга запущена (interval=%s сек)", interval)

    def stop_monitoring(self):
        self._stop = True

    def update_metrics(self):
        """Обновление метрик."""
        # Системные метрики
        self.metrics["memory_usage"] = self._memory_usage_mb()
        self.metrics["cpu_usage"] = self._cpu_usage_percent()

        # Проверка базы данных
        try:
            self.db.execute_query("SELECT 1")
            self.metrics["database_status"] = "healthy"
        except Exception as e:
            self.metrics["database_status"] = "error"
            logger.error("Ошибка базы данных: %s", e)

        # Аптайм
        uptime = time.time() - self.metrics["start_time"]
        self.metrics["uptime_hours"] = uptime / 3600.0

    def check_health(self):
        """Проверка здоровья системы и алерты."""
        issues = []

        mem_warn = float(MONITORING_CONFIG.get("mem_mb_warn", 500))
        cpu_warn = float(MONITORING_CONFIG.get("cpu_percent_warn", 80))

        if self.metrics["memory_usage"] > mem_warn:
            issues.append(f"Высокое потребление памяти: {self.metrics['memory_usage']:.1f}MB (> {mem_warn}MB)")

        if self.metrics["cpu_usage"] > cpu_warn:
            issues.append(f"Высокая загрузка CPU: {self.metrics['cpu_usage']:.1f}% (> {cpu_warn}%)")

        if self.metrics["database_status"] != "healthy":
            issues.append("Проблемы с базой данных")

        if self.metrics["errors_count"] > 100:
            issues.append(f"Много ошибок: {self.metrics['errors_count']}")

        if issues:
            logger.warning("Проблемы со здоровьем системы: %s", "; ".join(issues))
            self.send_alert_to_admins(issues)

    def send_alert_to_admins(self, issues):
        """Отправка алертов админам."""
        try:
            admins = self.db.execute_query("SELECT telegram_id FROM users WHERE is_admin = 1") or []
            alert_lines = [
                "🚨 <b>СИСТЕМНОЕ ПРЕДУПРЕЖДЕНИЕ</b>",
                "",
                "Обнаружены проблемы:",
                *(f"• {issue}" for issue in issues),
                "",
                "📊 Метрики:",
                f"💾 Память: {self.metrics['memory_usage']:.1f}MB",
                f"⚡ CPU: {self.metrics['cpu_usage']:.1f}%",
                f"🕐 Время работы: {self.metrics['uptime_hours']:.1f}ч",
                f"📨 Сообщений: {self.metrics['messages_processed']}",
            ]
            alert_message = "\n".join(alert_lines)

            for (admin_id,) in admins:
                try:
                    # Если ваш бот поддерживает parse_mode
                    self.bot.send_message(admin_id, alert_message, parse_mode="HTML")
                except TypeError:
                    # На случай, если у бота нет parse_mode
                    self.bot.send_message(admin_id, alert_message)
        except Exception as e:
            logger.error("Ошибка отправки алерта: %s", e)

    def increment_messages(self):
        self.metrics["messages_processed"] += 1

    def increment_errors(self, error_message=None):
        self.metrics["errors_count"] += 1
        if error_message:
            self.metrics["last_error"] = {
                "message": error_message,
                "timestamp": datetime.now().isoformat(),
            }

    def get_health_status(self):
        return {
            "status": "healthy" if self.metrics["database_status"] == "healthy" else "unhealthy",
            "uptime": self.metrics["uptime_hours"],
            "memory_mb": self.metrics["memory_usage"],
            "cpu_percent": self.metrics["cpu_usage"],
            "messages_processed": self.metrics["messages_processed"],
            "errors_count": self.metrics["errors_count"],
            "database_status": self.metrics["database_status"],
        }

    def create_health_endpoint(self):
        """Создание HTTP endpoint /health (порт из MONITORING_CONFIG)."""
        from http.server import HTTPServer, BaseHTTPRequestHandler
        import json

        class HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/health":
                    health_status = self.server.health_monitor.get_health_status()
                    self.send_response(200 if health_status["status"] == "healthy" else 503)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(health_status, indent=2).encode("utf-8"))
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, format, *args):
                # Тихий сервер
                pass

        def start_health_server():
            port = int(MONITORING_CONFIG.get("health_port", 8080))
            try:
                server = HTTPServer(("0.0.0.0", port), HealthHandler)
                server.health_monitor = self
                logger.info("Health check сервер запущен на порту %s", port)
                server.serve_forever()
            except Exception as e:
                logger.error("Ошибка запуска health check сервера: %s", e)

        threading.Thread(target=start_health_server, daemon=True).start()
