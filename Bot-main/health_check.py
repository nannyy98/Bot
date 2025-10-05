"""
Система мониторинга здоровья бота (psutil необязателен)
"""
from __future__ import annotations
import os, time, threading
from datetime import datetime

try:
    import psutil  # type: ignore
except Exception:
    psutil = None

try:
    from config import MONITORING_CONFIG  # type: ignore
except Exception:
    MONITORING_CONFIG = {
        "health_check_interval": 60,
        "mem_mb_warn": 500,
        "cpu_percent_warn": 80,
        "health_port": int(os.getenv("PORT", "8080")),
    }

from logger import logger

class HealthMonitor:
    def __init__(self, db, bot):
        self.db, self.bot = db, bot
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

    def _cpu_usage_percent(self) -> float:
        if psutil:
            try: return float(psutil.cpu_percent(interval=0.25))
            except Exception: pass
        try:
            load1, _, _ = os.getloadavg()
            cpus = os.cpu_count() or 1
            return max(0.0, min(100.0, (load1 / cpus) * 100.0))
        except Exception:
            return 0.0

    def _memory_usage_mb(self) -> float:
        if psutil:
            try:
                p = psutil.Process(os.getpid())
                return float(p.memory_info().rss) / (1024 * 1024)
            except Exception: pass
        try:
            import resource
            rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return float(rss_kb) / 1024.0 if rss_kb else 0.0
        except Exception:
            return 0.0

    def start_monitoring(self):
        interval = int(MONITORING_CONFIG.get("health_check_interval", 60))
        def worker():
            while not self._stop:
                try:
                    self.update_metrics()
                    self.check_health()
                    time.sleep(interval)
                except Exception as e:
                    logger.error("Ошибка мониторинга: %s", e, exc_info=True)
                    time.sleep(60)
        threading.Thread(target=worker, daemon=True).start()
        logger.info("Система мониторинга запущена (interval=%s сек)", interval)

    def stop_monitoring(self): self._stop = True

    def update_metrics(self):
        self.metrics["memory_usage"] = self._memory_usage_mb()
        self.metrics["cpu_usage"] = self._cpu_usage_percent()
        try:
            self.db.execute_query("SELECT 1")
            self.metrics["database_status"] = "healthy"
        except Exception as e:
            self.metrics["database_status"] = "error"
            logger.error("Ошибка базы данных: %s", e)
        self.metrics["uptime_hours"] = (time.time() - self.metrics["start_time"]) / 3600.0

    def check_health(self):
        issues = []
        if self.metrics["memory_usage"] > float(MONITORING_CONFIG.get("mem_mb_warn", 500)):
            issues.append(f"Высокое потребление памяти: {self.metrics['memory_usage']:.1f}MB")
        if self.metrics["cpu_usage"] > float(MONITORING_CONFIG.get("cpu_percent_warn", 80)):
            issues.append(f"Высокая загрузка CPU: {self.metrics['cpu_usage']:.1f}%")
        if self.metrics["database_status"] != "healthy":
            issues.append("Проблемы с базой данных")
        if self.metrics["errors_count"] > 100:
            issues.append(f"Много ошибок: {self.metrics['errors_count']}")
        if issues: self.send_alert_to_admins(issues)

    def send_alert_to_admins(self, issues):
        try:
            admins = self.db.execute_query("SELECT telegram_id FROM users WHERE is_admin = 1") or []
            msg = "🚨 <b>СИСТЕМНОЕ ПРЕДУПРЕЖДЕНИЕ</b>\n\n" + "\n".join([f"• {x}" for x in issues])
            msg += "\n\n📊 Метрики:\n"
            msg += f"💾 Память: {self.metrics['memory_usage']:.1f}MB\n"
            msg += f"⚡ CPU: {self.metrics['cpu_usage']:.1f}%\n"
            msg += f"🕐 Аптайм: {self.metrics['uptime_hours']:.1f}ч\n"
            msg += f"📨 Сообщений: {self.metrics['messages_processed']}"
            for row in admins:
                admin_id = row[0] if isinstance(row, (list, tuple)) else row
                try: self.bot.send_message(admin_id, msg, parse_mode="HTML")
                except TypeError: self.bot.send_message(admin_id, msg)
        except Exception as e:
            logger.error("Ошибка отправки алерта: %s", e)

    def increment_messages(self): self.metrics["messages_processed"] += 1
    def increment_errors(self, err=None):
        self.metrics["errors_count"] += 1
        if err:
            self.metrics["last_error"] = {"message": str(err), "timestamp": datetime.now().isoformat()}

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
        from http.server import HTTPServer, BaseHTTPRequestHandler
        import json
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path != "/health":
                    self.send_response(404); self.end_headers(); return
                st = self.server.health_monitor.get_health_status()
                self.send_response(200 if st["status"] == "healthy" else 503)
                self.send_header("Content-Type", "application/json"); self.end_headers()
                self.wfile.write(json.dumps(st, indent=2).encode("utf-8"))
            def log_message(self, *_): pass
        def run():
            port = int(MONITORING_CONFIG.get("health_port", int(os.getenv("PORT", "8080"))))
            srv = HTTPServer(("0.0.0.0", port), Handler)
            srv.health_monitor = self
            logger.info("Health check сервер запущен на порту %s", port)
            srv.serve_forever()
        threading.Thread(target=run, daemon=True).start()
