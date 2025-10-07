
from __future__ import annotations
import os, logging
from typing import Any, Iterable, Dict, Tuple, List
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL не задан (Render → Environment). Укажите новую Postgres-базу.")

# normalize for SQLAlchemy 2 + psycopg3
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+psycopg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

engine: Engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

def _qmark_to_named(sql: str, params: Iterable[Any] | Dict[str, Any] | None) -> Tuple[str, Dict[str, Any]]:
    if params is None:
        return sql, {}
    if isinstance(params, dict):
        return sql, params
    named: Dict[str, Any] = {}
    out: list[str] = []
    i = 0
    for ch in sql:
        if ch == '?':
            key = f"p{i}"
            out.append(f":{key}")
            i += 1
        else:
            out.append(ch)
    for j, v in enumerate(params):
        named[f"p{j}"] = v
    return ''.join(out), named

def run(sql: str, params: Iterable[Any] | Dict[str, Any] | None = None) -> None:
    sql, bind = _qmark_to_named(sql, params)
    with engine.begin() as con:
        con.execute(text(sql), bind)

def all(sql: str, params: Iterable[Any] | Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    sql, bind = _qmark_to_named(sql, params)
    with engine.begin() as con:
        res = con.execute(text(sql), bind)
        return [dict(r._mapping) for r in res.fetchall()]

def one(sql: str, params: Iterable[Any] | Dict[str, Any] | None = None) -> Dict[str, Any] | None:
    rows = all(sql, params)
    return rows[0] if rows else None

def scalar(sql: str, params: Iterable[Any] | Dict[str, Any] | None = None):
    sql, bind = _qmark_to_named(sql, params)
    with engine.begin() as con:
        return con.execute(text(sql), bind).scalar()

def executemany(sql: str, seq_params: Iterable[Iterable[Any]]):
    with engine.begin() as con:
        for params in seq_params:
            q, bind = _qmark_to_named(sql, params)
            con.execute(text(q), bind)

def debug_log_connection() -> None:
    try:
        with engine.begin() as con:
            res = con.execute(text("select current_database(), inet_server_addr(), inet_server_port()")).first()
            db = res[0] if res else "?"
            host = str(res[1]) if res else "?"
            port = str(res[2]) if res else "?"
        dsn = os.getenv("DATABASE_URL", "")
        if dsn and "@" in dsn:
            scheme = dsn.split("://",1)[0]
            dsn = scheme + "://****:****@" + dsn.split("@",1)[1]
        logging.info(f"🔌 Подключение к БД: {db} @ {host}:{port}  (DSN={dsn})")
    except Exception as e:
        logging.info(f"⚠️ Не удалось получить информацию о подключении к БД: {e}")
