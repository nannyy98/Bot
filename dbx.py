
"""
dbx.py — единый адаптер БД для Web и Bot (Postgres через SQLAlchemy).
Версия 1.1: executemany с dict, get_engine()/dispose().
Оставляйте в SQL знаки вопроса '?', адаптер сам преобразует их в именованные параметры для Postgres.
"""
from __future__ import annotations
import os
from typing import Any, Iterable, Dict, List, Tuple, Mapping
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL не задан. Создай переменную окружения в Render для Web и Bot "
        "(возьми значение из Internal Database URL)."
    )

_engine: Engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    future=True,
)

def get_engine() -> Engine:
    return _engine

def dispose() -> None:
    _engine.dispose()

def _qmark_to_named(sql: str, params: Iterable[Any] | Mapping[str, Any] | None):
    if params is None:
        return sql, {}
    if isinstance(params, Mapping):
        return sql, dict(params)
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

def run(sql: str, params: Iterable[Any] | Mapping[str, Any] | None = None) -> None:
    sql, bind = _qmark_to_named(sql, params)
    with _engine.begin() as con:
        con.execute(text(sql), bind)

def all(sql: str, params: Iterable[Any] | Mapping[str, Any] | None = None):
    sql, bind = _qmark_to_named(sql, params)
    with _engine.begin() as con:
        res = con.execute(text(sql), bind)
        return [dict(r._mapping) for r in res.fetchall()]

def one(sql: str, params: Iterable[Any] | Mapping[str, Any] | None = None):
    rows = all(sql, params)
    return rows[0] if rows else None

def scalar(sql: str, params: Iterable[Any] | Mapping[str, Any] | None = None):
    sql, bind = _qmark_to_named(sql, params)
    with _engine.begin() as con:
        return con.execute(text(sql), bind).scalar()

def executemany(sql: str, seq_params):
    with _engine.begin() as con:
        if '?' in sql:
            for params in seq_params:
                if isinstance(params, dict):
                    raise ValueError("Для SQL с '?' передавайте списки/кортежи, не dict.")
                q, bind = _qmark_to_named(sql, params)
                con.execute(text(q), bind)
        else:
            for params in seq_params:
                if not isinstance(params, dict):
                    raise ValueError("Для SQL с именованными параметрами передавайте dict.")
                con.execute(text(sql), dict(params))

def healthcheck() -> bool:
    try:
        return scalar("SELECT 1") == 1
    except Exception:
        return False
