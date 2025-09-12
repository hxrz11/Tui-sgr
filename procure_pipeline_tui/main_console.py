#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Procurement Pipeline — консольный вариант (Stage 0 → План).

Проверяет доступность Postgres и Ollama, запрашивает у пользователя
вопрос и просит модель построить План (до 3 шагов).
Все события логируются в ``./logs/<timestamp>_<session>.jsonl``.
"""
from __future__ import annotations

import os
import json
import uuid
import datetime as dt
from typing import Any, Optional, Tuple, List, Dict
from pathlib import Path
import sys
import time
import threading
from itertools import cycle
import re

import requests
import psycopg2
from dotenv import load_dotenv
import sqlparse
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table



# ------------------------------
# Config
# ------------------------------

load_dotenv()

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
MODEL_NAME = os.getenv("MODEL_NAME", "llama3.1:70b-instruct-q4_K_M")
OLLAMA_KEEP_ALIVE = "5m"
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "180"))
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/postgres")
LOG_DIR = os.getenv("LOG_DIR", "logs")
STATUS_API_URL = os.getenv("STATUS_API_URL", "http://localhost:8000")

os.makedirs(LOG_DIR, exist_ok=True)

console = Console()

# Описание структуры public."PurchaseAllView"
DB_SCHEMA: Dict[str, str] = {
    "GlobalUid": "uuid",
    "PurchaseCardId": "uuid",
    "PurchaseRecordStatus": "text",
    "PurchaseNumber": "text",
    "PurchaseCardDate": "timestamp",
    "PurchaseCardUserFio": "text",
    "OrderNumber": "text",
    "OrderDate": "timestamp",
    "ApprovalDate": "timestamp",
    "ObjectName": "text",
    "Nomenclature": "text",
    "NomenclatureFullName": "text",
    "ArtNumber": "text",
    "Quantity": "numeric",
    "RemainingQuantity": "numeric",
    "ProcessedQuantity": "numeric",
    "UnitName": "text",
    "ProcessingDate": "timestamp",
    "CompletedDate": "timestamp",
    "UserName": "text",
    "Notes": "text",
    "ArchiveStatus": "text",
    "Category": "text",
    "NomenclatureGroup": "text",
    "OKPD2Name": "text",
    "OKPD2Code": "text",
}


COMMON_CONTEXT = (
    "Ты — аналитик по закупкам. Отвечай и генерируй планы строго по данным.\n\n"
    "Домен:\n"
    '- public."PurchaseAllView" — это ЗАКАЗЫ по НОМЕНКЛАТУРАМ (операционная система). Несколько строк заказа могут относиться к одной ЗАКУПКЕ.\n'
    '- "PurchaseCardId" (может быть NULL) — служебная связка для внешнего STATUS API (для получения статусов).\n'
    '- "PurchaseNumber" (text) — Номер закупки (понятен пользователю). Для ссылок на закупку в текстах используй "PurchaseNumber", а не "PurchaseCardId".\n'
    '- Цен нет — работаем с количествами, датами и статусами.\n\n'
    "Поля, понятные пользователю (whitelist для проекций/ответов):\n"
    '(таблица public."PurchaseAllView" / ЗАКАЗЫ)\n'
    '- "OrderNumber" (text)                   — Номер заявки, напр. ЛГ000000524\n'
    '- "OrderDate" (timestamp)                — Дата создания заявки\n'
    '- "ApprovalDate" (timestamp)             — Дата утверждения заявки\n'
    '- "ObjectName" (text)                    — Название объекта строительства\n'
    '- "Nomenclature" (text)                  — Краткое название номенклатуры\n'
    '- "NomenclatureFullName" (text)          — Полное название номенклатуры\n'
    '- "ArtNumber" (text)                     — Артикул\n'
    '- "Quantity" (numeric)                   — Количество заказанное\n'
    '- "RemainingQuantity" (numeric)          — Остаток количества\n'
    '- "ProcessedQuantity" (numeric)          — Обработанное количество\n'
    '- "UnitName" (text)                      — Ед. изм. (шт, м, кг...)\n'
    '- "ProcessingDate" (timestamp)           — Дата обработки\n'
    '- "CompletedDate" (timestamp)            — Дата завершения\n'
    '- "UserName" (text)                      — Логин пользователя\n'
    '- "Notes" (text)                         — Примечания\n'
    '- "ArchiveStatus" (text)                 — Статус архивации\n'
    '(ЗАКУПКИ)\n'
    '- "PurchaseRecordStatus" (text)          — Статус записи закупки (A=активный)\n'
    '- "PurchaseNumber" (text)                — Номер закупки\n'
    '- "PurchaseCardDate" (timestamp)         — Дата создания карточки закупки\n'
    '- "PurchaseCardUserFio" (text)           — ФИО пользователя\n\n'
    "SQL-ограничения (для всех шагов с SQL):\n"
    '- Только SELECT из public."PurchaseAllView". Никаких джойнов/CTE/DML/других таблиц.\n'
    '- Всегда двойные кавычки для имён. Запрещён SELECT * — проектируй ТОЛЬКО поля из whitelist ИЛИ реально существующие поля из переданной схемы.\n'
    '- Обязателен фильтр активных записей: WHERE "PurchaseRecordStatus"=\'A\' (если явно не сказано иначе).\n'
    '- Дедупликация по "GlobalUid" через ROW_NUMBER() OVER (...) AS rn, фильтруй rn=1. Приоритет выбора: NOT NULL "PurchaseCardId"; затем по времени: "ProcessingDate" DESC, "CompletedDate" DESC, "ApprovalDate" DESC (NULLS LAST).\n'
    '- Формат дат для пользовательских представлений: to_char(...,\'DD-MM-YYYY\') AS "<FieldFmt>".\n'
    '- Если LIMIT не задан — добавь LIMIT 100 и считаем это срезкой.\n\n'
    "Поиск по номенклатуре (стемминг/категории):\n"
    "- Используй русскую основу слова (стемминг). Шаблон поиска '%<stem>%'. Пример: «ленолеум» → основа 'линол'.\n"
    "- Исключай ложные совпадения по смыслу: пример — при поиске «линолеум» НЕ включай позиции типа «клей для линолеума», «смывка для линолеума» и т.п. (добавь отрицательные условия по словам: 'клей', 'смывк', 'шпат', и т.д. — по ситуации).\n"
    "- Обобщённые запросы (напр. «весь инструмент»/«все инструменты»):\n"
    '  1) В приоритете фильтр по полям категорий (если есть в схеме): "Category", "NomenclatureGroup", "OKPD2Name", "OKPD2Code".\n'
    "  2) Если таких полей нет, применяй набор стемов к \"Nomenclature\"/\"NomenclatureFullName\" (инструм, перфорат, шуруповер, дрел, отвертк, пила ...), исключая ложные совпадения (например, NOT ILIKE '%инструкц%').\n\n"
    "Статусы и история:\n"
    '- STATUS API вызывается только при необходимости статусов.\n'
    '- «Текущий статус» закупки — ПОСЛЕДНЕЕ по времени событие из STATUS API по соответствующей карточке; укажи дату достижения.\n'
    '- Историю статусов показывай ТОЛЬКО если пользователь прямо спросил про историю/этапы/таймлайн (и ограниченно, без списка на десятки записей).\n\n'
    "Фокус на вопрос пользователя:\n"
    '- Всегда сначала отвечай на конкретный вопрос, а затем (по желанию) добавляй 1 уровень контекстной детали, напрямую объясняющей ответ (но без «аналитики ради аналитики»).\n'
    '- Не перегружай техническими полями — используй понятные пользователю поля из списка выше.\n'
    '- Если результаты урезаны LIMIT — явно укажи, что это срезка.\n\n'
    "Проверка полей:\n"
    '- Опирайся на фактическую схему, переданную в промпте. НИКОГДА не используй поля, которых нет в схеме.\n'
    '- Если требуется поле из whitelist, но его нет в схеме — просто не используй его (без выдумывания).\n'
)

def fetch_purchaseallview_schema() -> str:
    """Fetch column names and types for public."PurchaseAllView"."""
    query = (
        "SELECT column_name, data_type "
        "FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = 'PurchaseAllView' "
        "ORDER BY ordinal_position"
    )
    conn = None
    try:
        conn = psycopg2.connect(POSTGRES_DSN)
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            return "\n".join(f"{name} {dtype}" for name, dtype in rows)
    finally:
        if conn:
            conn.close()

# ------------------------------
# Prompts
# ------------------------------

PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"

PLAN_SYSTEM_PROMPT = (PROMPTS_DIR / "plan_system_prompt.txt").read_text(encoding="utf-8")
PLAN_USER_PROMPT_TEMPLATE = (PROMPTS_DIR / "plan_user_prompt_template.txt").read_text(encoding="utf-8")


FIX_SQL_SYSTEM_PROMPT = (
    COMMON_CONTEXT
    + "Используй COMMON CONTEXT. Ты исправляешь ТОЛЬКО SELECT к public.\"PurchaseAllView\".\n"
      "Правила:\n"
      "- Сохрани намеренную проекцию полей (если передан список intended_columns). Если каких-то полей нет в схеме — опусти только их, остальные сохрани.\n"
      "- Не добавляй несуществующие поля. Проекция должна соответствовать реальной схеме.\n"
      "- WHERE \"PurchaseRecordStatus\"='A' (если явно не требуется иное), дедупликация rn=1, форматы дат (to_char AS \"<FieldFmt>\"), стемминг/исключения по номенклатуре, LIMIT 100 если не указан.\n"
      "Верни ТОЛЬКО исправленный SQL (без пояснений).\n"
)

FIX_SQL_USER_PROMPT_TEMPLATE = (
    "schema_json:\n{schema_json}\n\n"
    "intended_columns (если есть, чтобы сохранить состав полей):\n{intended_columns_json}\n\n"
    "question:\n{question}\n\n"
    "bad_sql:\n{bad_sql}\n\n"
    "db_error_or_logic_error:\n{error}\n\n"
    "Верни только исправленный SQL."
)

SYNTHESIS_SYSTEM_PROMPT = (
    COMMON_CONTEXT
    + "Используй COMMON CONTEXT. Отвечай строго по данным. Принципы:\n"
      "- Сначала дай прямой ответ на вопрос пользователя.\n"
      "- Затем добавь 1 уровень короткой поясняющей детали, напрямую связанной с ответом.\n"
      "- Если речь о закупке — используйте \"PurchaseNumber\" для идентификации в тексте (НЕ \"PurchaseCardId\").\n"
      "- Историю статусов показывай только если пользователь прямо спросил про историю/этапы/таймлайн (и ограниченно, без списка на десятки записей).\n"
      "- Если \"PurchaseNumber\" отсутствует, не придумывай; сообщи факт отсутствия, статусы тогда неизвестны.\n"
      "- Формат дат в тексте: DD-MM-YYYY. Если выборка урезана LIMIT — упомяни, что это срезка.\n"
)

SYNTHESIS_USER_PROMPT_TEMPLATE = (
    "Вопрос пользователя:\n<<<{question}>>>\n\n"
    "Справочно (не для показа пользователю):\n"
    "schema_json: {schema_json}\n"
    "sql_query: {sql_query}\n\n"
    "Результаты SQL (после rn=1):\n{sql_rows_json}\n\n"
    "Статусы из STATUS API (по карточкам; может быть пусто):\n{statuses_json}\n\n"
    "limit_note (пусто, если нет): {limit_note}\n\n"
    "СФОРМИРУЙ ОТВЕТ:\n"
    "- Ответь по сути вопроса. Если спрашивали «сколько» — укажи количество и поясни, чего именно (строки заказов, уникальные закупки/номера).\n"
    "- Если спрашивали про статус — укажи текущий статус(ы) по соответствующим \"PurchaseNumber\" и дату достижения.\n"
    "- Если вопрос не про историю — не выводи длинные таймлайны.\n"
    "- Добавь уместные детали первого уровня (напр., объект, ключевые даты, единицы измерения/количества).\n"
    "- Если данных нет — скажи об этом. Если есть срезка LIMIT — напомни об этом.\n"
    "- Не показывай SQL/схему/служебные поля.\n"
)

REVIEW_SYSTEM_PROMPT = (
    COMMON_CONTEXT
    + "Используй COMMON CONTEXT. Сформируй 1–2 предложения, которые:\n"
      "- Резюмируют ОТВЕТ НА ВОПРОС пользователя (а не общую аналитику).\n"
      "- При необходимости упоминают \"PurchaseNumber\" и текущий статус (если это релевантно вопросу).\n"
      "- Не включают историю статусов, если её не спрашивали.\n"
      "- Кратко отмечают срезку (LIMIT), если она была.\n"
      "Без технических терминов и SQL.\n"
)

REVIEW_USER_PROMPT_TEMPLATE = (
    "Вопрос пользователя:\n<<<{question}>>>\n\n"
    "Финальный ответ (показанный пользователю):\n{final_answer_text}\n\n"
    "Контекст (для ориентира, не цитировать):\n"
    "plan_json: {plan_json}\n"
    "sql_query: {sql_query}\n"
    "api_responses_json: {api_responses_json}\n"
    "limit_note: {limit_note}\n\n"
    "СФОРМИРУЙ РЕЗЮМЕ:\n"
    "- 1–2 предложения, по сути вопроса.\n"
    "- Можно упомянуть ключевые числа/номера закупок/текущие статусы, если это помогает понять ответ.\n"
    "- Без техдеталей и истории, если её не просили.\n"
)

# ------------------------------
# Utils
# ------------------------------

def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")

def new_session_id() -> str:
    return uuid.uuid4().hex[:12]

def log_path(session_id: str) -> str:
    ts = dt.datetime.now().strftime("%Y%m%dT%H%M%S")
    return os.path.join(LOG_DIR, f"{ts}_{session_id}.jsonl")

def write_log(path: str, kind: str, payload: Any) -> None:
    entry = {"ts": now_iso(), "kind": kind, "payload": payload}
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def run_with_spinner(label: str, func, *args, **kwargs):
    """Run ``func`` displaying a spinner with ``label`` until completion."""
    spinner = cycle("|/-\\")
    start = time.monotonic()
    done = False

    def spin() -> None:
        while not done:
            sys.stdout.write(f"{label} {next(spinner)}\r")
            sys.stdout.flush()
            time.sleep(0.1)

    t = threading.Thread(target=spin)
    t.start()
    ok = True
    try:
        result = func(*args, **kwargs)
        if isinstance(result, bool):
            ok = result
        elif isinstance(result, tuple) and result:
            ok = bool(result[0])
    except Exception as e:
        ok = False
        result = e
    finally:
        done = True
        t.join()
        elapsed = time.monotonic() - start
        sys.stdout.write(" " * (len(label) + 4) + "\r")
        tag = "[OK]" if ok else "[FAIL]"
        print(f"{label} — {tag} ({elapsed:.2f}s)")
    if not ok and isinstance(result, Exception):
        raise result
    return result


def strip_sql_block(text: Optional[str]) -> str:
    """Удаляет обрамление ```sql``` из ответа LLM, если оно присутствует."""
    if not text:
        return ""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:sql)?\s*\n", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\n```$", "", text)
    return text.strip()


def db_check(dsn: str, log_file: str) -> Tuple[bool, str]:
    query = "SELECT 1"
    try:
        conn = psycopg2.connect(dsn)
        try:
            with conn.cursor() as cur:
                try:
                    cur.execute(query)
                    row = cur.fetchone()
                    result_val = row[0] if row else None
                    write_log(
                        log_file,
                        "sql_exec",
                        {"query": query, "result": result_val, "error": None},
                    )
                    ok = result_val == 1
                except Exception as e:
                    write_log(
                        log_file,
                        "sql_exec",
                        {"query": query, "result": None, "error": str(e)},
                    )
                    raise
        finally:
            conn.close()
        return ok, "OK"
    except Exception as e:
        return False, str(e)

def ollama_check(base_url: str, model: str) -> Tuple[bool, str, Optional[dict]]:
    try:
        resp = requests.get(base_url.rstrip('/') + "/api/tags", timeout=30)
        resp.raise_for_status()
        data = resp.json()
        models = {m.get("name"): m for m in data.get("models", [])}
        if model in models:
            return True, "OK", models[model]
        else:
            return False, f"Модель {model} не найдена на сервере", None
    except Exception as e:
        return False, str(e), None

def call_ollama_plan(
    question: str, schema_json: str, log_file: str
) -> Tuple[dict, dict, List[Dict[str, str]]]:
    """Возвращает (plan_json, meta, steps_preview) и логирует запрос."""
    user_prompt = PLAN_USER_PROMPT_TEMPLATE.format(
        question=question, schema_json=schema_json
    )
    url = OLLAMA_URL.rstrip('/') + "/api/generate"
    body = {
        "model": MODEL_NAME,
        "prompt": user_prompt,
        "system": PLAN_SYSTEM_PROMPT,
        "stream": False,
        "keep_alive": OLLAMA_KEEP_ALIVE,
        "options": {"temperature": 0.1, "num_ctx": 8192},
        "response_format": {
            "type": "json_schema",
            "schema": {
                "type": "object",
                "properties": {"steps": {"type": "array"}},
            },
        },
    }
    write_log(log_file, "llm_request", {"url": url, "body": body})

    resp = requests.post(url, json=body, timeout=OLLAMA_TIMEOUT)
    status_code = resp.status_code
    resp_text = resp.text
    write_log(
        log_file,
        "llm_response",
        {"status_code": status_code, "text": resp_text},
    )
    try:
        resp.raise_for_status()
        data = resp.json()
        raw = (data.get("response") or "").strip()

        json_text = raw
        if json_text.startswith("```"):
            json_text = json_text.strip("`\n ")
            json_text = json_text.replace("json\n", "").replace("javascript\n", "")
        try:
            plan = json.loads(json_text)
        except json.JSONDecodeError:
            if '"' not in json_text and "'" in json_text:
                plan = json.loads(json_text.replace("'", '"'))
            else:
                raise
    except requests.HTTPError:
        write_log(
            log_file,
            "llm_response_error",
            {"status_code": status_code, "text": resp_text},
        )
        raise
    except json.JSONDecodeError:
        write_log(
            log_file,
            "llm_response_error",
            {"status_code": status_code, "text": resp_text},
        )
        raise

    if not isinstance(plan, dict) or not isinstance(plan.get("steps"), list):
        raise ValueError("LLM response must be a dict with 'steps' list")

    previews: List[Dict[str, str]] = []
    for i, step in enumerate(plan.get("steps", [])):
        step_id = step.get("id", str(i + 1))
        previews.append({"id": step_id, "json": json.dumps(step, ensure_ascii=False, indent=2)})

    meta = {
        "model": data.get("model", MODEL_NAME),
        "options": body.get("options"),
        "eval_count": data.get("eval_count"),
        "prompt_eval_count": data.get("prompt_eval_count"),
        "total_duration": data.get("total_duration"),
        "eval_duration": data.get("eval_duration"),
        "prompt_eval_duration": data.get("prompt_eval_duration"),
        "prompt_chars": len(body.get("prompt", "")),
        "response_chars": len(raw),
    }
    return plan, meta, previews


SQL_SYSTEM_PROMPT = (
    COMMON_CONTEXT
    + "Жёсткие правила SQL:\n"
    "- Разрешены только SELECT к public.\"PurchaseAllView\". Никаких CTE/DML/других таблиц.\n"
    "- Имена таблиц/полей — в двойных кавычках.\n"
    "- Без * — только явные поля.\n"
    "- По умолчанию фильтр: WHERE \"PurchaseRecordStatus\"='A'. Другие статусы — только по явному запросу.\n"
    "- Дедупликация \"версий\" по \"GlobalUid\": оставить ровно одну строку по приоритету:\n"
    "  1) строка с непустым \"PurchaseCardId\";\n"
    "  2) затем наиболее поздняя \"ProcessingDate\", затем \"CompletedDate\", затем \"ApprovalDate\" (DESC NULLS LAST).\n"
    "  Использовать ROW_NUMBER() OVER (...) и фильтровать rn = 1.\n"
    "- Поиск по тексту — только ILIKE с основой слова '%<stem>%'. Для номенклатуры искать по двум полям:\n"
    "  (\"Nomenclature\" ILIKE '%<stem>%' OR \"NomenclatureFullName\" ILIKE '%<stem>%').\n"
    "- Формат дат для пользователя — DD-MM-YYYY: to_char(...,'DD-MM-YYYY') AS \"<FieldFmt>\".\n"
    "- Если LIMIT не задан — использовать LIMIT 100 и явно писать, что это срезка.\n"
    "- \"ArchiveStatus\" учитывать только если пользователь явно запросил.\n"
)


def call_ollama_sql(
    question: str, steps: List[Dict[str, Any]], log_file: str
) -> str:
    """Генерирует SQL на основе плана и вопроса."""
    user_prompt = (
        f"question:\n{question}\n\n"
        f"plan_steps_json:\n{json.dumps(steps, ensure_ascii=False)}\n\n"
        "Верни только SQL."
    )
    url = OLLAMA_URL.rstrip('/') + "/api/generate"
    body = {
        "model": MODEL_NAME,
        "prompt": user_prompt,
        "system": SQL_SYSTEM_PROMPT,
        "stream": False,
        "keep_alive": OLLAMA_KEEP_ALIVE,
    }
    write_log(log_file, "llm_sql_request", {"url": url, "body": body})

    resp = requests.post(url, json=body, timeout=OLLAMA_TIMEOUT)
    status_code = resp.status_code
    resp_text = resp.text
    write_log(
        log_file,
        "llm_sql_response",
        {"status_code": status_code, "text": resp_text},
    )
    resp.raise_for_status()
    data = resp.json()
    text = strip_sql_block(data.get("response"))
    return text


def call_ollama_fix_sql(
    question: str,
    schema: str,
    bad_sql: str,
    error: str,
    log_file: str,
    intended_columns: Optional[List[str]] = None,
) -> str:
    """Ask LLM to fix SQL based on schema and error message."""
    user_prompt = FIX_SQL_USER_PROMPT_TEMPLATE.format(
        schema_json=schema,
        intended_columns_json=json.dumps(intended_columns or []),
        question=question,
        bad_sql=bad_sql,
        error=error,
    )
    url = OLLAMA_URL.rstrip('/') + "/api/generate"
    body = {
        "model": MODEL_NAME,
        "prompt": user_prompt,
        "system": FIX_SQL_SYSTEM_PROMPT,
        "stream": False,
        "keep_alive": OLLAMA_KEEP_ALIVE,
    }
    write_log(log_file, "llm_fix_request", {"url": url, "body": body})

    resp = requests.post(url, json=body, timeout=OLLAMA_TIMEOUT)
    status_code = resp.status_code
    resp_text = resp.text
    write_log(
        log_file,
        "llm_fix_response",
        {"status_code": status_code, "text": resp_text},
    )
    resp.raise_for_status()
    data = resp.json()
    text = strip_sql_block(data.get("response"))
    return text


def execute_sql(
    query: str, log_file: str, limit: int = 10
) -> Tuple[List[Dict[str, Any]], int]:
    """Выполняет безопасный SELECT к Postgres и логирует результат."""
    query = strip_sql_block(query)
    stripped = query.strip().strip(";")
    if not stripped.upper().startswith("SELECT"):
        raise ValueError("Запрос должен начинаться с SELECT")

    pattern = re.compile(r"\bFROM\s+([^\s,;]+)|\bJOIN\s+([^\s,;]+)", re.IGNORECASE)
    tables = set()
    for match in pattern.finditer(stripped):
        tbl = match.group(1) or match.group(2)
        if tbl and not tbl.startswith("("):
            tables.add(tbl.strip())
    if tables and tables != {"public.\"PurchaseAllView\""}:
        raise ValueError('Разрешена только таблица public."PurchaseAllView"')

    conn = None
    try:
        conn = psycopg2.connect(POSTGRES_DSN)
        with conn.cursor() as cur:
            wrapped_query = (
                f"SELECT *, COUNT(*) OVER() AS total FROM ({stripped}) t LIMIT {limit}"
            )
            cur.execute(wrapped_query)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            result = [dict(zip(cols, r)) for r in rows]
            total_count = result[0]["total"] if result else 0
            for row in result:
                row.pop("total", None)

            write_log(
                log_file,
                "sql_exec",
                {
                    "query": stripped,
                    "result": result,
                    "total_count": total_count,
                    "error": None,
                },
            )
            return result, total_count
    except Exception as e:
        write_log(
            log_file,
            "sql_exec",
            {"query": stripped, "result": None, "error": str(e)},
        )
        raise
    finally:
        if conn:
            conn.close()


def fetch_statuses(card_id: str) -> dict:
    """Выполняет GET запрос к STATUS_API_URL для получения таймлайна статусов."""
    url = STATUS_API_URL.rstrip('/') + "/webhook/relay"
    params = {"id": card_id}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def call_ollama_synthesis(
    question: str,
    schema: Dict[str, str],
    sql_query: str,
    sql_rows: List[Dict[str, Any]],
    statuses: List[Dict[str, Any]],
    limit_note: str,
    log_file: str,
) -> str:
    """Запрашивает у Ollama итоговый ответ и логирует запрос/ответ."""
    user_prompt = SYNTHESIS_USER_PROMPT_TEMPLATE.format(
        question=question,
        schema_json=json.dumps(schema, ensure_ascii=False),
        sql_query=sql_query,
        sql_rows_json=json.dumps(sql_rows, ensure_ascii=False),
        statuses_json=json.dumps(statuses, ensure_ascii=False),
        limit_note=limit_note,
    )

    url = OLLAMA_URL.rstrip('/') + "/api/generate"
    body = {
        "model": MODEL_NAME,
        "prompt": user_prompt,
        "system": SYNTHESIS_SYSTEM_PROMPT,
        "stream": False,
        "keep_alive": OLLAMA_KEEP_ALIVE,
    }
    write_log(log_file, "llm_synth_request", {"url": url, "body": body})

    resp = requests.post(url, json=body, timeout=OLLAMA_TIMEOUT)
    status_code = resp.status_code
    resp_text = resp.text
    write_log(
        log_file,
        "llm_synth_response",
        {"status_code": status_code, "text": resp_text},
    )
    resp.raise_for_status()
    data = resp.json()
    text = (data.get("response") or "").strip()
    return text


def call_ollama_review(
    question: str,
    plan_json: dict | None,
    sql_query: str | None,
    answer: str,
    api_responses: List[Dict[str, Any]],
    limit_note: str,
    log_file: str,
) -> str:
    """Отправляет модели все артефакты и возвращает финальный обзор.

    Логирует запрос и ответ для трассировки.
    """
    user_prompt = REVIEW_USER_PROMPT_TEMPLATE.format(
        question=question,
        final_answer_text=answer,
        plan_json=json.dumps(plan_json, ensure_ascii=False) if plan_json is not None else "null",
        sql_query=sql_query or "",
        api_responses_json=json.dumps(api_responses, ensure_ascii=False),
        limit_note=limit_note,
    )

    url = OLLAMA_URL.rstrip('/') + "/api/generate"
    body = {
        "model": MODEL_NAME,
        "prompt": user_prompt,
        "system": REVIEW_SYSTEM_PROMPT,
        "stream": False,
        "keep_alive": OLLAMA_KEEP_ALIVE,
    }
    write_log(log_file, "llm_review_request", {"url": url, "body": body})

    resp = requests.post(url, json=body, timeout=OLLAMA_TIMEOUT)
    status_code = resp.status_code
    resp_text = resp.text
    write_log(
        log_file,
        "llm_review_response",
        {"status_code": status_code, "text": resp_text},
    )
    resp.raise_for_status()
    data = resp.json()
    text = (data.get("response") or "").strip()
    return text


def render_plan(plan: dict) -> None:
    sep = "-" * 30
    console.print(sep)
    for i, step in enumerate(plan.get("steps", []), 1):
        console.print(f"шаг{i}: {step.get('title', '')}")
        console.print(step.get("description", ""))
        sql = step.get("sql")
        if sql:
            formatted = sqlparse.format(
                sql, reindent=True, keyword_case="upper"
            )
            syntax = Syntax(formatted, "sql", theme="monokai")
            console.print(syntax)
        console.print(sep)

# ------------------------------
# Console workflow
# ------------------------------

class PipelineCLI:
    def __init__(self) -> None:
        self.session_id: str = new_session_id()
        self.log_file: str = log_path(self.session_id)
        write_log(self.log_file, "session", {"session_id": self.session_id})
        self.db_ok: bool = False
        self.db_msg: str = ""
        self.llm_ok: bool = False
        self.llm_msg: str = ""
        self.llm_model_info: Optional[dict] = None
        self.question: str = ""
        self.plan: Optional[dict] = None
        self.llm_meta: Optional[dict] = None

    # environment checks
    def run_checks(self) -> None:
        print("Environment checks")

        ok, msg = run_with_spinner(
            "DB: проверка соединения", db_check, POSTGRES_DSN, self.log_file
        )
        self.db_ok, self.db_msg = ok, msg
        write_log(self.log_file, "db_check", {"ok": ok, "msg": msg})

        ok2, msg2, info = run_with_spinner(
            "Ollama: проверка", ollama_check, OLLAMA_URL, MODEL_NAME
        )
        self.llm_ok, self.llm_msg, self.llm_model_info = ok2, msg2, info
        write_log(
            self.log_file, "ollama_check", {"ok": ok2, "msg": msg2, "model_info": info}
        )
        if info:
            print(
                f"Модель: {info.get('name')} | Сайз: {info.get('size', 'n/a')}"
            )

    # main action
    def do_start(self) -> None:
        if not self.db_ok or not self.llm_ok:
            print("Проверки не пройдены. Исправьте окружение и попробуйте снова.")
            return

        print("Schema/Plan Generation")

        # Question input
        q = input("Введите точный вопрос: ").strip()
        if not q:
            print("Вопрос пустой, ничего не делаем.")
            return
        self.question = q
        write_log(self.log_file, "question", {"text": q})
        print(f"Вопрос: {q}")

        schema_json = json.dumps(DB_SCHEMA, ensure_ascii=False)
        try:
            plan, meta, _previews = run_with_spinner(
                "Запрашиваю у LLM план действий", call_ollama_plan, q, schema_json, self.log_file
            )
            self.plan = plan
            self.llm_meta = meta
            write_log(self.log_file, "plan", plan)
            write_log(self.log_file, "llm_meta", meta)
            print("План от llm получен!")
            render_plan(plan)

            card_ids: List[str] = []
            sql_rows: List[Dict[str, Any]] = []
            status_results: List[Dict[str, Any]] = []
            sql_query: Optional[str] = None
            steps_list = plan.get("steps", [])

            try:
                sql_query = run_with_spinner(
                    "Генерирую SQL",
                    call_ollama_sql,
                    self.question,
                    steps_list,
                    self.log_file,
                )
                write_log(
                    self.log_file,
                    "sql_generated",
                    {"sql": sql_query, "steps": steps_list},
                )
            except Exception as e:
                write_log(
                    self.log_file,
                    "error",
                    {"stage": "sql_gen", "error": str(e)},
                )
                console.print(f"Ошибка генерации SQL: {e}")
                return

            intended_columns = []
            if steps_list:
                intended_columns = (
                    steps_list[0].get("details", {}).get("projection_fields") or []
                )

            try:
                rows, total_count = run_with_spinner(
                    "Выполняю SQL", execute_sql, sql_query, self.log_file
                )
                if rows:
                    sql_rows = rows
                    table = Table(show_header=True, header_style="bold")
                    for col in rows[0].keys():
                        table.add_column(col)
                    for row in rows:
                        table.add_row(
                            *[str(row.get(col, "")) for col in rows[0].keys()]
                        )
                    console.print(table)
                    card_ids = [
                        str(r.get("PurchaseCardId"))
                        for r in rows
                        if r.get("PurchaseCardId")
                    ]
                    card_ids = list(dict.fromkeys(card_ids))
                else:
                    console.print("SQL не вернул данных.")
                console.print(
                    f"Всего в ответе {total_count} строк, выше первые {len(rows)}."
                )
            except Exception as e:
                write_log(
                    self.log_file,
                    "error",
                    {"stage": "sql_exec", "error": str(e)},
                )
                console.print(f"Ошибка выполнения SQL: {e}")
                try:
                    schema_text = run_with_spinner(
                        "Получаю схему", fetch_purchaseallview_schema
                    )
                    write_log(
                        self.log_file,
                        "schema_description",
                        {"text": schema_text},
                    )
                    fixed_sql = run_with_spinner(
                        "Исправляю SQL",
                        call_ollama_fix_sql,
                        self.question,
                        schema_text,
                        sql_query,
                        str(e),
                        self.log_file,
                        intended_columns=intended_columns,
                    )
                    write_log(
                        self.log_file,
                        "sql_fix",
                        {
                            "original_sql": sql_query,
                            "fixed_sql": fixed_sql,
                            "error": str(e),
                        },
                    )
                    try:
                        rows, total_count = run_with_spinner(
                            "Повторное выполнение SQL",
                            execute_sql,
                            fixed_sql,
                            self.log_file,
                        )
                        write_log(
                            self.log_file,
                            "sql_retry",
                            {
                                "original_sql": sql_query,
                                "fixed_sql": fixed_sql,
                                "error": None,
                            },
                        )
                        sql_query = fixed_sql
                        if rows:
                            sql_rows = rows
                            table = Table(show_header=True, header_style="bold")
                            for col in rows[0].keys():
                                table.add_column(col)
                            for row in rows:
                                table.add_row(
                                    *[
                                        str(row.get(col, ""))
                                        for col in rows[0].keys()
                                    ]
                                )
                            console.print(table)
                            card_ids = [
                                str(r.get("PurchaseCardId"))
                                for r in rows
                                if r.get("PurchaseCardId")
                            ]
                            card_ids = list(dict.fromkeys(card_ids))
                        else:
                            console.print("SQL не вернул данных.")
                        console.print(
                            f"Всего в ответе {total_count} строк, выше первые {len(rows)}."
                        )
                    except Exception as e2:
                        write_log(
                            self.log_file,
                            "sql_retry",
                            {
                                "original_sql": sql_query,
                                "fixed_sql": fixed_sql,
                                "error": str(e2),
                            },
                        )
                        console.print(
                            f"Повторный SQL также завершился ошибкой: {e2}"
                        )
                except Exception as fix_err:
                    write_log(
                        self.log_file,
                        "error",
                        {"stage": "sql_fix", "error": str(fix_err)},
                    )
                    console.print(f"Ошибка исправления SQL: {fix_err}")

            has_api = any(step.get("type") == "api" for step in steps_list)
            if has_api and card_ids:
                total = len(card_ids)
                print(
                    f"Нужно обработать {total} записей закупок. Ниже прогресс."
                )
                start_time = time.time()
                success_count = 0
                for i, card_id in enumerate(card_ids, start=1):
                    write_log(
                        self.log_file, "status_api_request", {"card_id": card_id}
                    )
                    try:
                        data = run_with_spinner(
                            f"[{i}/{total}] {card_id}", fetch_statuses, card_id
                        )
                        write_log(
                            self.log_file,
                            "status_api_response",
                            {"card_id": card_id, "response": data},
                        )
                        status_results.append({"card_id": card_id, "response": data})
                        success_count += 1
                    except Exception as e:
                        write_log(
                            self.log_file,
                            "error",
                            {
                                "stage": "status_api",
                                "card_id": card_id,
                                "error": str(e),
                            },
                        )
                        print(f"Ошибка запроса статусов: {e}")
                elapsed = time.time() - start_time
                print(
                    f"Заняло {elapsed:.2f} сек, {success_count} статусов получено успешно."
                )
            elif has_api:
                print("PurchaseCardId не найдены, API шаг пропущен.")

            has_synthesis = any(step.get("type") == "synthesis" for step in steps_list)
            if has_synthesis:
                try:
                    answer = run_with_spinner(
                        "Формирую финальный ответ",
                        call_ollama_synthesis,
                        self.question,
                        DB_SCHEMA,
                        sql_query,
                        sql_rows,
                        status_results,
                        "",
                        self.log_file,
                    )
                    console.print(answer)
                    write_log(self.log_file, "final_answer", {"text": answer})

                    review = run_with_spinner(
                        "Финальный обзор",
                        call_ollama_review,
                        self.question,
                        self.plan,
                        sql_query,
                        answer,
                        status_results,
                        "",
                        self.log_file,
                    )
                    console.print(review)
                    write_log(self.log_file, "final_review", {"text": review})
                except Exception as e:
                    write_log(
                        self.log_file,
                        "error",
                        {"stage": "synthesis", "error": str(e)},
                    )
                    console.print(f"Ошибка шага синтеза: {e}")
            else:
                console.print("Шаг synthesis не найден, финальный ответ не сформирован.")
            return
        except Exception as e:
            write_log(self.log_file, "error", {"stage": "plan", "error": str(e)})
            print(f"Ошибка построения плана: {e}")

    def run(self) -> None:
        self.run_checks()
        self.do_start()


if __name__ == "__main__":
    PipelineCLI().run()
