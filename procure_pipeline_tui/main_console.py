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
    "Nomenclature": "text",
    "NomenclatureFullName": "text",
    "OrderDate": "date",
    "ProcessingDate": "timestamp",
    "CompletedDate": "timestamp",
    "ApprovalDate": "timestamp",
    "ArchiveStatus": "text",
}


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

COMMON_CONTEXT = (
    "Ты — аналитик по закупкам. Отвечаешь ТОЛЬКО на основе: \n"
    '1) SELECT к public."PurchaseAllView"\n'
    '2) Истории статусов из внешнего API (вход: "PurchaseCardId", выход: таймлайн статусов по ИКЗ).\n'
    "Никаких домыслов вне данных.\n\n"
    "Жёсткие правила:\n"
    '- Разрешены только SELECT из public."PurchaseAllView". Никаких CTE/DML/других таблиц.\n'
    '- Всегда используешь двойные кавычки для имён полей/таблицы.\n'
    '- Не используешь SELECT * — только явные поля.\n'
    '- По умолчанию добавляешь WHERE "PurchaseRecordStatus"=\'A\'.\n'
    '- Дедупликация по "GlobalUid": приоритет строки с непустым "PurchaseCardId"; затем по дате: "ProcessingDate" DESC, "CompletedDate" DESC, "ApprovalDate" DESC (NULLS LAST). Используй ROW_NUMBER() OVER (...) и фильтруй rn=1.\n'
    '- Поиск ILIKE только по «основе слова», шаблон всегда \'%<stem>%\'. Для номенклатуры: ("Nomenclature" ILIKE \'%<stem>%\' OR "NomenclatureFullName" ILIKE \'%<stem>%\').\n'
    '- Даты для пользователя — формат DD-MM-YYYY (to_char(...,\'DD-MM-YYYY\') AS "<FieldFmt>").\n'
    '- Цен нет — работаешь с количествами/счётчиками/датами.\n'
    '- Если LIMIT не задан — ставь LIMIT 100 и явно указывай, что это срезка.\n'
    '- "ArchiveStatus" учитывай только если явно попросят.\n'
    '- Статусы нужны, если в вопросе есть «статус/текущий статус/история/этап/таймлайн».\n'
    '- Каждый SQL-запрос должен содержать WHERE, ROW_NUMBER и LIMIT.\n'
    '- План максимум из 3 шагов. Если требуются статусы — sql → api → synthesis, иначе sql → synthesis. Если есть шаг api, во внешнем SELECT добавь AND "PurchaseCardId" IS NOT NULL.\n\n'
    "Формы ответов строго в JSON по запрошенной схеме. Без дополнительного текста.\n"
)
SYSTEM_PROMPT = COMMON_CONTEXT

SYNTHESIS_SYSTEM_PROMPT = COMMON_CONTEXT + ("Давай ответ одним уровнем объяснения без вложенных списков. Текущий статус для каждого PurchaseCardId — последняя запись в statuses_json. Если statuses_json пуст, явно сообщи, что статусы недоступны.")

PLAN_USER_PROMPT_TEMPLATE = (
    "ТВОЯ ЗАДАЧА: построить план решения запроса пользователя.\n"
    "ВХОД:\n- user_question: <<<{{QUESTION}}>>>\n\n"
    "ТРЕБОВАНИЯ К ПЛАНУ:\n"
    "- steps: массив до 3 объектов. Каждый объект имеет поля id, type (sql|api|synthesis), title, description, requires, outputs.\n"
    "- Первый шаг обязателен и имеет type='sql'. Для шага 'sql':\n"
    "  - поля: id, type, title, description, requires, outputs, sql.\n"
    "  - outputs: [\"sql\", \"preview_columns\", \"purchase_card_ids?\", \"metrics?\"].\n"
    "  - sql: полный SELECT к public.\"PurchaseAllView\" с WHERE \"PurchaseRecordStatus\"='A',\n"
    "    дедупликацией через ROW_NUMBER() OVER (...) rn=1 (приоритет PurchaseCardId и дат),\n"
    "    явными полями, ILIKE '%stem%', to_char(...,'DD-MM-YYYY'), ORDER BY и LIMIT.\n"
    "    Если далее будет шаг api — во внешнем запросе добавь AND \"PurchaseCardId\" IS NOT NULL.\n"
    "- Без статусов: 2 шага (sql → synthesis). Со статусами: 3 шага (sql → api → synthesis).\n\n"
    "ВЫХОД: строго JSON.\n"
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

def call_ollama_plan(question: str, log_file: str) -> Tuple[dict, dict, List[Dict[str, str]]]:
    """Возвращает (plan_json, meta, steps_preview) и логирует запрос."""
    user_prompt = PLAN_USER_PROMPT_TEMPLATE.replace("{{QUESTION}}", question)
    url = OLLAMA_URL.rstrip('/') + "/api/generate"
    body = {
        "model": MODEL_NAME,
        "prompt": user_prompt,
        "system": SYSTEM_PROMPT,
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


def call_ollama_fix_sql(
    question: str, schema: str, bad_sql: str, error: str, log_file: str
) -> str:
    """Ask LLM to fix SQL based on schema and error message."""
    parts = [
        "Схема таблицы:",
        schema,
        f"Вопрос: {question}",
        "Неудачный SQL:",
        bad_sql,
        f"Ошибка БД: {error}",
        "Исправь запрос, соблюдая правила:",
        '- только SELECT из public."PurchaseAllView";',
        '- обязательный фильтр WHERE "PurchaseRecordStatus"=\'A\';',
        '- дедупликация версий по "GlobalUid" через ROW_NUMBER() OVER (...) rn=1 (приоритет "PurchaseCardId" и даты "ProcessingDate"/"CompletedDate"/"ApprovalDate" DESC NULLS LAST);',
        "- даты форматируй to_char(...,'DD-MM-YYYY');",
        '- если LIMIT не указан — добавь LIMIT 100;',
        "Верни только исправленный SQL.",
    ]
    prompt = "\n".join(parts)
    url = OLLAMA_URL.rstrip('/') + "/api/generate"
    body = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "system": SYSTEM_PROMPT,
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
    user_prompt = (
        f"question:\n{question}\n\n"
        f"schema_json:\n```json\n{json.dumps(schema, ensure_ascii=False)}\n```\n\n"
        f"sql_query:\n```sql\n{sql_query}\n```\n\n"
        f"sql_rows_json:\n```json\n{json.dumps(sql_rows, ensure_ascii=False)}\n```\n\n"
        f"statuses_json:\n```json\n{json.dumps(statuses, ensure_ascii=False)}\n```\n\n"
        f"limit_note:\n{limit_note}\n"
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
    schema: dict | None,
    sql_query: str | None,
    answer: str,
    api_responses: List[Dict[str, Any]],
    log_file: str,
) -> str:
    """Отправляет модели все артефакты и возвращает финальный обзор.

    Логирует запрос и ответ для трассировки.
    """
    parts = [
        f"Вопрос: {question}",
    ]
    if schema is not None:
        parts.append("План:")
        parts.append(json.dumps(schema, ensure_ascii=False))
    if sql_query:
        parts.append("SQL:")
        parts.append(sql_query)
    if api_responses:
        parts.append("API ответы:")
        parts.append(json.dumps(api_responses, ensure_ascii=False))
    parts.append("Финальный ответ:")
    parts.append(answer)
    parts.append(
        "Сформулируй 1-2 предложения с фактологическим саммари. "
        "Явно укажи ограничение LIMIT из SQL, если оно применялось."
    )
    prompt = "\n".join(parts)

    url = OLLAMA_URL.rstrip('/') + "/api/generate"
    body = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "system": (
            "Ты — аналитик по закупкам. Дай фактологическое саммари "
            "на русском в 1-2 предложения. Укажи ограничение LIMIT, "
            "если оно есть."
        ),
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

        try:
            plan, meta, _previews = run_with_spinner(
                "Запрашиваю у LLM план действий", call_ollama_plan, q, self.log_file
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
            limit_note: str = ""
            sql_query: Optional[str] = None
            steps_list = plan.get("steps", [])
            if steps_list:
                first_step = steps_list[0]
            else:
                first_step = {}

            if first_step.get("type") == "sql":
                sql_query = strip_sql_block(first_step.get("sql"))
                if sql_query:
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
                        limit_note = (
                            f"Показаны первые {len(rows)} строк из {total_count}"
                            if total_count > len(rows)
                            else f"Показаны все {total_count} строк"
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
                                    table = Table(
                                        show_header=True, header_style="bold"
                                    )
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
                                limit_note = (
                                    f"Показаны первые {len(rows)} строк из {total_count}"
                                    if total_count > len(rows)
                                    else f"Показаны все {total_count} строк"
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
                            console.print(
                                f"Ошибка исправления SQL: {fix_err}"
                            )

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
                          sql_query or "",
                          sql_rows,
                          status_results,
                          limit_note,
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
