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

import requests
import psycopg2
from dotenv import load_dotenv
from dashboard import Dashboard



# ------------------------------
# Config
# ------------------------------

load_dotenv()

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
MODEL_NAME = os.getenv("MODEL_NAME", "llama3.1:70b-instruct-q4_K_M")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/postgres")
LOG_DIR = os.getenv("LOG_DIR", "logs")

os.makedirs(LOG_DIR, exist_ok=True)

# ------------------------------
# Prompts
# ------------------------------

SYSTEM_PROMPT = (
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
    '- Статусы нужны, если в вопросе есть «статус/текущий статус/история/этап/таймлайн».\n\n'
    "Формы ответов строго в JSON по запрошенной схеме. Без дополнительного текста.\n"
)

PLAN_USER_PROMPT_TEMPLATE = (
    "ТВОЯ ЗАДАЧА: построить план решения запроса пользователя.\n"
    "ВХОД:\n- user_question: <<<{{QUESTION}}>>>\n\n"
    "ТРЕБОВАНИЯ К ПЛАНУ:\n"
    "- steps: массив объектов с полями: id, type (sql|api|synthesis), title, description, requires, outputs.\n"
    '- Если первый шаг type=\'sql\' — включи в объект поле \'sql\' со ПОЛНЫМ SELECT к public."PurchaseAllView"\n'
    '  с дедупликацией rn=1, фильтром "PurchaseRecordStatus"=\'A\', явными полями, ILIKE \'%stem%\', to_char дат, ORDER BY, LIMIT.\n'
    '  Если далее будет api — во внешнем запросе добавь AND "PurchaseCardId" IS NOT NULL.\n'
    "- Если статусы не нужны — 2 шага: sql → synthesis. Если нужны — 3 шага: sql → api → synthesis.\n\n"
    "ВЫХОД (строго JSON).\n"
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


def db_check(dsn: str) -> Tuple[bool, str]:
    try:
        conn = psycopg2.connect(dsn)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
                ok = (row is not None and row[0] == 1)
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
    resp = requests.post(url, json=body, timeout=180)
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
        self.dashboard = Dashboard()

    # environment checks
    def run_checks(self) -> None:
        self.dashboard.set_stage("Environment")
        self.dashboard.update_status("Environment", "DB: проверка соединения …")
        ok, msg = db_check(POSTGRES_DSN)
        icon = "✅" if ok else "❌"
        self.db_ok, self.db_msg = ok, msg
        write_log(self.log_file, "db_check", {"ok": ok, "msg": msg})
        self.dashboard.update_status("Environment", f"DB: {icon} {msg}")

        self.dashboard.update_status("Environment", "Ollama: проверка …")
        ok2, msg2, info = ollama_check(OLLAMA_URL, MODEL_NAME)
        icon = "✅" if ok2 else "❌"
        self.llm_ok, self.llm_msg, self.llm_model_info = ok2, msg2, info
        write_log(self.log_file, "ollama_check", {"ok": ok2, "msg": msg2, "model_info": info})
        self.dashboard.update_status("Environment", f"Ollama: {icon} {msg2}")
        if info:
            self.dashboard.update_status(
                "Environment",
                f"Модель: {info.get('name')} | Сайз: {info.get('size', 'n/a')}",
            )

    # main action
    def do_start(self) -> None:
        if not self.db_ok or not self.llm_ok:
            self.dashboard.update_status(
                "Environment", "Проверки не пройдены. Исправьте окружение и попробуйте снова."
            )
            return
        self.dashboard.set_stage("Schema/Plan Generation")
        self.dashboard.update_status(
            "Schema/Plan Generation", f"Модель: {MODEL_NAME}"
        )
        self.dashboard.update_status(
            "Schema/Plan Generation", "Опции: {'temperature': 0.1, 'num_ctx': 8192}"
        )
        q = input("Введите точный вопрос: ").strip()
        if not q:
            self.dashboard.update_status(
                "Schema/Plan Generation", "Вопрос пустой, ничего не делаем."
            )
            return
        self.question = q
        write_log(self.log_file, "question", {"text": q})
        self.dashboard.update_status("Schema/Plan Generation", f"Вопрос: {q}")
        self.dashboard.update_status(
            "Schema/Plan Generation", "Запрашиваю у LLM план действий…"
        )
        try:
            plan, meta, previews = call_ollama_plan(q, self.log_file)
            self.plan = plan
            self.llm_meta = meta
            write_log(self.log_file, "plan", plan)
            write_log(self.log_file, "llm_meta", meta)
            self.dashboard.update_status("Schema/Plan Generation", "План (JSON):")
            self.dashboard.update_status(
                "Schema/Plan Generation", json.dumps(plan, ensure_ascii=False, indent=2)
            )
            self.dashboard.update_meta(meta)
            self.dashboard.set_plan_preview(previews)
            self.dashboard.update_status(
                "Schema/Plan Generation",
                "Пока следующий шаг не реализован. Переходим к доработке Шага 1 (SQL).",
            )
        except Exception as e:
            write_log(self.log_file, "error", {"stage": "plan", "error": str(e)})
            self.dashboard.update_status(
                "Schema/Plan Generation", f"Ошибка построения плана: {e}"
            )

    def run(self) -> None:
        self.run_checks()
        self.do_start()
        self.dashboard.close()


if __name__ == "__main__":
    PipelineCLI().run()
