#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Procurement Pipeline — Console TUI (Stage 0 → Plan)

Проверка доступности Postgres и Ollama (и наличия модели)

Приглашение ввести вопрос

LLM-запрос на построение ПЛАНА (макс. 3 шага)

Показ плана в JSON и метрик LLM (модель/опции/токены если даны/тайминги)

Пауза: кнопки «Далее» и «Прервать» (пока шагов дальше не делаем)

Лог на каждый запуск: ./logs/<timestamp>_<session>.jsonl


Зависимости: pip install textual rich requests psycopg2-binary python-dotenv
Переменные окружения:
  OLLAMA_URL   (по умолчанию http://localhost:11434)
  MODEL_NAME   (по умолчанию llama3.1:70b-instruct-q4_K_M)
  POSTGRES_DSN (по умолчанию postgresql://user:pass@localhost:5432/postgres)
  LOG_DIR      (по умолчанию ./logs)
Запуск: python procure_pipeline_tui/main_tui.py

Примечание: это «суровый» MVP под «План». Следующий этап — Шаг1 (сбор SQL-запроса) поверх этого же TUI.
"""
from __future__ import annotations

import os
import json
import uuid
import datetime as dt
from typing import Any, Dict, List, Optional

import requests
import psycopg2
from psycopg2.extras import RealDictCursor

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical, Container
from textual.widgets import (
    Header, Footer, Button, Input, Static, Log, DataTable, Label,
    Tabs, TabbedContent, TabPane,
)
from textual.reactive import reactive

from dotenv import load_dotenv

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

def db_check(dsn: str) -> tuple[bool, str]:
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

def ollama_check(base_url: str, model: str) -> tuple[bool, str, Optional[dict]]:
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

def call_ollama_plan(question: str) -> tuple[dict, dict]:
    """Возвращает (plan_json, meta) где meta содержит модель/опции/метрики если есть."""
    user_prompt = PLAN_USER_PROMPT_TEMPLATE.replace("{{QUESTION}}", question)
    url = OLLAMA_URL.rstrip('/') + "/api/generate"
    body = {
        "model": MODEL_NAME,
        "prompt": user_prompt,
        "system": SYSTEM_PROMPT,
        "stream": False,
        "options": {"temperature": 0.1, "num_ctx": 8192},
    }
    resp = requests.post(url, json=body, timeout=180)
    resp.raise_for_status()
    data = resp.json()
    raw = (data.get("response") or "").strip()

    # попытка вынуть голый JSON из возможных ```json оградителей
    json_text = raw
    if json_text.startswith("```"):
        json_text = json_text.strip("`\n ")
        json_text = json_text.replace("json\n", "").replace("javascript\n", "")
    try:
        plan = json.loads(json_text)
    except json.JSONDecodeError:
        # запасной вариант: заменить одиночные кавычки
        if '"' not in json_text and "'" in json_text:
            plan = json.loads(json_text.replace("'", '"'))
        else:
            raise

    meta = {
        "model": data.get("model", MODEL_NAME),
        "options": body.get("options"),
        # эти поля появляются в некоторых сборках ollama при stream=false
        "eval_count": data.get("eval_count"),
        "prompt_eval_count": data.get("prompt_eval_count"),
        "total_duration": data.get("total_duration"),
        "eval_duration": data.get("eval_duration"),
        "prompt_eval_duration": data.get("prompt_eval_duration"),
        # бэкап-метрики
        "prompt_chars": len(body.get("prompt", "")),
        "response_chars": len(raw),
    }
    return plan, meta

# ------------------------------
# TUI App
# ------------------------------

class PipelineTUI(App):
    CSS = """
    Screen { layout: vertical; }
    #top { height: 3; }
    #main { height: 1fr; }
    #left, #center, #right { border: round $surface; padding: 1 1; }
    #left { width: 32; }
    #center { width: 1fr; }
    #right { width: 48; }
    #controls { height: 3; }
    .title { color: $text; }
    """

    session_id: reactive[str | None] = reactive(None)
    log_file: reactive[str | None] = reactive(None)

    db_ok: reactive[bool] = reactive(False)
    db_msg: reactive[str] = reactive("")
    llm_ok: reactive[bool] = reactive(False)
    llm_msg: reactive[str] = reactive("")
    llm_model_info: reactive[Optional[dict]] = reactive(None)

    question: reactive[str] = reactive("")
    plan: reactive[Optional[dict]] = reactive(None)
    llm_meta: reactive[Optional[dict]] = reactive(None)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="top"):
            yield Label("Вопрос:")
            yield Input(placeholder="Введите точный вопрос…", id="question")
            yield Button("Старт", id="start", variant="primary")
        with Horizontal(id="main"):
            with Vertical(id="left"):
                yield Static("[b]Проверки[/b]\n", id="chk_title")
                yield Log(id="checks", highlight=True)
            with Vertical(id="center"):
                yield Static("[b]План[/b]", id="plan_title")
                yield Log(id="plan_log", highlight=True)
            with Vertical(id="right"):
                yield Static("[b]LLM мета[/b]", id="meta_title")
                yield Log(id="meta_log", highlight=True)
        with Horizontal(id="controls"):
            yield Button("▶ Далее", id="next", disabled=True)
            yield Button("⛔ Прервать", id="abort", disabled=True)
            yield Button("Очистить", id="clear")
        yield Footer()

    # helpers to write into logs
    def log_checks(self, text: str):
        self.query_one("#checks", Log).write(text)

    def log_plan(self, text: str):
        self.query_one("#plan_log", Log).write(text)

    def log_meta(self, text: str):
        self.query_one("#meta_log", Log).write(text)

    def on_mount(self):
        # init session
        self.session_id = new_session_id()
        self.log_file = log_path(self.session_id)
        write_log(self.log_file, "session", {"session_id": self.session_id})
        # run checks
        self.run_checks()
        # focus input
        self.query_one("#question", Input).focus()

    def run_checks(self):
        self.log_checks("[b]DB:[/b] проверка соединения …")
        ok, msg = db_check(POSTGRES_DSN)
        self.db_ok, self.db_msg = ok, msg
        write_log(self.log_file, "db_check", {"ok": ok, "msg": msg})
        self.log_checks(("[green]OK[/green]" if ok else "[red]FAIL[/red]") + f": {msg}")

        self.log_checks(f"\n[b]Ollama:[/b] проверка …")
        ok2, msg2, info = ollama_check(OLLAMA_URL, MODEL_NAME)
        self.llm_ok, self.llm_msg, self.llm_model_info = ok2, msg2, info
        write_log(self.log_file, "ollama_check", {"ok": ok2, "msg": msg2, "model_info": info})
        self.log_checks(("[green]OK[/green]" if ok2 else "[red]FAIL[/red]") + f": {msg2}")
        if info:
            self.log_checks(f"Модель: {info.get('name')} | Сайз: {info.get('size', 'n/a')}")

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        b = event.button
        if b.id == "start":
            await self.do_start()
        elif b.id == "next":
            self.log_plan("[yellow]Пока следующий шаг не реализован. Переходим к доработке Шага 1 (SQL).[/yellow]")
        elif b.id == "abort":
            self.reset()
        elif b.id == "clear":
            self.reset()

    def reset(self):
        # new session, clear logs
        self.session_id = new_session_id()
        self.log_file = log_path(self.session_id)
        write_log(self.log_file, "session", {"session_id": self.session_id})
        self.query_one("#checks", Log).clear()
        self.query_one("#plan_log", Log).clear()
        self.query_one("#meta_log", Log).clear()
        self.plan = None
        self.llm_meta = None
        self.query_one("#next", Button).disabled = True
        self.query_one("#abort", Button).disabled = True
        self.run_checks()
        self.query_one("#question", Input).value = ""
        self.query_one("#question", Input).focus()

    async def do_start(self):
        q = self.query_one("#question", Input).value.strip()
        if not q:
            self.bell()
            return
        if not self.db_ok or not self.llm_ok:
            self.log_checks("[red]Проверки не пройдены. Исправьте окружение и попробуйте снова.[/red]")
            return
        self.question = q
        write_log(self.log_file, "question", {"text": q})
        self.log_plan(f"[b]Вопрос:[/b] {q}")
        self.log_plan("Запрашиваю у LLM план действий…")
        try:
            plan, meta = call_ollama_plan(q)
            self.plan = plan
            self.llm_meta = meta
            write_log(self.log_file, "plan", plan)
            write_log(self.log_file, "llm_meta", meta)
            # pretty print
            self.log_plan("[b]План (JSON):[/b]")
            self.log_plan(json.dumps(plan, ensure_ascii=False, indent=2))
            # meta panel
            self.log_meta("[b]Модель:[/b] " + str(meta.get("model")))
            self.log_meta("[b]Опции:[/b] " + json.dumps(meta.get("options"), ensure_ascii=False))
            # токены/тайминги если есть
            ec = meta.get("eval_count")
            pec = meta.get("prompt_eval_count")
            td = meta.get("total_duration")
            ed = meta.get("eval_duration")
            ped = meta.get("prompt_eval_duration")
            self.log_meta(f"[b]prompt_eval_count:[/b] {pec if pec is not None else 'n/a'}")
            self.log_meta(f"[b]eval_count:[/b] {ec if ec is not None else 'n/a'}")
            self.log_meta(f"[b]total_duration:[/b] {td if td is not None else 'n/a'}")
            self.log_meta(f"[b]prompt_eval_duration:[/b] {ped if ped is not None else 'n/a'}")
            self.log_meta(f"[b]eval_duration:[/b] {ed if ed is not None else 'n/a'}")
            # запасные метрики
            self.log_meta(f"[b]prompt_chars:[/b] {meta.get('prompt_chars')}")
            self.log_meta(f"[b]response_chars:[/b] {meta.get('response_chars')}")

            # enable controls
            self.query_one("#next", Button).disabled = False
            self.query_one("#abort", Button).disabled = False
        except Exception as e:
            write_log(self.log_file, "error", {"stage": "plan", "error": str(e)})
            self.log_plan(f"[red]Ошибка построения плана: {e}[/red]")

if __name__ == "__main__":
    PipelineTUI().run()
