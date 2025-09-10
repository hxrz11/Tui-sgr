# agent.md

## Назначение

Этот репозиторий — консольный CLI-пайплайн для аналитики закупок. Пользователь задаёт вопрос → модель строит **План** (макс. 3 шага) → далее будут выполняться шаги (SQL, API, синтез). Сейчас реализован этап **проверок окружения** и **Планирование**; следующие шаги добавим итеративно.

Файл служит «бортовой картой» для Codex/ChatGPT: что делает проект, какие ограничения у данных и как с ним работать.

---

## Текущий статус (MVP Stage: План)

- Язык/стек: Python 3.10+, requests, psycopg2-binary, python-dotenv.
- Точка входа: `procure_pipeline_tui/main_console.py`.
- Что уже есть:
  - Проверка доступности Postgres (`SELECT 1`).
  - Проверка Ollama (`/api/tags`) и наличия модели.
  - Ввод вопроса → запрос к Ollama `/api/generate` → **получение Плана** в **строгом JSON**.
  - Вывод метаданных LLM (модель/опции/метрики, если возвращаются).
  - Логи на запуск: `./logs/<timestamp>_<session>.jsonl`.

---

## Запуск локально

### Зависимости

```bash
pip install requests psycopg2-binary python-dotenv
```

### Переменные окружения

```bash
export OLLAMA_URL="http://localhost:11434"
export MODEL_NAME="llama3.1:70b-instruct-q4_K_M"
export POSTGRES_DSN="postgresql://user:pass@localhost:5432/postgres"
export LOG_DIR="./logs"
```

### Старт

```bash
python procure_pipeline_tui/main_console.py
```

---

## Роль LLM и границы данных (критично)

Модель = **аналитик по закупкам**. Отвечает **только** на основе:

1) `SELECT` к `public."PurchaseAllView"`.
2) истории статусов из внешнего API (вход строго `"PurchaseCardId"`, выход — таймлайн статусов по ИКЗ).

**Без домыслов** вне данных. Цен **нет** — работаем с количествами, числом позиций/ИКЗ/заказов и датами.

### Жёсткие правила SQL

- Разрешены **только SELECT** к `public."PurchaseAllView"`. Никаких CTE/DML/других таблиц.
- Имена таблиц/полей — **в двойных кавычках**.
- **Без `*`** — только явные поля.
- По умолчанию фильтр: `WHERE "PurchaseRecordStatus" = 'A'`. Другие статусы — только по явному запросу.
- **Дедупликация «версий»** по `"GlobalUid"`: оставить ровно одну строку по приоритету:
  1) строка с **непустым** `"PurchaseCardId"`;
  2) затем наиболее поздняя `"ProcessingDate"`, потом `"CompletedDate"`, потом `"ApprovalDate"` (везде `DESC NULLS LAST`).
  Использовать `ROW_NUMBER() OVER (...)` и фильтровать `rn = 1`.
- Поиск по тексту — **только ILIKE с основой слова**: шаблон всегда `'%<stem>%'`.
  Для номенклатуры искать по **двум** полям:
  `("Nomenclature" ILIKE '%<stem>%' OR "NomenclatureFullName" ILIKE '%<stem>%')`.
- Формат дат для пользователя — **DD-MM-YYYY**. В SQL применять:
  `to_char(...,'DD-MM-YYYY') AS "<FieldFmt>"`.
- Если `LIMIT` не задан — использовать `LIMIT 100` и **явно** писать, что это «срезка».
- `"ArchiveStatus"` — учитывать **только** если пользователь явно запросил.

### Работа со статусами (API)

- Требуются, если в вопросе есть слова: «статус/текущий статус/история/этап/таймлайн».
- Алгоритм: **Сначала SQL** (с дедупликацией) → собрать **уникальные непустые** `"PurchaseCardId"` → вызывать статус-API.
- Ограничение интеграции: **1 `PurchaseCardId` за 1 HTTP-запрос** (итеративно).
- «Текущий статус» = **последний по времени** в таймлайне API (с датой).

---

## Формат «План» (выход LLM)

Максимум 3 шага:

- **Без статусов:** 2 шага → `sql` → `synthesis`.
- **Со статусами:** 3 шага → `sql` → `api` → `synthesis`.

Требования к шагу `sql`: вернуть **полный корректный SELECT** к `public."PurchaseAllView"` с дедупликацией (`ROW_NUMBER... rn=1`), фильтром `'A'`, ILIKE-условиями, `to_char` дат, `ORDER BY`, `LIMIT`.
Если далее будет `api`, во **внешнем** запросе добавить `AND "PurchaseCardId" IS NOT NULL`.

**Строгий JSON** (пример полей):

```json
{
  "plan_version": 1,
  "user_question": "…",
  "steps": [
    {
      "id": "1",
      "type": "sql",
      "title": "…",
      "description": "…",
      "requires": [],
      "outputs": ["sql", "preview_columns", "purchase_card_ids?", "metrics?"],
      "sql": "SELECT … FROM (SELECT … ROW_NUMBER() OVER (...) AS rn FROM public.\"PurchaseAllView\" WHERE \"PurchaseRecordStatus\"='A' /* фильтры */) t WHERE rn=1 /* если нужен API: AND \"PurchaseCardId\" IS NOT NULL */ ORDER BY \"OrderDate\" DESC NULLS LAST LIMIT 100;"
    },
    {
      "id": "2",
      "type": "api",
      "title": "…",
      "description": "Вызвать внешний API статусов по списку PurchaseCardId из шага 1 (по одному ID за запрос).",
      "requires": ["1"],
      "outputs": ["status_timeline", "current_status_summary"]
    },
    {
      "id": "3",
      "type": "synthesis",
      "title": "…",
      "description": "Собрать итоговый ответ: свод, табличный срез, текущие статусы (если были).",
      "requires": ["1", "2"],
      "outputs": ["final_answer"]
    }
  ]
}
```

---

## Промпты для Ollama

### `system` (жёсткий)

- Роль аналитика, правила SQL/данных, работа со статусами и формат дат — как в разделе «Роль LLM и границы данных».
- Запрет домыслов.
- Ответы — **строго JSON** по запрошенной схеме (без лишнего текста).

### `user` (План)

- Текст задачи пользователя + требования к структуре плана (см. формат выше).
- Если первый шаг `sql` — включить поле `sql` с полным запросом (см. требования).

---

## Проверки и метрики LLM

При `stream=false` Ollama иногда возвращает метрики. Мы отображаем:

- `model` (фактически применённая),
- `options` (переданные в запрос),
- `prompt_eval_count`, `eval_count`,
- `total_duration`, `prompt_eval_duration`, `eval_duration`.

Если метрик нет — показать `n/a`. Дополнительно логируем `prompt_chars` и `response_chars`.

---

## Логирование

Каждый запуск — файл `./logs/<timestamp>_<session>.jsonl`. Записываются события:

- `session` (id),
- `db_check` (ok/msg),
- `ollama_check` (ok/msg/model_info),
- `question` (текст),
- `plan` (JSON плана),
- `llm_meta` (если были метрики),
- `error` (stage, message) — при сбоях.

---

## Guard для SQL (в будущих шагах)

Перед выполнением любых SQL:

- Проверка: начинается с `SELECT`.
- Содержит **только** `public."PurchaseAllView"`.
- Нет DML/DDL/прочих опасных команд.

Ошибка → пользовательское сообщение и запись в лог, CLI не падает.

---

## Архитектура и файлы

- `procure_pipeline_tui/main_console.py` — консольный CLI: проверки окружения, ввод вопроса, план от LLM, вывод метаданных.
- `./logs/` — JSONL-журналы.

---

## Дорожная карта

1) **Шаг 1: генерация и исполнение SQL**
   - LLM генерирует SQL (по Плану), показываем SQL, кнопка «Выполнить», превью (до 10 строк), счётчики.
   - Логи: сырой SQL, превью, totals.

2) **Шаг 2: статус-API (по одному `PurchaseCardId`)**
   - Собираем ID из результата SQL.
   - Кнопка «Вызвать API для следующего ID», показываем таймлайн, текущий статус.
   - Логи: сырой ответ API на каждый ID.

3) **Шаг 3: синтез**
   - Свод (кол-во позиций/ИКЗ/заказов, суммы количеств, диапазоны дат), мини-таблица, текущие статусы.
   - Экспорт `…_report.md` и `…_report.json`.
