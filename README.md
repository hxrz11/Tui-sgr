# Procurement Pipeline Console CLI

Консольный CLI для построения плана анализа закупок.

## Подготовка окружения

1. Установите зависимости:
   ```bash
   pip install requests psycopg2-binary python-dotenv
   ```
2. Создайте файл `.env` (можно скопировать из `.example.env`) и заполните переменные окружения:
   - `OLLAMA_URL` – URL сервера Ollama (по умолчанию `http://localhost:11434`)
   - `MODEL_NAME` – название модели LLM (по умолчанию `llama3.1:70b-instruct-q4_K_M`)
   - `POSTGRES_DSN` – строка подключения к Postgres
   - `LOG_DIR` – путь к каталогу для логов (по умолчанию `./logs`)

## Запуск

```bash
python procure_pipeline_tui/main_console.py
```

Скрипт проверит соединение с Postgres и Ollama, попросит ввести вопрос в консоли и выведет полученный от модели план в формате JSON.
