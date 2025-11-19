# Data Quality Monitoring Sandbox

FastAPI-сервис для автоматической проверки качества данных: completeness, uniqueness, диапазоны, schema drift, freshness и контроль объёма.

## Архитектура

- `domain` — правила валидации и результаты проверок.
- `application` — orchestrator для запуска проверок по конфигу.
- `infrastructure` — ClickHouse client/репозиторий и движок правил.
- `presentation` — REST API и CLI-триггеры.

## Запуск

```bash
make infra-up
make api
make run-checks
```

## Конфиг

`config/rules.yaml` описывает таблицы и правила:

```yaml
rules:
  - table: default.events
    expectations:
      - type: completeness
        column: value
        threshold: 0.99
      - type: freshness
        column: ts
        max_minutes: 60
      - type: volume
        min_rows: 1000
```

Сервис хранит результаты в ClickHouse таблице `dq_reports` и возвращает их через `/reports` + экспортирует Prometheus-метрику количества запусков.

## Мониторинг

- логирование через loguru
- прометеевские метрики (экспорт `/metrics`)

## Тесты

`make test` — unit по движку правил.
