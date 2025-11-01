# Pipeline + Anomaly Detection Sandbox

Мини-ETL на ClickHouse с расчётом агрегатов и автодетектом аномалий.

## Архитектура

Hex-слои:

- `domain` — чистые объекты предметной области (батчи, отчёты, алерты).
- `application` — use-case'ы пайплайна.
- `infrastructure` — интеграция с ClickHouse, генератор синтетики, ML-алгоритмы.
- `presentation` — CLI-обёртка поверх use-case'ов.

## Быстрый старт

```bash
make infra-up
make pipeline
make infra-down
```

## Конфиг

`config/pipeline.yaml` контролит объём синтетики, батчи, алгоритмы и пороги алертов. Можно хранить разные профили и передавать через `--config`.

## Данные

Генератор создаёт 100 млн строк по умолчанию. Для локальной отладки поставьте `row_count: 100000` — пайплайн всё равно гоняет батчами.

## Метрики

- Z-score по агрегатам.
- IsolationForest и DBSCAN по фичам.
- Отчёт в виде JSON + Markdown (в ClickHouse хранится табличная витрина `anomaly_reports`).

## Технологии

- Python 3.11
- ClickHouse (docker-compose)
- scikit-learn, numpy, pandas
- loguru для логов

## Тесты

`make test` — лёгкая проверка доменных сервисов и генератора аномалий.
