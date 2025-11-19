# Feature Store + ML Sandbox

Feature Store поверх ClickHouse + LightGBM-пайплайн для детекта аномалий.

## Архитектура

- `domain` — сущности фичей, датасетов и артефактов модели.
- `application` — orchestrators для materialize/train/serve.
- `infrastructure` — ClickHouse, feature registry, LightGBM-тренер.
- `presentation` — CLI на Typer.

## Запуск

```bash
make infra-up
make features
make train
make serve
```

Параметры лежат в `config/store.yaml`.

## Фичи

- time-bucket агрегаты (mean/std/count/quantiles) с `bucket_minutes` из `store.yaml`;
- производные фичи — IQR, тренд по среднему, волатильность среднего за 3 окна;
- материализация в ClickHouse в формате `Map(String, Float64)` для гибкой эволюции схемы.

## Модель и inference

- `make train` берёт материализованный feature view и обучает LightGBM, артефакты кладутся в `artifacts/`.
- `make serve` подтягивает свежие фичи, прогоняет инференс батчами и пишет скор в таблицу `predictions`.

## Тесты

`make test` — smoke по registry и тренеру.
