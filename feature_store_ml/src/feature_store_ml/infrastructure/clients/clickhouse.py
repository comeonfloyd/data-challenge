from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import clickhouse_connect

from feature_store_ml.infrastructure.config import ClickHouseConfig


class ClickHouseFactory:
    def __init__(self, config: ClickHouseConfig) -> None:
        self._config = config

    @contextmanager
    def connect(self) -> Iterator[clickhouse_connect.driver.Client]:  # type: ignore[name-defined]
        client = clickhouse_connect.get_client(
            host=self._config.host,
            port=self._config.port,
            username=self._config.username,
            password=self._config.password,
            database=self._config.database,
        )
        try:
            yield client
        finally:
            client.close()
