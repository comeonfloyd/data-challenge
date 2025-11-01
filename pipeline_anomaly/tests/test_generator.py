from pipeline_anomaly.infrastructure.generators.synthetic_generator import (
    SyntheticDatasetConfig,
    SyntheticDatasetGenerator,
)


def test_batches_shape():
    config = SyntheticDatasetConfig(row_count=1000, batch_size=200, anomaly_ratio=0.1, seed=42)
    generator = SyntheticDatasetGenerator(config)
    batches = generator.batches()
    assert sum(batch.size for batch in batches) == 1000
