import pandas as pd

from pipeline_anomaly.infrastructure.detectors.median_absolute_deviation import (
    MedianAbsoluteDeviationDetector,
)


def test_mad_detector_flags_heavy_outlier():
    dataframe = pd.DataFrame({"value": [1.0, 1.1, 0.9, 50.0]})
    detector = MedianAbsoluteDeviationDetector(threshold=3.5)

    scores = detector.fit_predict(dataframe)

    assert scores.iloc[-1] == 1
    assert scores.sum() == 1
