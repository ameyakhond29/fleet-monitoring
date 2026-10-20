"""Unit tests for pure pandas monitoring checks."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.monitoring import (
    daily_fleet_summary,
    detect_vehicle_silence,
    detect_volume_anomalies,
    route_coverage_summary,
)


def test_daily_fleet_summary_aggregates_volume_and_unique_vehicles() -> None:
    logs = pd.DataFrame(
        [
            ["2026-01-01", "v1", "r1", 100.0, 2.0],
            ["2026-01-01", "v2", "r2", 80.0, 1.5],
            ["2026-01-02", "v1", "r1", 120.0, 2.5],
        ],
        columns=[
            "date",
            "vehicle_id",
            "route",
            "miles_collected",
            "hours_collected",
        ],
    )

    result = daily_fleet_summary(logs)

    assert result["total_miles"].tolist() == [180.0, 120.0]
    assert result["total_hours"].tolist() == [3.5, 2.5]
    assert result["active_vehicles"].tolist() == [2, 1]


def test_silence_detection_honors_threshold_boundary() -> None:
    dates = pd.date_range("2026-01-01", periods=9, freq="D")
    rows = []
    for date in dates:
        if date not in dates[2:5]:  # Exactly three missing days: flag.
            rows.append({"date": date, "vehicle_id": "v_threshold"})
        if date not in dates[4:6]:  # Only two missing days: do not flag.
            rows.append({"date": date, "vehicle_id": "v_below"})

    result = detect_vehicle_silence(pd.DataFrame(rows), threshold_days=3)

    assert len(result) == 1
    period = result.iloc[0]
    assert period["vehicle_id"] == "v_threshold"
    assert period["silence_start"] == pd.Timestamp("2026-01-03")
    assert period["silence_end"] == pd.Timestamp("2026-01-05")
    assert period["missing_days"] == 3


def test_volume_anomaly_detects_clear_vehicle_specific_drop() -> None:
    dates = pd.date_range("2026-02-01", periods=9, freq="D")
    logs = pd.DataFrame(
        {
            "date": list(dates) * 2,
            "vehicle_id": ["short_route"] * 9 + ["long_route"] * 9,
            "miles_collected": [100.0] * 8
            + [25.0]
            + [300.0] * 8
            + [300.0],
        }
    )

    result = detect_volume_anomalies(
        logs, rolling_window_days=7, drop_threshold_pct=0.4
    )

    assert len(result) == 1
    anomaly = result.iloc[0]
    assert anomaly["vehicle_id"] == "short_route"
    assert anomaly["miles_collected"] == 25.0
    assert anomaly["trailing_avg_miles"] == pytest.approx(100.0)
    assert anomaly["drop_pct"] == pytest.approx(0.75)


def test_stable_low_noise_data_has_no_false_positive_across_large_sample() -> None:
    rng = np.random.default_rng(2026)
    dates = pd.date_range("2026-01-01", periods=120, freq="D")
    rows = []
    for vehicle_number in range(30):
        baseline = 100.0 + vehicle_number * 7.5
        for date in dates:
            rows.append(
                {
                    "date": date,
                    "vehicle_id": f"vehicle_{vehicle_number:03d}",
                    "miles_collected": baseline
                    * (1 + rng.normal(0.0, 0.08)),
                }
            )
    logs = pd.DataFrame(rows)

    result = detect_volume_anomalies(
        logs, rolling_window_days=7, drop_threshold_pct=0.4
    )

    assert len(logs) == 3_600
    assert result.empty, (
        f"Stable per-vehicle baselines produced {len(result)} false positives"
    )


def test_route_coverage_flags_only_material_under_sampling() -> None:
    logs = pd.DataFrame(
        {
            "route": ["r1", "r2", "r3", "r4"],
            "miles_collected": [33.0, 33.0, 32.0, 2.0],
        }
    )

    result = route_coverage_summary(logs)

    assert result.loc[result["coverage_gap"], "route"].tolist() == ["r4"]
