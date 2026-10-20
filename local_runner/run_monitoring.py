"""Run the complete pandas fleet-monitoring pipeline."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.alerting import build_daily_alert_summary, send_alert  # noqa: E402
from src.monitoring import (  # noqa: E402
    daily_fleet_summary,
    detect_vehicle_silence,
    detect_volume_anomalies,
    route_coverage_summary,
)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    input_path = PROJECT_ROOT / "data" / "fleet_collection_logs.csv"
    reports_dir = PROJECT_ROOT / "reports"

    if not input_path.exists():
        raise FileNotFoundError(
            f"{input_path} does not exist. Run src/generate_fleet_data.py first."
        )

    logs = pd.read_csv(input_path, parse_dates=["date"])
    summaries = daily_fleet_summary(logs)
    silences = detect_vehicle_silence(logs)
    anomalies = detect_volume_anomalies(logs)
    coverage = route_coverage_summary(logs)

    reports_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "daily_fleet_summary.csv": summaries,
        "vehicle_silences.csv": silences,
        "volume_anomalies.csv": anomalies,
        "route_coverage.csv": coverage,
    }
    for filename, result in outputs.items():
        result.to_csv(reports_dir / filename, index=False)

    coverage_gaps = coverage[coverage["coverage_gap"]]
    print("Fleet monitoring run complete")
    print(f"  Input rows: {len(logs):,}")
    print(f"  Daily summaries: {len(summaries)}")
    print(f"  Silence periods: {len(silences)}")
    print(f"  Volume anomalies: {len(anomalies)}")
    print(f"  Route coverage gaps: {len(coverage_gaps)}")
    print(f"  Reports directory: {reports_dir}")

    run_date = logs["date"].max()
    message = build_daily_alert_summary(
        silences, anomalies, coverage, run_date=run_date
    )
    if message:
        send_alert(message)


if __name__ == "__main__":
    main()
