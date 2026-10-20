"""Generate reproducible synthetic fleet collection logs."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from monitoring import detect_volume_anomalies


RANDOM_SEED = 42
ROUTES = [
    "I-280-North",
    "I-280-South",
    "US-101-North",
    "US-101-South",
    "I-80-East",
    "I-80-West",
    "CA-85-North",
    "CA-85-South",
]
UNDER_SAMPLED_ROUTE = "CA-85-South"
SILENT_VEHICLE = "vehicle_003"
DECLINING_VEHICLE = "vehicle_007"


def generate_fleet_data() -> tuple[pd.DataFrame, set[tuple[str, pd.Timestamp]]]:
    """Build 60 days of stable-baseline logs with deliberate anomalies."""
    rng = np.random.default_rng(RANDOM_SEED)
    dates = pd.date_range("2026-05-01", periods=60, freq="D")
    vehicles = [f"vehicle_{number:03d}" for number in range(1, 21)]
    baselines = {
        vehicle: rng.uniform(155.0, 245.0) for vehicle in vehicles
    }

    route_weights = np.array([0.14] * 7 + [0.02])
    route_weights /= route_weights.sum()
    silent_dates = set(dates[30:37])
    decline_dates = dates[-10:]

    random_gaps: set[tuple[str, pd.Timestamp]] = set()
    gap_eligible = [
        vehicle
        for vehicle in vehicles
        if vehicle not in {SILENT_VEHICLE, DECLINING_VEHICLE}
    ]
    while len(random_gaps) < 18:
        vehicle = str(rng.choice(gap_eligible))
        date = pd.Timestamp(rng.choice(dates[2:-2]))
        adjacent = {
            (vehicle, date - pd.Timedelta(days=1)),
            (vehicle, date + pd.Timedelta(days=1)),
        }
        if not (adjacent & random_gaps):
            random_gaps.add((vehicle, date))

    rows: list[dict[str, object]] = []
    for vehicle in vehicles:
        for date in dates:
            if vehicle == SILENT_VEHICLE and date in silent_dates:
                continue
            if (vehicle, date) in random_gaps:
                continue

            multiplier = 1.0
            if vehicle == DECLINING_VEHICLE and date in decline_dates:
                decline_day = decline_dates.get_loc(date)
                multiplier = float(np.linspace(0.95, 0.20, 10)[decline_day])

            baseline = baselines[vehicle]
            miles = max(5.0, baseline * multiplier + rng.normal(0, baseline * 0.08))
            route = str(rng.choice(ROUTES, p=route_weights))
            hours = max(0.2, miles / rng.normal(52.0, 2.5))
            disengagements = int(rng.poisson(max(miles / 180.0, 0.05)))
            rows.append(
                {
                    "date": date,
                    "vehicle_id": vehicle,
                    "route": route,
                    "miles_collected": round(miles, 2),
                    "hours_collected": round(hours, 2),
                    "disengagement_events": disengagements,
                }
            )

    return (
        pd.DataFrame(rows).sort_values(["date", "vehicle_id"]).reset_index(drop=True),
        random_gaps,
    )


def print_generation_summary(
    df: pd.DataFrame, random_gaps: set[tuple[str, pd.Timestamp]]
) -> None:
    """Print generation metadata and the requested detector calibration check."""
    anomalies = detect_volume_anomalies(
        df, rolling_window_days=7, drop_threshold_pct=0.4
    )
    normal_vehicles = set(df["vehicle_id"]) - {
        SILENT_VEHICLE,
        DECLINING_VEHICLE,
    }
    normal_rows = df[df["vehicle_id"].isin(normal_vehicles)]
    normal_anomalies = anomalies[anomalies["vehicle_id"].isin(normal_vehicles)]
    false_positive_rate = (
        len(normal_anomalies) / len(normal_rows) * 100 if len(normal_rows) else 0
    )

    print("Generated fleet collection logs")
    print(f"  Rows: {len(df):,}")
    print(f"  Date range: {df['date'].min().date()} through {df['date'].max().date()}")
    print(f"  Vehicles: {df['vehicle_id'].nunique()} | Routes: {df['route'].nunique()}")
    print(
        f"  Injected silence: {SILENT_VEHICLE}, "
        "2026-05-31 through 2026-06-06 (7 days)"
    )
    print(f"  Injected decline: {DECLINING_VEHICLE}, final 10 days")
    print(f"  Under-sampled route: {UNDER_SAMPLED_ROUTE}")
    print(f"  Random single-day gaps: {len(random_gaps)}")
    print(
        "  False-positive check (non-injected vehicles): "
        f"{len(normal_anomalies)} anomalies / {len(normal_rows)} rows "
        f"= {false_positive_rate:.2f}%"
    )


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=project_root / "data" / "fleet_collection_logs.csv",
    )
    args = parser.parse_args()

    data, random_gaps = generate_fleet_data()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    data.to_csv(args.output, index=False)
    print_generation_summary(data, random_gaps)
    print(f"  Wrote: {args.output}")


if __name__ == "__main__":
    main()
