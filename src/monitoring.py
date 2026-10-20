"""Pure pandas checks for fleet data-collection health."""

from __future__ import annotations

import pandas as pd


DAILY_SUMMARY_COLUMNS = [
    "date",
    "total_miles",
    "total_hours",
    "active_vehicles",
]
SILENCE_COLUMNS = [
    "vehicle_id",
    "silence_start",
    "silence_end",
    "missing_days",
]
ANOMALY_COLUMNS = [
    "date",
    "vehicle_id",
    "miles_collected",
    "trailing_avg_miles",
    "drop_pct",
]
COVERAGE_COLUMNS = [
    "route",
    "total_miles",
    "share_pct",
    "coverage_gap",
]


def daily_fleet_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate collection volume and active vehicle counts by calendar day."""
    if df.empty:
        return pd.DataFrame(columns=DAILY_SUMMARY_COLUMNS)

    work = df.copy()
    work["date"] = pd.to_datetime(work["date"]).dt.normalize()
    result = (
        work.groupby("date", as_index=False)
        .agg(
            total_miles=("miles_collected", "sum"),
            total_hours=("hours_collected", "sum"),
            active_vehicles=("vehicle_id", "nunique"),
        )
        .sort_values("date")
        .reset_index(drop=True)
    )
    return result[DAILY_SUMMARY_COLUMNS]


def detect_vehicle_silence(
    df: pd.DataFrame, threshold_days: int = 3
) -> pd.DataFrame:
    """Return every run of threshold_days or more missing calendar days."""
    if threshold_days < 1:
        raise ValueError("threshold_days must be at least 1")
    if df.empty:
        return pd.DataFrame(columns=SILENCE_COLUMNS)

    work = df[["date", "vehicle_id"]].copy()
    work["date"] = pd.to_datetime(work["date"]).dt.normalize()
    start_date, end_date = work["date"].min(), work["date"].max()
    calendar = pd.date_range(start_date, end_date, freq="D")
    periods: list[dict[str, object]] = []

    for vehicle_id, vehicle_rows in work.groupby("vehicle_id", sort=True):
        reported = pd.DatetimeIndex(vehicle_rows["date"].unique())
        missing = pd.Series(~calendar.isin(reported), index=calendar)
        run_ids = missing.ne(missing.shift(fill_value=False)).cumsum()

        for _, run in missing[missing].groupby(run_ids[missing]):
            if len(run) >= threshold_days:
                periods.append(
                    {
                        "vehicle_id": vehicle_id,
                        "silence_start": run.index[0],
                        "silence_end": run.index[-1],
                        "missing_days": len(run),
                    }
                )

    return pd.DataFrame(periods, columns=SILENCE_COLUMNS)


def detect_volume_anomalies(
    df: pd.DataFrame,
    rolling_window_days: int = 7,
    drop_threshold_pct: float = 0.4,
) -> pd.DataFrame:
    """Flag daily mileage drops versus each vehicle's prior calendar-day history."""
    if rolling_window_days < 1:
        raise ValueError("rolling_window_days must be at least 1")
    if not 0 < drop_threshold_pct < 1:
        raise ValueError("drop_threshold_pct must be between 0 and 1")
    if df.empty:
        return pd.DataFrame(columns=ANOMALY_COLUMNS)

    work = df[["date", "vehicle_id", "miles_collected"]].copy()
    work["date"] = pd.to_datetime(work["date"]).dt.normalize()
    work = (
        work.groupby(["vehicle_id", "date"], as_index=False)["miles_collected"]
        .sum()
        .sort_values(["vehicle_id", "date"])
    )

    pieces: list[pd.DataFrame] = []
    for _, vehicle_rows in work.groupby("vehicle_id", sort=False):
        vehicle_rows = vehicle_rows.set_index("date").sort_index()
        vehicle_rows["trailing_avg_miles"] = vehicle_rows[
            "miles_collected"
        ].rolling(f"{rolling_window_days}D", closed="left").mean()
        pieces.append(vehicle_rows.reset_index())

    scored = pd.concat(pieces, ignore_index=True)
    scored["drop_pct"] = (
        scored["trailing_avg_miles"] - scored["miles_collected"]
    ) / scored["trailing_avg_miles"]
    anomalies = scored[
        scored["trailing_avg_miles"].notna()
        & (scored["drop_pct"] >= drop_threshold_pct)
    ].copy()

    return (
        anomalies[ANOMALY_COLUMNS]
        .sort_values(["date", "vehicle_id"])
        .reset_index(drop=True)
    )


def route_coverage_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize route mileage and flag materially under-covered routes."""
    if df.empty:
        return pd.DataFrame(columns=COVERAGE_COLUMNS)

    coverage = (
        df.groupby("route", as_index=False)
        .agg(total_miles=("miles_collected", "sum"))
        .sort_values("route")
        .reset_index(drop=True)
    )
    fleet_miles = coverage["total_miles"].sum()
    coverage["share_pct"] = (
        coverage["total_miles"] / fleet_miles * 100 if fleet_miles else 0.0
    )
    gap_threshold_pct = 0.5 * (100 / len(coverage))
    coverage["coverage_gap"] = coverage["share_pct"] < gap_threshold_pct
    return coverage[COVERAGE_COLUMNS]
