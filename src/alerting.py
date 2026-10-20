"""Alert formatting and Slack delivery for fleet monitoring."""

from __future__ import annotations

import logging
import os
from datetime import date, datetime

import pandas as pd
import requests


LOGGER = logging.getLogger(__name__)


def send_alert(message: str) -> bool:
    """Send an alert to Slack, or emit a prominent local log fallback."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        LOGGER.warning("FLEET MONITORING ALERT (Slack webhook not set)\n%s", message)
        return False

    try:
        response = requests.post(webhook_url, json={"text": message}, timeout=10)
        response.raise_for_status()
    except requests.RequestException:
        LOGGER.exception("Slack delivery failed. Alert contents:\n%s", message)
        return False
    return True


def build_daily_alert_summary(
    silences_df: pd.DataFrame,
    anomalies_df: pd.DataFrame,
    coverage_df: pd.DataFrame,
    run_date: str | date | datetime | pd.Timestamp,
) -> str | None:
    """Build a single signal-heavy alert, returning None when all checks pass."""
    coverage_gaps = (
        coverage_df[coverage_df["coverage_gap"].fillna(False)]
        if "coverage_gap" in coverage_df
        else coverage_df.iloc[0:0]
    )
    if silences_df.empty and anomalies_df.empty and coverage_gaps.empty:
        return None

    formatted_date = pd.Timestamp(run_date).date().isoformat()
    lines = [f":warning: Fleet monitoring findings for {formatted_date}"]

    if not silences_df.empty:
        lines.append(f"• Silence periods: {len(silences_df)}")
        for row in silences_df.itertuples(index=False):
            lines.append(
                f"  - {row.vehicle_id}: {pd.Timestamp(row.silence_start).date()} "
                f"to {pd.Timestamp(row.silence_end).date()} ({row.missing_days} days)"
            )

    if not anomalies_df.empty:
        vehicles = anomalies_df["vehicle_id"].nunique()
        lines.append(
            f"• Volume anomalies: {len(anomalies_df)} day(s) across "
            f"{vehicles} vehicle(s)"
        )

    if not coverage_gaps.empty:
        route_details = ", ".join(
            f"{row.route} ({row.share_pct:.2f}%)"
            for row in coverage_gaps.itertuples(index=False)
        )
        lines.append(f"• Route coverage gaps: {route_details}")

    return "\n".join(lines)
