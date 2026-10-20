# Databricks notebook source
# MAGIC %md
# MAGIC # Fleet data collection monitoring
# MAGIC
# MAGIC PySpark/Delta implementation of the same checks as `src/monitoring.py`.
# MAGIC The input table is expected to contain `date`, `vehicle_id`, `route`,
# MAGIC `miles_collected`, `hours_collected`, and `disengagement_events`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

RAW_TABLE = "fleet_monitoring.raw_collection_logs"
DAILY_TABLE = "fleet_monitoring.daily_fleet_summary"
SILENCE_TABLE = "fleet_monitoring.vehicle_silences"
ANOMALY_TABLE = "fleet_monitoring.volume_anomalies"
COVERAGE_TABLE = "fleet_monitoring.route_coverage"

SILENCE_THRESHOLD_DAYS = 3
ROLLING_WINDOW_DAYS = 7
DROP_THRESHOLD_PCT = 0.4

raw_df = (
    spark.table(RAW_TABLE)
    .withColumn("date", F.to_date("date"))
    .withColumn("miles_collected", F.col("miles_collected").cast("double"))
    .withColumn("hours_collected", F.col("hours_collected").cast("double"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Daily fleet summary

# COMMAND ----------

daily_summary_df = (
    raw_df.groupBy("date")
    .agg(
        F.sum("miles_collected").alias("total_miles"),
        F.sum("hours_collected").alias("total_hours"),
        F.countDistinct("vehicle_id").alias("active_vehicles"),
    )
    .orderBy("date")
)

(
    daily_summary_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DAILY_TABLE)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Consecutive vehicle silence periods

# COMMAND ----------

date_bounds_df = raw_df.agg(
    F.min("date").alias("start_date"), F.max("date").alias("end_date")
)
calendar_df = date_bounds_df.select(
    F.explode(F.sequence("start_date", "end_date")).alias("date")
)
vehicle_calendar_df = raw_df.select("vehicle_id").distinct().crossJoin(calendar_df)
reported_days_df = raw_df.select("vehicle_id", "date").distinct()

missing_days_df = vehicle_calendar_df.join(
    reported_days_df, ["vehicle_id", "date"], "left_anti"
)
missing_order = Window.partitionBy("vehicle_id").orderBy("date")
missing_with_groups_df = (
    missing_days_df.withColumn(
        "missing_row_number", F.row_number().over(missing_order)
    )
    .withColumn(
        "silence_group",
        F.expr("date_sub(date, missing_row_number)"),
    )
)
silence_df = (
    missing_with_groups_df.groupBy("vehicle_id", "silence_group")
    .agg(
        F.min("date").alias("silence_start"),
        F.max("date").alias("silence_end"),
        F.count("*").alias("missing_days"),
    )
    .filter(F.col("missing_days") >= SILENCE_THRESHOLD_DAYS)
    .drop("silence_group")
    .orderBy("vehicle_id", "silence_start")
)

(
    silence_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILENCE_TABLE)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Per-vehicle volume anomalies

# COMMAND ----------

vehicle_daily_df = raw_df.groupBy("vehicle_id", "date").agg(
    F.sum("miles_collected").alias("miles_collected")
)

# pandas uses `.rolling("7D", closed="left")` on each vehicle's date index.
# In Spark, ordering by Unix seconds and using rangeBetween maps that calendar
# window exactly: prior seven days through the prior day, excluding today.
seconds_per_day = 86_400
trailing_window = (
    Window.partitionBy("vehicle_id")
    .orderBy(F.col("date").cast("timestamp").cast("long"))
    .rangeBetween(-ROLLING_WINDOW_DAYS * seconds_per_day, -seconds_per_day)
)

anomaly_df = (
    vehicle_daily_df.withColumn(
        "trailing_avg_miles", F.avg("miles_collected").over(trailing_window)
    )
    .withColumn(
        "drop_pct",
        (F.col("trailing_avg_miles") - F.col("miles_collected"))
        / F.col("trailing_avg_miles"),
    )
    .filter(
        F.col("trailing_avg_miles").isNotNull()
        & (F.col("drop_pct") >= DROP_THRESHOLD_PCT)
    )
    .select(
        "date",
        "vehicle_id",
        "miles_collected",
        "trailing_avg_miles",
        "drop_pct",
    )
    .orderBy("date", "vehicle_id")
)

(
    anomaly_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(ANOMALY_TABLE)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Route coverage

# COMMAND ----------

route_totals_df = raw_df.groupBy("route").agg(
    F.sum("miles_collected").alias("total_miles")
)
fleet_window = Window.partitionBy()
coverage_df = (
    route_totals_df.withColumn(
        "share_pct",
        F.col("total_miles") / F.sum("total_miles").over(fleet_window) * 100,
    )
    .withColumn("route_count", F.count("*").over(fleet_window))
    .withColumn(
        "coverage_gap",
        F.col("share_pct") < (F.lit(0.5) * (F.lit(100.0) / F.col("route_count"))),
    )
    .drop("route_count")
    .orderBy("route")
)

(
    coverage_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(COVERAGE_TABLE)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Job result

# COMMAND ----------

silence_count = silence_df.count()
anomaly_count = anomaly_df.count()
coverage_gap_count = coverage_df.filter("coverage_gap").count()
needs_attention = any([silence_count, anomaly_count, coverage_gap_count])

dbutils.notebook.exit(
    {
        "status": "ATTENTION_REQUIRED" if needs_attention else "HEALTHY",
        "silence_periods": silence_count,
        "volume_anomalies": anomaly_count,
        "route_coverage_gaps": coverage_gap_count,
    }
)
