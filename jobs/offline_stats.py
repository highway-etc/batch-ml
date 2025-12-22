"""
Offline aggregation job for ETC traffic.

Computes per-station windowed stats (total/unique counts, by direction, by vehicle type)
from the raw `traffic_pass_dev` table and writes results into `stats_realtime` via JDBC.

Usage (local Spark example):
    spark-submit --master local[*] jobs/offline_stats.py \
      --jdbc-url jdbc:mysql://localhost:8066/highway_etc?useUnicode=true&characterEncoding=utf8 \
      --user etcuser --password etcpass --window 5 minutes --mode overwrite

Notes:
- Requires MySQL/MyCat JDBC driver on Spark's classpath (e.g., --jars mysql-connector-j.jar).
- Window defaults to tumbling 5 minutes. Adjust with --window "10 minutes" if needed.
- Output table schema expected by services StatsRepository: station_id, window_start, window_end,
  cnt (total), unique_cnt, by_dir (JSON), by_type (JSON).
"""

import argparse
from pyspark.sql import SparkSession, functions as F


def build_args():
    ap = argparse.ArgumentParser(description="ETC offline stats aggregator")
    ap.add_argument("--jdbc-url", default="jdbc:mysql://localhost:8066/highway_etc?useUnicode=true&characterEncoding=utf8",
                    help="JDBC URL to MyCat/MySQL logical database highway_etc")
    ap.add_argument("--user", default="etcuser", help="DB user")
    ap.add_argument("--password", default="etcpass", help="DB password")
    ap.add_argument("--window", default="5 minutes", help="Tumbling window duration, e.g. '5 minutes'")
    ap.add_argument("--output-table", default="stats_realtime", help="Target table name")
    ap.add_argument("--mode", default="overwrite", choices=["overwrite", "append"], help="Save mode")
    ap.add_argument("--fetchsize", type=int, default=20000, help="JDBC fetch size for source read")
    ap.add_argument("--partitions", type=int, default=8, help="Number of shuffle partitions")
    return ap.parse_args()


def main():
    args = build_args()

    spark = (
        SparkSession.builder
        .appName("ETCOfflineStats")
        .config("spark.sql.shuffle.partitions", args.partitions)
        .getOrCreate()
    )

    # Read raw traffic data
    traffic = (
        spark.read.format("jdbc")
        .option("url", args.jdbc_url)
        .option("dbtable", "traffic_pass_dev")
        .option("user", args.user)
        .option("password", args.password)
        .option("fetchsize", args.fetchsize)
        .load()
        .select("station_id", "gcsj", "fxlx", "hpzl", "hphm_mask")
        .dropna(subset=["gcsj", "station_id"])
    )

    # Normalize direction/hpzl strings a bit to reduce duplication
    traffic = traffic.withColumn("fxlx_norm", F.upper(F.col("fxlx")))
    traffic = traffic.withColumn("hpzl_norm", F.lpad(F.col("hpzl"), 2, "0"))

    window_col = F.window("gcsj", args.window)

    totals = traffic.groupBy("station_id", window_col).agg(
        F.count("*").alias("cnt"),
        F.countDistinct("hphm_mask").alias("unique_cnt")
    )

    dir_counts = traffic.groupBy("station_id", window_col, "fxlx_norm").agg(
        F.count("*").alias("dir_cnt")
    )
    dir_maps = dir_counts.groupBy("station_id", "window").agg(
        F.map_from_entries(F.collect_list(F.struct(F.col("fxlx_norm"), F.col("dir_cnt"))))
        .alias("by_dir")
    )

    type_counts = traffic.groupBy("station_id", window_col, "hpzl_norm").agg(
        F.count("*").alias("type_cnt")
    )
    type_maps = type_counts.groupBy("station_id", "window").agg(
        F.map_from_entries(F.collect_list(F.struct(F.col("hpzl_norm"), F.col("type_cnt"))))
        .alias("by_type")
    )

    joined = (
        totals
        .join(dir_maps, ["station_id", "window"], "left")
        .join(type_maps, ["station_id", "window"], "left")
        .select(
            F.col("station_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("cnt").alias("cnt"),
            F.col("unique_cnt"),
            F.to_json(F.col("by_dir")).alias("by_dir"),
            F.to_json(F.col("by_type")).alias("by_type"),
        )
    )

    (
        joined.write.format("jdbc")
        .option("url", args.jdbc_url)
        .option("dbtable", args.output_table)
        .option("user", args.user)
        .option("password", args.password)
        .mode(args.mode)
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    main()
