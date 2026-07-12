"""
spark_customer_stream.py
--------------------------------------------------------------------------
Consumes events from Kafka (topics: user-events, recommendation-actions,
notification-events) produced by the synthetic e-commerce event generator,
and writes append-only output keyed at the customer level (user_id if
identified, else visitor_id).

Two modes:

  --mode stream (default)
      Runs the streaming job. Primary output:
        1. raw_events   -> every event, UN-aggregated, at full item/event
                           grain (event_type, product_id, order_id_for_refund,
                           is_recommended, recommendation_view, timestamp,
                           etc.), tagged with a unified `customer_key` and
                           `id_type` (user/visitor). This is the feature-store
                           source-of-truth log -- feature computation (rolling
                           counts, last-N-products, recency, etc.) should be
                           derived from this raw log downstream (batch or a
                           separate streaming job), not baked in here, so you
                           keep point-in-time correctness and can recompute
                           features however you need later.

                           Partitioned by id_type/event_date (NOT by
                           customer_key -- too high cardinality, causes the
                           small-files problem). Each micro-batch is sorted by
                           customer_key before writing so rows for the same
                           customer cluster together within files, which
                           speeds up later filtering via Parquet's per-file
                           min/max column stats.

      Optional output (off by default, enable with --enable-activity-agg):
        2. customer_activity -> windowed per-customer aggregate counts.
                           Useful for dashboards/monitoring, but NOT a
                           substitute for the raw log when building a feature
                           store -- aggregating collapses the item-level
                           detail (which product was viewed/added/bought)
                           that most useful features are actually built from.

  --mode compact
      One-shot BATCH job (run periodically, e.g. nightly via cron/Airflow --
      NOT run continuously). Re-reads everything under raw_events and
      rewrites it as a bucketed table (bucketBy is batch-only, not supported
      on DataStreamWriter), so downstream point-lookups and joins on
      customer_key can use bucket pruning / avoid a shuffle. Overwrites the
      table each run to avoid re-bucketing small-file buildup from repeated
      appends.

Usage:
    # streaming ingestion
    spark-submit spark_customer_stream.py --mode stream \
        --brokers localhost:9092 \
        --topics user-events,recommendation-actions,notification-events \
        --raw-events-path /data/raw_events \
        --activity-path /data/customer_activity \
        --checkpoint-dir /chk \
        --window-duration "10 minutes" \
        --watermark-delay "10 minutes" \
        --starting-offsets latest

    # periodic batch compaction into a bucketed table
    spark-submit spark_customer_stream.py --mode compact \
        --raw-events-path /data/raw_events \
        --bucketed-table customer_events_bucketed \
        --num-buckets 50
--------------------------------------------------------------------------
"""

import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, LongType
)

# --------------------------------------------------------------------------
# Schema matching the events.csv / Kafka payload structure
# --------------------------------------------------------------------------
EVENT_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("topic", StringType()),
    StructField("visitor_id", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("order_id_for_refund", StringType()),
    StructField("is_recommended", BooleanType()),
    StructField("recommendation_view", StringType()),
])


def parse_args():
    parser = argparse.ArgumentParser(description="Customer-level Spark Structured Streaming consumer")
    parser.add_argument("--mode", type=str, default="stream", choices=["stream", "compact"],
                         help="'stream' runs the continuous Kafka consumer; "
                              "'compact' is a one-shot batch job that rewrites raw_events "
                              "into a bucketed table for fast customer-level lookups/joins")
    parser.add_argument("--brokers", type=str, default="localhost:9092",
                         help="Kafka bootstrap servers, comma-separated")
    parser.add_argument("--topics", type=str,
                         default="user-events,recommendation-actions,notification-events",
                         help="Comma-separated list of Kafka topics to subscribe to")
    parser.add_argument("--starting-offsets", type=str, default="latest",
                         choices=["latest", "earliest"])
    parser.add_argument("--raw-events-path", type=str, default="/data/raw_events")
    parser.add_argument("--products-path", type=str, default=None,
                         help="Optional path to products.csv. If given, each raw event row "
                              "is enriched with the product's category/price via a broadcast "
                              "join -- useful item-level context for feature engineering "
                              "beyond just product_id.")
    parser.add_argument("--checkpoint-dir", type=str, default="/chk")
    parser.add_argument("--enable-activity-agg", action="store_true",
                         help="Also write a windowed per-customer aggregate stream "
                              "(dashboards/monitoring). Off by default -- for a feature "
                              "store, build features from raw_events instead, not this.")
    parser.add_argument("--activity-path", type=str, default="/data/customer_activity")
    parser.add_argument("--window-duration", type=str, default="10 minutes",
                         help="[only if --enable-activity-agg] window size for aggregation")
    parser.add_argument("--watermark-delay", type=str, default="10 minutes",
                         help="How late data is allowed to arrive before a window is closed")
    parser.add_argument("--trigger-interval", type=str, default="30 seconds",
                         help="Micro-batch trigger interval")
    parser.add_argument("--app-name", type=str, default="CustomerLevelStreamConsumer")
    parser.add_argument("--master", type=str, default="local[*]",
                         help="Spark master URL, e.g. 'local[*]', 'yarn', 'spark://host:7077', "
                              "'k8s://https://host:port'. Ignored if spark-submit's own "
                              "--master flag is also set (that one wins).")
    # --- compact mode only ---
    parser.add_argument("--bucketed-table", type=str, default="customer_events_bucketed",
                         help="[compact mode] Metastore table name to write bucketed output to")
    parser.add_argument("--num-buckets", type=int, default=50,
                         help="[compact mode] Fixed number of buckets to hash customer_key into")
    return parser.parse_args()


def build_spark_session(app_name: str, master: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "8")  # tune for your cluster/local size
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, brokers: str, topics: str, starting_offsets: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topics)
        .option("startingOffsets", starting_offsets)
        # Keep behavior predictable if offsets fall out of retention
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_key_events(raw_df):
    """Parse Kafka JSON payloads, derive event_time, and compute a unified
    customer_key that works for both identified users and anonymous visitors.
    Deliberately does NOT aggregate or drop any columns -- every field from
    the source event (event_type, product_id, order_id_for_refund,
    is_recommended, recommendation_view, ...) is preserved as-is, since this
    is the raw log a feature store will be built from."""
    parsed = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_value", "topic AS kafka_topic")
        .select(F.from_json("json_value", EVENT_SCHEMA).alias("data"), "kafka_topic")
        .select("data.*", "kafka_topic")
    )

    keyed = (
        parsed
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn(
            "customer_key",
            F.when((F.col("user_id").isNotNull()) & (F.col("user_id") != ""), F.col("user_id"))
             .otherwise(F.col("visitor_id"))
        )
        .withColumn(
            "id_type",
            F.when((F.col("user_id").isNotNull()) & (F.col("user_id") != ""), F.lit("user"))
             .otherwise(F.lit("visitor"))
        )
        # Drop any row we genuinely can't attribute to anyone (shouldn't happen, but be safe)
        .filter(F.col("customer_key").isNotNull() & (F.col("customer_key") != ""))
    )
    return keyed


def enrich_with_products(keyed_df, spark, products_path: str):
    """Static broadcast join: attach category/price to each event row from
    products.csv. Broadcast join is safe/cheap here since products.csv is
    small (hundreds of rows) and static -- this is a lookup enrichment, not a
    stream-stream join, so no watermarking is needed."""
    products = (
        spark.read.csv(products_path, header=True, inferSchema=True)
        .select(
            F.col("id").alias("product_id"),
            F.col("category").alias("product_category"),
            F.col("price").alias("product_price"),
        )
        .withColumn("product_id", F.col("product_id").cast("string"))
    )
    return keyed_df.join(F.broadcast(products), on="product_id", how="left")


def build_customer_activity(keyed_df, window_duration: str, watermark_delay: str):
    """Append-mode windowed aggregation: per customer, per time window."""
    return (
        keyed_df
        .withWatermark("event_time", watermark_delay)
        .groupBy(
            F.window("event_time", window_duration).alias("window"),
            "customer_key",
            "id_type",
        )
        .agg(
            F.count("*").alias("event_count"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_carts"),
            F.sum(F.when(F.col("event_type") == "delete_from_cart", 1).otherwise(0)).alias("cart_removals"),
            F.sum(F.when(F.col("event_type") == "transaction", 1).otherwise(0)).alias("transactions"),
            F.sum(F.when(F.col("event_type") == "refund_request", 1).otherwise(0)).alias("refund_requests"),
            F.sum(F.when(F.col("event_type") == "recommendation_clicked", 1).otherwise(0)).alias("recs_clicked"),
            F.sum(F.when(F.col("is_recommended") == True, 1).otherwise(0)).alias("recommended_events"),
            F.countDistinct("product_id").alias("distinct_products_touched"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )


def start_raw_events_sink(keyed_df, path: str, checkpoint_dir: str, trigger_interval: str):
    """Writes every event, partitioned by low-cardinality id_type/event_date
    (never by customer_key -- see module docstring). Uses foreachBatch so we
    can sort each micro-batch by customer_key before writing: this doesn't
    reduce file count, but it clusters same-customer rows together within
    each file, which makes Parquet's per-file min/max column stats much more
    effective for later `filter(customer_key == ...)` queries."""

    def write_batch(batch_df, batch_id):
        (batch_df
         .sortWithinPartitions("customer_key")
         .write
         .partitionBy("id_type", "event_date")
         .mode("append")
         .parquet(path))

    query = (
        keyed_df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{checkpoint_dir}/raw_events")
        .outputMode("append")
        .trigger(processingTime=trigger_interval)
        .start()
    )
    return query


def start_customer_activity_sink(activity_df, path: str, checkpoint_dir: str, trigger_interval: str):
    query = (
        activity_df.writeStream
        .format("parquet")
        .option("path", path)
        .option("checkpointLocation", f"{checkpoint_dir}/customer_activity")
        .outputMode("append")   # safe because window+watermark guarantees a row is final when emitted
        .trigger(processingTime=trigger_interval)
        .start()
    )
    return query


def run_stream_job(spark, args):
    raw_df = read_kafka_stream(spark, args.brokers, args.topics, args.starting_offsets)
    keyed_df = parse_and_key_events(raw_df)

    if args.products_path:
        keyed_df = enrich_with_products(keyed_df, spark, args.products_path)
        print(f"[INFO] Enriching events with product category/price from: {args.products_path}")

    start_raw_events_sink(
        keyed_df, args.raw_events_path, args.checkpoint_dir, args.trigger_interval
    )
    print(f"[INFO] raw_events (item-level, un-aggregated) streaming to: {args.raw_events_path}")

    if args.enable_activity_agg:
        activity_df = build_customer_activity(keyed_df, args.window_duration, args.watermark_delay)
        start_customer_activity_sink(
            activity_df, args.activity_path, args.checkpoint_dir, args.trigger_interval
        )
        print(f"[INFO] customer_activity (windowed aggregates) streaming to: {args.activity_path}")

    print("[INFO] Query(ies) running. Awaiting termination (Ctrl+C to stop)...")

    # Wait on all active queries; if any fails, this will raise and stop the process
    spark.streams.awaitAnyTermination()


def run_compact_job(spark, args):
    """Batch job: rewrite raw_events as a bucketed metastore table, keyed on
    customer_key. bucketBy is batch-only (not supported on DataStreamWriter),
    which is why this runs separately from the streaming job -- schedule it
    periodically (cron/Airflow), don't run it continuously.

    Uses mode('overwrite') and re-reads the full raw_events dataset each run,
    rather than mode('append'), because repeatedly appending to a bucketed
    table creates a fresh set of per-bucket files on every run -- you'd end
    up with num_buckets x num_runs files instead of num_buckets, recreating
    the small-files problem at the bucket level."""

    print(f"[INFO] Reading raw events from: {args.raw_events_path}")
    raw = spark.read.parquet(args.raw_events_path)

    row_count = raw.count()
    print(f"[INFO] {row_count:,} rows to bucket into '{args.bucketed_table}' "
          f"({args.num_buckets} buckets on customer_key)")

    (raw.write
        .bucketBy(args.num_buckets, "customer_key")
        .sortBy("customer_key")
        .mode("overwrite")
        .saveAsTable(args.bucketed_table))

    print(f"[INFO] Done. Verify with: DESCRIBE FORMATTED {args.bucketed_table}")


def main():
    args = parse_args()
    spark = build_spark_session(args.app_name, args.master)
    spark.sparkContext.setLogLevel("WARN")

    if args.mode == "stream":
        run_stream_job(spark, args)
    else:
        run_compact_job(spark, args)


if __name__ == "__main__":
    main()