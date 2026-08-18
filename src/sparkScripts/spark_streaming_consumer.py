# sparkScripts/spark_streaming_consumer.py

import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, LongType
)
from metrics import (
    start_user_event_metrics, 
    start_recommendation_metrics, 
    start_notifications_metrics
)


EVENT_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("topic", StringType()),
    StructField("session_id", StringType()),
    StructField("visitor_id", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("order_id_for_refund", StringType()),
    StructField("is_recommended", BooleanType()),
    StructField("recommendation_view", StringType()),
])

EVENT_CONFIG = {
    # 1. user-events topic
    "view": "user-events",
    "add_to_cart": "user-events",
    "delete_from_cart": "user-events",
    "quantity": "user-events",
    "transaction": "user-events",
    "refund_request": "user-events",

    # 2. recommendation-actions topic
    "recommendation_shown": "recommendation-actions",
    "recommendation_clicked": "recommendation-actions",
    "recommendation_accepted": "recommendation-actions",
    "recommendation_rejected": "recommendation-actions",

    # 3. notification-events topic
    "discount_offer_sent": "notification-events",
    "reminder_sent": "notification-events",
    "offer_accepted": "notification-events",
    "offer_declined": "notification-events",
}

USER_EVENTS = [k for k, v in EVENT_CONFIG.items() if v == "user-events"]
RECOMMENDATION_EVENTS = [k for k, v in EVENT_CONFIG.items() if v == "recommendation-actions"]
NOTIFICATION_EVENTS = [k for k, v in EVENT_CONFIG.items() if v == "notification-events"]


EVENT_TYPE_TO_TOPIC = EVENT_CONFIG

# _EVENT_TOPIC_MAP_COL = F.create_map(
#     [F.lit(x) for kv in EVENT_TYPE_TO_TOPIC.items() for x in kv]
# )

# _event_topic_map_col = None

# def _get_event_topic_map_col():
#     global _event_topic_map_col
#     if _event_topic_map_col is None:
#         _event_topic_map_col = F.create_map(
#             [F.lit(x) for kv in EVENT_TYPE_TO_TOPIC.items() for x in kv]
#         )
#     return _event_topic_map_col



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
    parser.add_argument("--checkpoint-dir", type=str, default="/checkpoints")
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
    # for compact mode only
    parser.add_argument("--bucketed-table", type=str, default="customer_events_bucketed",
                         help="[compact mode] Metastore table name to write bucketed output to")
    parser.add_argument("--num-buckets", type=int, default=50,
                         help="[compact mode] Fixed number of buckets to hash customer_key into")

    # for dumping raw and metrics
    parser.add_argument("--dump-raw-topics", action="store_true",
                     help="Also write validated per-topic data to CSV for inspection")
    parser.add_argument("--dump-path", type=str, default="/DB/topic_dumps")
    
    parser.add_argument("--enable-console-metrics", action="store_true",
                        help="Start per-topic aggregate metrics (console or csv, see --metrics-sink)")
    parser.add_argument("--metrics-sink", type=str, default="console", choices=["console", "csv"])
    parser.add_argument("--metrics-path", type=str, default="/DB/metrics")

    return parser.parse_args()




def build_spark_session(app_name: str, master: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "6")
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
    # Parse Kafka JSON payloads, derive event_time, and compute a unified customer_key that works for both identified users and anonymous visitors.

    parsed = (
        raw_df
        .selectExpr(
            "CAST(value AS STRING) AS json_value", 
            "topic AS kafka_topic"
        ).select(
            F.from_json("json_value", EVENT_SCHEMA)
            .alias("data"), "kafka_topic"
        ).select("data.*", "kafka_topic")
    )

    keyed = (
        parsed
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn(
            "customer_key",
            F.when(
                (F.col("user_id").isNotNull()) & (F.col("user_id") != ""), 
                F.col("user_id")
            ).otherwise(F.col("visitor_id"))
        )
        .withColumn(
            "id_type",
            F.when(
                (F.col("user_id").isNotNull()) & (F.col("user_id") != ""), 
                F.lit("user")
            ).otherwise(F.lit("visitor"))
        )
        .filter(F.col("customer_key").isNotNull() & (F.col("customer_key") != ""))
    )
    return keyed


def enrich_with_products(keyed_df, spark, products_path: str):
    # Static broadcast join: attach category/price to each event row from products.csv. Broadcast join is cheap here since products.csv is small (100s of rows), this is not a stream-stream join

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
        .outputMode("append")
        .trigger(processingTime=trigger_interval)
        .start()
    )
    return query


def _validate_common(df):
    return (
        df
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("id").isNotNull())
        .filter(F.col("visitor_id").isNotNull())
        .filter(F.col("event_type").isNotNull())
        .dropDuplicates(["id"])
        .filter(F.col("event_type").isin(list(EVENT_TYPE_TO_TOPIC.keys())))
        # .filter(F.col("kafka_topic") == _get_event_topic_map_col()[F.col("event_type")])
    )


def validate_user_events(df):
    return (
        _validate_common(df)
        .filter(F.col("event_type").isin(USER_EVENTS))
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("product_id").cast("long") > 0)
        # refund requests must carry an order id
        .filter(
            ~(
                (F.col("event_type") == "refund_request")
                & F.col("order_id_for_refund").isNull()
            )
        )
    )


def validate_recommendation_events(df):
    return (
        _validate_common(df)
        .filter(F.col("event_type").isin(RECOMMENDATION_EVENTS))
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("product_id").cast("long") > 0)
    )


def validate_notification_events(df):
    return (
        _validate_common(df)
        .filter(F.col("event_type").isin(NOTIFICATION_EVENTS))
    )



def start_topic_dump_sink(df, path: str, checkpoint_dir: str, topic_name: str, trigger_interval: str):
    query = (
        df.writeStream
        .format("parquet")
        .option("path", f"{path}/{topic_name}")
        # .option("header", True)
        .option("checkpointLocation", f"{checkpoint_dir}/dump_{topic_name}")
        .outputMode("append")
        .trigger(processingTime=trigger_interval)
        .start()
    )
    return query


def run_stream_job(spark, args):
    raw_df = read_kafka_stream(spark, args.brokers, args.topics, args.starting_offsets)
    keyed_df = parse_and_key_events(raw_df)

    watermarked = keyed_df.withWatermark("event_time", args.watermark_delay)
    user_df = validate_user_events(watermarked.filter(F.col("kafka_topic") == "user-events"))
    rec_df = validate_recommendation_events(watermarked.filter(F.col("kafka_topic") == "recommendation-actions"))
    notif_df = validate_notification_events(watermarked.filter(F.col("kafka_topic") == "notification-events"))

    validated_df = user_df.unionByName(rec_df).unionByName(notif_df)

    if args.products_path:
        validated_df = enrich_with_products(validated_df, spark, args.products_path)
        print(f"[INFO] Enriching events with product category/price from: {args.products_path}")

    start_raw_events_sink(
        validated_df, args.raw_events_path, args.checkpoint_dir, args.trigger_interval
    )
    print(f"[INFO] raw_events (item-level, un-aggregated) streaming to: {args.raw_events_path}")

    if args.enable_activity_agg:
        activity_df = build_customer_activity(validated_df, args.window_duration, args.watermark_delay)
        start_customer_activity_sink(
            activity_df, args.activity_path, args.checkpoint_dir, args.trigger_interval
        )
        print(f"[INFO] customer_activity (windowed aggregates) streaming to: {args.activity_path}")

    if args.dump_raw_topics:
        start_topic_dump_sink(user_df, args.dump_path, args.checkpoint_dir, "user_events", args.trigger_interval)
        start_topic_dump_sink(rec_df, args.dump_path, args.checkpoint_dir, "recommendation_events", args.trigger_interval)
        start_topic_dump_sink(notif_df, args.dump_path, args.checkpoint_dir, "notification_events", args.trigger_interval)
        print(
            f"[INFO] Raw per-topic Parquet dumps writing to: "
            f"{args.dump_path}/{{user_events,recommendation_events,notification_events}}"
        )

    if args.enable_console_metrics:
        start_user_event_metrics(
            user_df, args.metrics_sink, args.metrics_path, args.checkpoint_dir, args.trigger_interval
        )
        start_recommendation_metrics(
            rec_df, args.metrics_sink, args.metrics_path, args.checkpoint_dir, args.trigger_interval
        )
        start_notifications_metrics(
            notif_df, args.metrics_sink, args.metrics_path, args.checkpoint_dir, args.trigger_interval
        )
        print(f"[INFO] Metrics running -> sink={args.metrics_sink}"
            + (f", path={args.metrics_path}" if args.metrics_sink == "csv" else ""))

    print("[INFO] Query(ies) running. Awaiting termination (Ctrl+C to stop)...")
    spark.streams.awaitAnyTermination()




def run_compact_job(spark, args):

    print(f"[INFO] Reading raw events from: {args.raw_events_path}")
    raw = spark.read.parquet(args.raw_events_path)

    row_count = raw.count()
    print(f"[INFO] {row_count:,} rows to bucket into '{args.bucketed_table}' "
          f"({args.num_buckets} buckets on customer_key)")

    (raw.write
        .bucketBy(args.num_buckets, "customer_key")
        .sortBy("customer_key")
        .mode("overwrite")
        .saveAsTable(args.bucketed_table)
    )

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





