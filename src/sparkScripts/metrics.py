from pyspark.sql import functions as F

def _csv_foreach_batch_writer(path: str):
    def write_batch(batch_df, batch_id):
        (batch_df
         .coalesce(1)
         .write
         .mode("overwrite")
         .option("header", True)
         .csv(path))
    return write_batch


def _start_metric_query(agg_df, query_name: str, output_mode: str,
                         metrics_path: str, checkpoint_dir: str, trigger_interval: str,
                         sink: str = "console"):
    """sink: 'console' or 'csv'. Handles the complete/update -> file-sink incompatibility."""
    stream = agg_df.writeStream.queryName(query_name).outputMode(output_mode)

    if sink == "console":
        stream = stream.format("console").option("truncate", False)
    else:  # csv
        safe_name = query_name.replace(" ", "_")
        stream = stream.foreachBatch(
            _csv_foreach_batch_writer(f"{metrics_path}/{safe_name}")
        ).option("checkpointLocation", f"{checkpoint_dir}/metrics_{safe_name}")

    return stream.trigger(processingTime=trigger_interval).start()


# streaming queries for metrics
def start_user_event_metrics(df, sink="console", metrics_path=None, checkpoint_dir=None, trigger_interval="30 seconds"):
    queries = []

    def flat_window(agg_df):
        return (agg_df
                .withColumn("window_start", F.col("window.start"))
                .withColumn("window_end", F.col("window.end"))
                .drop("window"))

    queries.append(_start_metric_query(
        df.groupBy("event_type").count().withColumnRenamed("count", "event_count"),
        "event_counts", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        df.groupBy().count().withColumnRenamed("count", "total_events_received"),
        "total_events_received", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        flat_window(df.groupBy(F.window("event_time", "1 minute")).count()
                    .withColumnRenamed("count", "events_per_minute")),
        "events_per_minute", "update", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        flat_window(df.filter(F.col("event_type") == "view")
                    .groupBy(F.window("event_time", "1 minute")).count()
                    .withColumnRenamed("count", "views_per_minute")),
        "views_per_minute", "update", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        flat_window(df.filter(F.col("event_type") == "add_to_cart")
                    .groupBy(F.window("event_time", "1 minute")).count()
                    .withColumnRenamed("count", "add_to_cart_per_minute")),
        "add_to_cart_per_minute", "update", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        df.filter(F.col("event_type") == "view").groupBy("product_id").count()
        .withColumnRenamed("count", "top_viewed_products")
        .orderBy("top_viewed_products", ascending=False),
        "top_viewed_products", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        df.filter(F.col("event_type") == "add_to_cart").groupBy("product_id").count()
        .withColumnRenamed("count", "top_products_added_to_cart")
        .orderBy("top_products_added_to_cart", ascending=False),
        "top_products_added_to_cart", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        flat_window(df.filter(F.col("event_type") == "transaction")
                    .groupBy(F.window("event_time", "1 minute")).count()
                    .withColumnRenamed("count", "purchases_per_minute")),
        "purchases_per_minute", "update", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        flat_window(df.filter(F.col("event_type") == "refund_request")
                    .groupBy(F.window("event_time", "1 minute")).count()
                    .withColumnRenamed("count", "refunds_per_minute")),
        "refunds_per_minute", "update", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        df.filter(F.col("event_type") == "transaction").groupBy("product_id").count()
        .withColumnRenamed("count", "top_purchased_products")
        .orderBy("top_purchased_products", ascending=False),
        "top_purchased_products", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        df.filter(F.col("event_type") == "refund_request").groupBy("product_id").count()
        .withColumnRenamed("count", "top_refunded_products")
        .orderBy("top_refunded_products", ascending=False),
        "top_refunded_products", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    return queries


def start_recommendation_metrics(df, sink="console", metrics_path=None, checkpoint_dir=None, trigger_interval="30 seconds"):
    queries = []

    def flat_window(agg_df):
        return (agg_df.withColumn("window_start", F.col("window.start"))
                       .withColumn("window_end", F.col("window.end")).drop("window"))

    queries.append(_start_metric_query(
        df.groupBy("event_type").count().withColumnRenamed("count", "recommendation_event_count"),
        "recommendation_events_by_type", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        df.groupBy().count().withColumnRenamed("count", "total_recommendations_received"),
        "total_recommendations_received", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        flat_window(df.groupBy(F.window("event_time", "1 minute")).count()
                    .withColumnRenamed("count", "recommendations_per_minute")),
        "recommendations_per_minute", "update", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        df.groupBy("product_id").count().withColumnRenamed("count", "recommendation_count")
        .orderBy("recommendation_count", ascending=False),
        "top_recommended_products", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        df.filter(F.col("event_type") == "recommendation_clicked").groupBy("product_id").count()
        .withColumnRenamed("count", "recommendation_clicks")
        .orderBy("recommendation_clicks", ascending=False),
        "recommendation_clicks", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        df.filter(F.col("event_type") == "recommendation_accepted").groupBy("product_id").count()
        .withColumnRenamed("count", "recommendation_acceptance")
        .orderBy("recommendation_acceptance", ascending=False),
        "recommendations_accepted", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    return queries


def start_notifications_metrics(df, sink="console", metrics_path=None, checkpoint_dir=None, trigger_interval="30 seconds"):
    queries = []

    def flat_window(agg_df):
        return (agg_df.withColumn("window_start", F.col("window.start"))
                       .withColumn("window_end", F.col("window.end")).drop("window"))

    queries.append(_start_metric_query(
        df.groupBy("event_type").count().withColumnRenamed("count", "notification_event_count"),
        "notification_events_count", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        df.groupBy().count().withColumnRenamed("count", "total_notifications"),
        "total_notifications", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        flat_window(df.groupBy(F.window("event_time", "1 minute")).count()
                    .withColumnRenamed("count", "notifications_per_minute")),
        "notifications_per_minute", "update", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        df.groupBy("product_id").count().withColumnRenamed("count", "notifications_by_product")
        .orderBy("notifications_by_product", ascending=False),
        "notifications_by_product", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        df.filter(F.col("event_type") == "offer_accepted").groupBy("product_id").count()
        .withColumnRenamed("count", "accepted_offers")
        .orderBy("accepted_offers", ascending=False),
        "accepted_offers", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    queries.append(_start_metric_query(
        df.filter(F.col("event_type") == "offer_declined").groupBy("product_id").count()
        .withColumnRenamed("count", "declined_offers")
        .orderBy("declined_offers", ascending=False),
        "declined_offers", "complete", metrics_path, checkpoint_dir, trigger_interval, sink))

    return queries