from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class Analytics:

    def __init__(self, spark: SparkSession, user_events_path: str,
                 recommendations_path: str, notifications_path: str):
        self.spark = spark
        self.user_events = spark.read.parquet(user_events_path)
        self.recommendations = spark.read.parquet(recommendations_path)
        self.notifications = spark.read.parquet(notifications_path)

    # ---------------------------------------------------------
    # User metrics
    # ---------------------------------------------------------

    def total_registered_customers(self):
        return (
            self.user_events
            .filter(F.col('user_id').isNotNull())
            .select('user_id')
            .distinct()
            .count()
        )

    def total_anonymous_visitors(self):
        return (
            self.user_events
            .filter(F.col('user_id').isNull())
            .select('visitor_id')
            .distinct()
            .count()
        )

    def total_customers(self):
        registered = (
            self.user_events
            .filter(F.col("user_id").isNotNull())
            .select(F.col("user_id").alias("customer"))
        )

        anonymous = (
            self.user_events
            .filter(F.col("user_id").isNull())
            .select(F.col("visitor_id").alias("customer"))  # fixed: was user_id
        )

        return (
            registered
            .union(anonymous)
            .distinct()
            .count()
        )

    # ---------------------------------------------------------
    # Event metrics
    # ---------------------------------------------------------

    def total_events(self):
        return self.user_events.count()

    def events_by_type(self):
        return (
            self.user_events
            .groupBy('event_type')
            .count()
            .withColumnRenamed('count', 'event_count')
            .orderBy('event_count', ascending=False)
        )

    # ---------------------------------------------------------
    # Product metrics
    # ---------------------------------------------------------

    def total_views(self):
        return self.user_events.filter(F.col('event_type') == 'view').count()

    def total_add_to_cart(self):
        return self.user_events.filter(F.col('event_type') == 'add_to_cart').count()

    def total_purchases(self):
        return self.user_events.filter(F.col('event_type') == 'transaction').count()

    def total_refunds(self):
        return self.user_events.filter(F.col('event_type') == 'refund_request').count()

    # ---------------------------------------------------------
    # Rankings
    # ---------------------------------------------------------

    def top_viewed_products(self, limit=10):
        return (
            self.user_events
            .filter(F.col('event_type') == 'view')
            .groupBy('product_id')
            .count()
            .withColumnRenamed('count', 'view_count')
            .orderBy('view_count', ascending=False)
            .limit(limit)
        )

    def top_cart_products(self, limit=10):
        return (
            self.user_events
            .filter(F.col('event_type') == 'add_to_cart')
            .groupBy('product_id')
            .count()
            .withColumnRenamed('count', 'add_to_cart_count')
            .orderBy('add_to_cart_count', ascending=False)
            .limit(limit)
        )

    def top_purchased_products(self, limit=10):
        return (
            self.user_events
            .filter(F.col('event_type') == 'transaction')
            .groupBy('product_id')
            .count()
            .withColumnRenamed('count', 'purchase_count')
            .orderBy('purchase_count', ascending=False)
            .limit(limit)
        )

    def most_active_users(self, limit=10):
        return (
            self.user_events
            .filter(F.col('user_id').isNotNull())
            .groupBy('user_id')
            .count()
            .withColumnRenamed('count', 'event_count')
            .orderBy('event_count', ascending=False)
            .limit(limit)
        )

    def most_active_visitors(self, limit=10):
        return (
            self.user_events
            .filter(F.col('user_id').isNull())
            .groupBy('visitor_id')
            .count()
            .withColumnRenamed('count', 'event_count')
            .orderBy('event_count', ascending=False)
            .limit(limit)
        )

    # ---------------------------------------------------------
    # Recommendation metrics
    # ---------------------------------------------------------

    def recommendations_generated(self):
        return self.recommendations.count()

    def recommendation_event_counts(self):
        return (
            self.recommendations
            .groupBy('event_type')
            .count()
            .withColumnRenamed('count', 'event_count')
            .orderBy('event_count', ascending=False)
        )

    # ---------------------------------------------------------
    # Notification metrics
    # ---------------------------------------------------------

    def notifications_generated(self):
        return self.notifications.count()

    def notification_counts(self):
        return (
            self.notifications
            .groupBy('event_type')
            .count()
            .withColumnRenamed('count', 'event_count')
            .orderBy('event_count', ascending=False)
        )

    # ---------------------------------------------------------
    # Report generation (for the dashboard)
    # ---------------------------------------------------------

    def generate_report(self, run_date: str, output_path: str):
        """
        Computes every metric and writes two kinds of output under output_path:
        - a single-row summary (scalar metrics) per run_date
        - one small table per ranking metric, tagged with run_date

        Overwrite mode per run_date partition makes reruns idempotent --
        rerunning the same date replaces that date's output instead of duplicating it.
        """
        summary = {
            "run_date": run_date,
            "total_registered_customers": self.total_registered_customers(),
            "total_anonymous_visitors": self.total_anonymous_visitors(),
            "total_customers": self.total_customers(),
            "total_events": self.total_events(),
            "total_views": self.total_views(),
            "total_add_to_cart": self.total_add_to_cart(),
            "total_purchases": self.total_purchases(),
            "total_refunds": self.total_refunds(),
            "recommendations_generated": self.recommendations_generated(),
            "notifications_generated": self.notifications_generated(),
        }

        (self.spark.createDataFrame([summary])
            .coalesce(1)
            .write.mode("overwrite")
            .json(f"{output_path}/summary/run_date={run_date}"))

        rankings = {
            "events_by_type": self.events_by_type(),
            "top_viewed_products": self.top_viewed_products(),
            "top_cart_products": self.top_cart_products(),
            "top_purchased_products": self.top_purchased_products(),
            "most_active_users": self.most_active_users(),
            "most_active_visitors": self.most_active_visitors(),
            "recommendation_event_counts": self.recommendation_event_counts(),
            "notification_counts": self.notification_counts(),
        }

        for name, df in rankings.items():
            (df
                .withColumn("run_date", F.lit(run_date))
                .coalesce(1)
                .write.mode("overwrite")
                .json(f"{output_path}/{name}/run_date={run_date}"))

        print(f"[INFO] Analytics report for {run_date} written to {output_path}")


