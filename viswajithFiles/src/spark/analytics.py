from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class Analytics:

    def __init__(self, spark : SparkSession):
        self.spark = spark
        self.user_events = spark.read.parquet("./data/raw/user_events")
        self.recommendations = spark.read.parquet("./data/raw/recommendation_actions")
        self.notifications = spark.read.parquet("./data/raw/notification_events")

    def total_registered_customers(self):
        return (
            self.user_events
            .filter(F.col('user_id').isNotNull())
            .select('user_id')
            .distinct()
            .count()
        )


    def total_anonymous_visitors(self):
        pass

    def total_customers(self):
        pass

    # ---------------------------------------------------------
    # Event Metrics
    # ---------------------------------------------------------

    def total_events(self):
        pass

    def events_by_type(self):
        pass

    # ---------------------------------------------------------
    # Product Metrics
    # ---------------------------------------------------------

    def total_views(self):
        pass

    def total_add_to_cart(self):
        pass

    def total_purchases(self):
        pass

    def total_refunds(self):
        pass

    # ---------------------------------------------------------
    # Rankings
    # ---------------------------------------------------------

    def top_viewed_products(self, limit=10):
        pass

    def top_cart_products(self, limit=10):
        pass

    def top_purchased_products(self, limit=10):
        pass

    def most_active_users(self, limit=10):
        pass

    def most_active_visitors(self, limit=10):
        pass

    # ---------------------------------------------------------
    # Recommendation Metrics
    # ---------------------------------------------------------

    def recommendations_generated(self):
        pass

    def recommendation_event_counts(self):
        pass

    # ---------------------------------------------------------
    # Notification Metrics
    # ---------------------------------------------------------

    def notifications_generated(self):
        pass

    def notification_counts(self):
        pass

