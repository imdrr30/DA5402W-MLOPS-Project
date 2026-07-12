from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class Analytics:

    def __init__(self, spark : SparkSession):
        self.spark = spark
        self.user_events = spark.read.parquet("./data/raw/user_events")
        self.recommendations = spark.read.parquet("./data/raw/recommendation_actions")
        self.notifications = spark.read.parquet("./data/raw/notification_events")

    # User metrics

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
            .select(F.col("user_id").alias("customer"))
        )

        return (
            registered
            .union(anonymous)
            .distinct()
            .count()
        )

    # Event Metrics
    
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

    # Product Metrics
    
    def total_views(self):
        return (
            self.user_events
            .filter(F.col('event_type') == 'view')
            .count()
        )

    def total_add_to_cart(self):
        return (
            self.user_events
            .filter(F.col('event_type') == 'add_to_cart')
            .count()
        )

    def total_purchases(self):
        return (
            self.user_events
            .filter(F.col('event_type') == 'transaction')
            .count()
        )

    def total_refunds(self):
        return (
            self.user_events
            .filter(F.col('event_type') == 'refund_request')
            .count()
        )

    # Rankings

    def top_viewed_products(self, limit=10):
        return (
            self.user_events
            .filter(F.col('event_type') == 'view')
            .groupBy('product_id')
            .count()
            .withColumnRenamed('count', 'top_viewed_products')
            .orderBy('top_viewed_products', ascending=False)
        )

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

