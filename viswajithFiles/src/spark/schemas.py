from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType, BooleanType
)



USER_EVENT_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("visitor_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("order_id_for_refund", StringType(), True),
    StructField("is_recommended", BooleanType(), True)
])

RECOMMENDATION_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("visitor_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("order_id_for_refund", StringType(), True),
    StructField("is_recommended", BooleanType(), True)
])

NOTIFICATION_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("visitor_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("order_id_for_refund", StringType(), True),
    StructField("is_recommended", BooleanType(), True)
])
