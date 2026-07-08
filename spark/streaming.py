import os
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType, BooleanType
)


os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'


VERSION = pyspark.__version__
KAFKA_PKG = f'org.apache.spark:spark-sql-kafka-0-10_2.12:{VERSION}'
KAFKA_BROKER = 'localhost:9092'
TOPIC_LIST = ['user-events', 'recommendation-actions', 'notification-events' ]

spark = (
    SparkSession.builder
    .appName('user-event-streaming')
    .master('local[4]')
    .config(
        'spark.jars.packages',
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0'
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel('WARN')


kafka_df = (
    spark.readStream
    .format('kafka')
    .option('kafka.bootstrap.servers', KAFKA_BROKER)
    .option(
        'subscribe', 
        'user-events,recommendation-actions,notification-events'
    )
    .option('failOnDataLoss', 'false')
    .option('startingOffsets', 'earliest')
    .load()
)

json_df = kafka_df.select(
    F.col('value').cast('string').alias('json')
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


# user_events_df = events_df.filter(
#     F.col("topic") == "user-events"
# )

# recommendation_df = events_df.filter(
#     F.col("topic") == "recommendation-actions"
# )

# notification_df = events_df.filter(
#     F.col("topic") == "notification-events"
# )

user_events_raw = json_df.filter(F.col("topic") == "user-events")
recommendation_raw = json_df.filter(F.col("topic") == "recommendation-actions")
notification_raw = json_df.filter(F.col("topic") == "notification-events")


user_events_df = (
    user_events_raw
    .select(F.from_json("json", USER_EVENT_SCHEMA).alias("data"))
    .select("data.*")
)
user_events_df.printSchema()

recommendation_df = (
    recommendation_raw
    .select(F.from_json("json", RECOMMENDATION_SCHEMA).alias("data"))
    .select("data.*")
)
recommendation_df.printSchema()

notification_df = (
    notification_raw
    .select(F.from_json("json", NOTIFICATION_SCHEMA).alias("data"))
    .select("data.*")
)
notification_df.printSchema()




user_events_df_query = (
    user_events_df.writeStream
    .format('console')
    .outputMode('append')
    .option("truncate", "false")
    .option("numRows", 20)
    .start()
)

recommendation_df_query = (
    recommendation_df.writeStream
    .format('console')
    .outputMode('append')
    .option("truncate", "false")
    .option("numRows", 20)
    .start()
)

notification_df_query = (
    notification_df.writeStream
    .format('console')
    .outputMode('append')
    .option("truncate", "false")
    .option("numRows", 20)
    .start()
)



queries = [
    user_events_df_query,
    recommendation_df_query,
    notification_df_query,
]


try:
    for query in queries:
        query.awaitTermination()
except KeyboardInterrupt:
    for query in queries:
        query.stop()
    print('*' * 100)
    print('Stopping queries')
    print('*' * 100)
finally:
    print('-' * 50)
    print('All queries completed')
    print('-' * 50)




