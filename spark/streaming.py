import os
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType
)


os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'


VERSION = pyspark.__version__
KAFKA_PKG = f'org.apache.spark:spark-sql-kafka-0-10_2.12:{VERSION}'
TOPIC_LIST = ['view', 'add_to_cart', 'delete_from_cart', 'quantity', 'transaction', 'refund_request']
KAFKA_BROKER = 'localhost:9092'

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
        'view, add_to_cart, delete_from_cart, quantity, transaction, refund_request'
    )
    .option('startingOffsets', 'latest')
    .load()
)

json_df = kafka_df.select(
    F.col('value').cast('string').alias('json')
)

EVENT_SCHEMA = StructType([
    StructField('id', StringType(), False),
    StructField('visitor_id', StringType(), True),
    StructField('user_id', StringType(), True),
    StructField('timestamp', StringType(), True),
    StructField('event_type', StringType(), True),
    StructField('product_id', StringType(), True),
    StructField('order_id_for_refund', StringType(), True),
    StructField('is_recommended', StringType(), True),
])

events_df = (
    json_df.select(
        F.from_json(F.col('json'), EVENT_SCHEMA).alias('data')
    ).select('data.*')
)

events_df.printSchema()


# 'view, add_to_cart, delete_from_cart, quantity, transaction, refund_request'

view_df = events_df.filter(
    F.col('event_type') == 'view'
)
add_to_cart_df = events_df.filter(events_df.event_type == 'add_to_cart')
delete_from_cart_df = events_df.filter(events_df.event_type == 'delete_from_cart')
quantity_df = events_df.filter(events_df.event_type == 'quantity')
transaction_df = events_df.filter(events_df.event_type == 'transaction')
refund_request_df = events_df.filter(events_df.event_type == 'refund_request')



write_to_console_query = (
    events_df.writeStream
    .format('console')
    .outputMode('append')
    # .option('path', './csv_output')
    .start()
)

view_query = (
    view_df.writeStream
    .format("csv")
    .outputMode("append")
    .option("path", "./output/view")
    .option("checkpointLocation", "./checkpoint/view")
    .start()
)

add_to_cart_query = (
    add_to_cart_df.writeStream
    .format("csv")
    .outputMode("append")
    .option("path", "./output/add_to_cart")
    .option("checkpointLocation", "./checkpoint/add_to_cart")
    .start()
)

quantity_query = (
    quantity_df.writeStream
    .format("csv")
    .outputMode("append")
    .option("path", "./output/quantity")
    .option("checkpointLocation", "./checkpoint/quantity")
    .start()
)

delete_from_cart_query = (
    delete_from_cart_df.writeStream
    .format("csv")
    .outputMode("append")
    .option("path", "./output/delete_from_cart")
    .option("checkpointLocation", "./checkpoint/delete_from_cart")
    .start()
)

transaction_query = (
    transaction_df.writeStream
    .format("csv")
    .outputMode("append")
    .option("path", "./output/transaction")
    .option("checkpointLocation", "./checkpoint/transaction")
    .start()
)

refund_request_query = (
    refund_request_df.writeStream
    .format("csv")
    .outputMode("append")
    .option("path", "./output/refund_request")
    .option("checkpointLocation", "./checkpoint/refund_request")
    .start()
)


queries = [
    write_to_console_query,
    view_query,
    add_to_cart_query,
    delete_from_cart_query,
    transaction_query,
    refund_request_query,
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


