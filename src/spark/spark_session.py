from pyspark.sql import SparkSession


def create_spark():
    
    return (
        SparkSession.builder
        .appName('user-event-streaming')
        .master('local[4]')
        .config(
            'spark.jars.packages',
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0'
        )
        .getOrCreate()
    )



