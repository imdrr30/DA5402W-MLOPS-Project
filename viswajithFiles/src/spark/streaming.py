import os
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.spark.schemas import *
from src.spark.spark_session import create_spark
from src.spark.pipeline import build_pipeline



os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'




def main():
    
    spark = create_spark()
    spark.sparkContext.setLogLevel('WARN')

    queries = build_pipeline(spark)

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
        spark.stop()


if __name__ == '__main__':
    main()