from config.config import KAFKA_BROKER
from config.config import TOPIC_LIST_STR



def read_kafka(spark):

    return (
        spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', KAFKA_BROKER)
        .option(
            'subscribe', 
            TOPIC_LIST_STR
        )
        .option('failOnDataLoss', 'false')
        .option('startingOffsets', 'earliest')
        .load()
    )

