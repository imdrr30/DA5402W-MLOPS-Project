
import pyspark


VERSION = pyspark.__version__

KAFKA_PKG = f'org.apache.spark:spark-sql-kafka-0-10_2.12:{VERSION}'

KAFKA_BROKER = 'localhost:9092'

TOPIC_LIST = ['user-events', 'recommendation-actions', 'notification-events' ]

TOPIC_LIST_STR = 'user-events,recommendation-actions,notification-events'

CHECKPOINT_DIR = ''

DATA_DIR = ''

DATABASE_DIR = ''

