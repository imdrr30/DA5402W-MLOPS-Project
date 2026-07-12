import os
import pyspark


VERSION = pyspark.__version__

KAFKA_PKG = f'org.apache.spark:spark-sql-kafka-0-10_2.12:{VERSION}'

KAFKA_BROKER = 'localhost:9092'

TOPIC_LIST = [
    'user-events', 
    'recommendation-actions', 
    'notification-events' 
]

TOPIC_LIST_STR = 'user-events,recommendation-actions,notification-events'

CHECKPOINT_DIR = 'checkpoints/'

DATA_DIR = 'data/'

DATABASE_DIR = ''

USER_EVENTS = [
    "view",
    "add_to_cart",
    "delete_from_cart",
    "quantity",
    "transaction",
    "refund_request",
]

RECOMMENDATION_EVENTS = [
    "recommendation_shown",
    "recommendation_clicked",
    "recommendation_accepted",
    "recommendation_rejected",
]

NOTIFICATION_EVENTS = [
    "discount_offer_sent",
    "reminder_sent",
    "offer_accepted",
    "offer_declined",
]

