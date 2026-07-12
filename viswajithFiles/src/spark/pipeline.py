import os

from src.spark.kafka_reader import read_kafka
from src.spark.parser import create_stream
from src.spark.schemas import *
from src.spark.writers import write_parquet
from src.spark.metrics import (
    start_user_event_metrics, start_recommendation_metrics, start_notifications_metrics
)

def build_pipeline(spark):

    kafka_df = read_kafka(spark)

    user_events_df = create_stream(
        kafka_df,
        "user-events",
        USER_EVENT_SCHEMA
    )

    recommendation_df = create_stream(
        kafka_df,
        "recommendation-actions",
        RECOMMENDATION_SCHEMA
    )

    notification_df = create_stream(
        kafka_df,
        "notification-events",
        NOTIFICATION_SCHEMA
    )

    queries = []

    queries.append(
        write_parquet(
            user_events_df,
            os.path.join('data/raw/user_events'),
            os.path.join('checkpoints/user_events'),
        )
    )
    queries.append(
        write_parquet(
            recommendation_df,
            os.path.join('data/raw/recommendation_actions'),
            os.path.join('checkpoints/recommendation_actions'),
        )
    )
    queries.append(
        write_parquet(
            notification_df,
            os.path.join('data/raw/notification_events'),
            os.path.join('checkpoints/notification_events'),
        )
    )

    queries.extend(start_user_event_metrics(user_events_df))
    queries.extend(start_recommendation_metrics(user_events_df))
    queries.extend(start_notifications_metrics(user_events_df))

    return queries