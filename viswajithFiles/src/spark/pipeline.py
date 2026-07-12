import os

from src.spark.kafka_reader import read_kafka
from src.spark.parser import create_stream
from src.spark.schemas import *
from src.spark.writers import write_parquet
from src.spark.metrics import (
    start_user_event_metrics, start_recommendation_metrics, start_notifications_metrics
)
from src.spark.validator import (
    validate_user_events, validate_recommendation_events, validate_notification_events
)
from config.config import CHECKPOINT_DIR



def build_pipeline(spark):

    kafka_df = read_kafka(spark)

    user_events_df = create_stream(
        kafka_df,
        "user-events",
        USER_EVENT_SCHEMA
    )
    user_events_df = validate_user_events(user_events_df)

    recommendation_df = create_stream(
        kafka_df,
        "recommendation-actions",
        RECOMMENDATION_SCHEMA
    )
    recommendation_df = validate_recommendation_events(recommendation_df)

    notification_df = create_stream(
        kafka_df,
        "notification-events",
        NOTIFICATION_SCHEMA
    )
    notification_df = validate_notification_events(notification_df)

    queries = []

    queries.append(
        write_parquet(
            user_events_df,
            os.path.join('data/raw/user_events'),
            os.path.join(CHECKPOINT_DIR + '/user_events'),
        )
    )
    queries.append(
        write_parquet(
            recommendation_df,
            os.path.join('data/raw/recommendation_actions'),
            os.path.join(CHECKPOINT_DIR + '/recommendation_actions'),
        )
    )
    queries.append(
        write_parquet(
            notification_df,
            os.path.join('data/raw/notification_events'),
            os.path.join(CHECKPOINT_DIR + '/notification_events'),
        )
    )

    queries.extend(start_user_event_metrics(user_events_df))
    queries.extend(start_recommendation_metrics(recommendation_df))
    queries.extend(start_notifications_metrics(notification_df))

    return queries