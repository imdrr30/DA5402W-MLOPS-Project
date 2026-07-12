from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.config import (
    USER_EVENTS, RECOMMENDATION_EVENTS, NOTIFICATION_EVENTS
)


def validate_user_events(df):
    
    cleaned_df = (
        df
        # apply water mark for late-arriaval
        .withWatermark('timestamp', '10 minutes')
        # remove invalid timestamps
        .filter(F.col('timestamp').isNotNull())
        # remove events without event id
        .filter(F.col('id').isNotNull())
        # remove vents without visitor id
        .filter(F.col('visitor_id').isNotNull())
        # remove events without event_type
        .filter(F.col('event_type').isNotNull())
        # remove duplicate events
        .dropDuplicates(["id"])
        # remove invalid events
        .filter(F.col("event_type").isin(USER_EVENTS))
        # remove events without product id
        .filter(F.col('product_id').isNotNull())
        # remove product id less than 0
        .filter(F.col('product_id') > 0)
        # remove refund request without order id
        .filter(
            ~(
                (F.col('event_type') == 'refund_request') &
                (F.col('order_id_for_refund').isNull())
            )
        )
    )

    return cleaned_df


def validate_recommendation_events(df):
    
    cleaned_df = (
        df
        # apply water mark for late-arriaval
        .withWatermark('timestamp', '10 minutes')
        # remove invalid timestamps
        .filter(F.col('timestamp').isNotNull())
        # remove events without event id
        .filter(F.col('id').isNotNull())
        # remove vents without visitor id
        .filter(F.col('visitor_id').isNotNull())
        # remove events without event_type
        .filter(F.col('event_type').isNotNull())
        # remove duplicate events
        .dropDuplicates(["id"])
        # remove invalid events
        .filter(F.col("event_type").isin(RECOMMENDATION_EVENTS))
        # remove events without product id
        .filter(F.col('product_id').isNotNull())
        # remove product id less than 0
        .filter(F.col('product_id') > 0)
        # remove events without recommendation flag
        .filter(F.col('is_recommended').isNotNull())
        # remove events that are not recommendations
        .filter(F.col('is_recommended') == True)
    )

    return cleaned_df

def validate_notification_events(df):
    
    cleaned_df = (
        df
        # apply water mark for late-arriaval
        .withWatermark('timestamp', '10 minutes')
        # remove invalid timestamps
        .filter(F.col('timestamp').isNotNull())
        # remove events without event id
        .filter(F.col('id').isNotNull())
        # remove vents without visitor id
        .filter(F.col('visitor_id').isNotNull())
        # remove events without event_type
        .filter(F.col('event_type').isNotNull())
        # remove duplicate events
        .dropDuplicates(["id"])
        # remove invalid events
        .filter(F.col("event_type").isin(NOTIFICATION_EVENTS))
    )

    return cleaned_df


