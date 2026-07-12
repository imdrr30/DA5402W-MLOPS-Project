from pyspark.sql import functions as F


# streaming queries for metrics
def start_user_event_metrics(df):
    queries = []


    # event counts
    queries.append(
        df
        .groupBy('event_type')
        .count()
        .withColumnRenamed('count', 'event_count')
        .writeStream
        .format('console')
        .outputMode('complete')
        .option('truncate', False)
        .start()
    )

    # total events received
    queries.append(
        df
        .groupBy()
        .count()
        .withColumnRenamed('count', 'total_events_received')
        .writeStream
        .format('console')
        .outputMode('complete')
        .option('truncate', False)
        .start()
    )

    # events per minute
    queries.append(
        df
        .groupBy(
            F.window('timestamp', '1 minute')
        )
        .count()
        .withColumnRenamed('count', 'events_per_minute')
        .writeStream
        .format('console')
        .outputMode('complete')
        .option('truncate', False)
        .start()
    )

    # views per minute
    queries.append(
        df
        .filter(
            F.col('event_type') == 'view'
        )
        .groupBy(
            F.window('timestamp', '1 minute')
        )
        .count()
        .withColumnRenamed('count', 'views_per_minute')
        .writeStream
        .format('console')
        .outputMode('complete')
        .option('truncate', False)
        .start()
    )

    # add to cart per minute
    queries.append(
        df
        .filter(
            F.col('event_type') == 'add_to_cart'
        )
        .groupBy(
            F.window('timestamp', '1 minute')
        )
        .count()
        .withColumnRenamed('count', 'add_to_cart_per_minute')
        .writeStream
        .format('console')
        .outputMode('complete')
        .option('truncate', False)
        .start()
    )

    # top viewed products
    queries.append(
        df
        .filter(
            F.col('event_type') == 'view'
        )
        .groupBy('product_id')
        .count()
        .withColumnRenamed('count', 'top_viewed_products')
        .orderBy('top_viewed_products', ascending=False)
        .writeStream
        .format('console')
        .outputMode('complete')
        .option('truncate', False)
        .start()
    )

    # top products added to cart
    queries.append(
        df
        .filter(
            F.col('event_type') == 'add_to_cart'
        )
        .groupBy('product_id')
        .count()
        .withColumnRenamed('count', 'top_products_added_to_cart')
        .orderBy('top_products_added_to_cart', ascending=False)
        .writeStream
        .format('console')
        .outputMode('complete')
        .option('truncate', False)
        .start()
    )



    return queries




def start_recommendation_metrics(df):
    queries = []
    return queries


def start_notifications_metrics(df):
    queries = []
    return queries



