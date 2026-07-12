from pyspark.sql import functions as F


# streaming queries for metrics
def start_user_event_metrics(df):
    queries = []

    # active users
    # queries.append(
    #     df
    #     .filter(F.col("user_id").isNotNull())
    #     .groupBy()
    #     .agg(F.countDistinct("user_id").alias("active_users"))
    #     .writeStream
        # .queryName("active users")
    #     .format("console")
    #     .outputMode("complete")
    #     .option("truncate", False)
    #     .start()
    # )

    # # active visitors
    # queries.append(
    #     df
    #     .groupBy()
    #     .agg(F.countDistinct("visitor_id").alias("active_visitors"))
    #     .writeStream
        # .queryName("active visitors")
    #     .format("console")
    #     .outputMode("complete")
    #     .option("truncate", False)
    #     .start()
    # )

    # event counts
    queries.append(
        df
        .groupBy('event_type')
        .count()
        .withColumnRenamed('count', 'event_count')
        .writeStream
        .queryName("event counts")
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
        .queryName("total events received")
        .format('console')
        .outputMode('complete')
        .option('truncate', False)
        .start()
    )

    # events per minute
    queries.append(
        df
        .groupBy(F.window('timestamp', '1 minute'))
        .count()
        .withColumnRenamed('count', 'events_per_minute')
        .writeStream
        .queryName("events per minute")
        .format('console')
        .outputMode('complete')
        .option('truncate', False)
        .start()
    )

    # views per minute
    queries.append(
        df
        .filter(F.col('event_type') == 'view')
        .groupBy(F.window('timestamp', '1 minute'))
        .count()
        .withColumnRenamed('count', 'views_per_minute')
        .writeStream
        .queryName("views per minute")
        .format('console')
        .outputMode('complete')
        .option('truncate', False)
        .start()
    )

    # add to cart per minute
    queries.append(
        df
        .filter(F.col('event_type') == 'add_to_cart')
        .groupBy(F.window('timestamp', '1 minute'))
        .count()
        .withColumnRenamed('count', 'add_to_cart_per_minute')
        .writeStream
        .queryName("add to cart per minute")
        .format('console')
        .outputMode('complete')
        .option('truncate', False)
        .start()
    )

    # top viewed products
    queries.append(
        df
        .filter(F.col('event_type') == 'view')
        .groupBy('product_id')
        .count()
        .withColumnRenamed('count', 'top_viewed_products')
        .orderBy('top_viewed_products', ascending=False)
        .writeStream
        .queryName("top viewed products")
        .format('console')
        .outputMode('complete')
        .option('truncate', False)
        .start()
    )

    # top products added to cart
    queries.append(
        df
        .filter(F.col('event_type') == 'add_to_cart')
        .groupBy('product_id')
        .count()
        .withColumnRenamed('count', 'top_products_added_to_cart')
        .orderBy('top_products_added_to_cart', ascending=False)
        .writeStream
        .queryName("top products added to cart")
        .format('console')
        .outputMode('complete')
        .option('truncate', False)
        .start()
    )

    # purchase per minute
    queries.append(
        df
        .filter(F.col("event_type") == "transaction")
        .groupBy(F.window("timestamp", "1 minute"))
        .count()
        .withColumnRenamed("count", "purchases_per_minute")
        .writeStream
        .queryName("purchase per minute")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    # refunds per minute
    queries.append(
        df
        .filter(F.col("event_type") == "refund_request")
        .groupBy(F.window("timestamp", "1 minute"))
        .count()
        .withColumnRenamed("count", "refunds_per_minute")
        .writeStream
        .queryName("refunds per minute")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    # top purchased product
    queries.append(
        df
        .filter(F.col("event_type") == "transaction")
        .groupBy("product_id")
        .count()
        .withColumnRenamed("count", "top_purchased_products")
        .orderBy("top_purchased_products", ascending=False)
        .writeStream
        .queryName("top purchased product")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    # top refunded product
    queries.append(
        df
        .filter(F.col("event_type") == "refund_request")
        .groupBy("product_id")
        .count()
        .withColumnRenamed("count", "top_refunded_products")
        .orderBy("top_refunded_products", ascending=False)
        .writeStream
        .queryName("top refunded product")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )


    return queries




def start_recommendation_metrics(df):
    queries = []

    # recommendation events by type
    queries.append(
        df
        .groupBy("event_type")
        .count()
        .withColumnRenamed("count", "recommendation_event_count")
        .writeStream
        .queryName("recommendation events by type")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    # total recommendations received
    queries.append(
        df
        .groupBy("event_type")
        .count()
        .withColumnRenamed("count", "total_recommendations_received")
        .writeStream
        .queryName("total recommendations received")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    # recommendations per minute
    queries.append(
        df
        .groupBy(F.window("timestamp", "1 minute"))
        .count()
        .withColumnRenamed("count", "recommendations_per_minute")
        .writeStream
        .queryName("recommendations per minute")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    # top recommended products
    queries.append(
        df
        .groupBy("product_id")
        .count()
        .withColumnRenamed("count", "recommendation_count")
        .orderBy("recommendation_count", ascending=False)
        .writeStream
        .queryName("top recommended products")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    # recommendation clicks
    queries.append(
        df
        .filter(F.col("event_type") == "recommendation_clicked")
        .groupBy("product_id")
        .count()
        .withColumnRenamed("count", "recommendation_clicks")
        .orderBy("recommendation_clicks", ascending=False)
        .writeStream
        .queryName("recommendation clicks")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    # recommendations accepted
    queries.append(
        df
        .filter(F.col("event_type") == "recommendation_accepted")
        .groupBy("product_id")
        .count()
        .withColumnRenamed("count", "recommendation_acceptance")
        .orderBy("recommendation_acceptance", ascending=False)
        .writeStream
        .queryName("recommendations accepted")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    return queries


def start_notifications_metrics(df):
    queries = []

    # notification events count
    queries.append(
        df
        .groupBy("event_type")
        .count()
        .withColumnRenamed("count", "notification_event_count")
        .writeStream
        .queryName("notification events count")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    # total notifications
    queries.append(
        df
        .groupBy()
        .count()
        .withColumnRenamed("count", "total_notifications")
        .writeStream
        .queryName("total notifications")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    # notifications per minute
    queries.append(
        df
        .groupBy(F.window("timestamp", "1 minute"))
        .count()
        .withColumnRenamed("count", "notifications_per_minute")
        .writeStream
        .queryName("notifications per minute")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    # notifications by product
    queries.append(
        df
        .groupBy("product_id")
        .count()
        .withColumnRenamed("count", "notifications_by_product")
        .orderBy("notifications_by_product", ascending=False)
        .writeStream
        .queryName("notifications by product")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    # accepted offers
    queries.append(
        df
        .filter(F.col("event_type") == "offer_accepted")
        .groupBy("product_id")
        .count()
        .withColumnRenamed("count", "accepted_offers")
        .orderBy("accepted_offers", ascending=False)
        .writeStream
        .queryName("accepted offers")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )

    # declined offers
    queries.append(
        df
        .filter(F.col("event_type") == "offer_declined")
        .groupBy("product_id")
        .count()
        .withColumnRenamed("count", "declined_offers")
        .orderBy("declined_offers", ascending=False)
        .writeStream
        .queryName("declined offers")
        .format("console")
        .outputMode("complete")
        .option("truncate", False)
        .start()
    )


    return queries



