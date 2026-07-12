from pyspark.sql import functions as F




def create_stream(df, topic_name, schema):

    return (
        df
        .filter(
            F.col("topic") == topic_name
        )
        .select(
            F.from_json(
                F.col("value").cast("string"),
                schema
            ).alias("data")
        )
        .select("data.*")
        .withColumn(
            "timestamp",
            F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss")
        )
    )


