


def write_parquet(df, write_path, checkpoint):
    return (
        df.writeStream
        .format('parquet')
        .outputMode('append')
        .option('path', write_path)
        .option('checkpointLocation', checkpoint)
        .start()
    )
