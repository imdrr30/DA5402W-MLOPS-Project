# scripts/run_daily_analytics.py
import argparse
from pyspark.sql import SparkSession
from analytics import Analytics


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True, help="YYYY-MM-DD, e.g. Airflow's {{ ds }}")
    parser.add_argument("--user-events-path", default="/DB/topic_dumps/user_events")
    parser.add_argument("--recommendations-path", default="/DB/topic_dumps/recommendation_events")
    parser.add_argument("--notifications-path", default="/DB/topic_dumps/notification_events")
    parser.add_argument("--output-path", default="/DB/analytics")
    parser.add_argument("--app-name", default="DailyAnalytics")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName(args.app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    analytics = Analytics(
        spark,
        args.user_events_path,
        args.recommendations_path,
        args.notifications_path,
    )
    analytics.generate_report(args.run_date, args.output_path)
    spark.stop()


if __name__ == "__main__":
    main()