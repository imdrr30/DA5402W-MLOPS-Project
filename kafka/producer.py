"""
Run:
    python producer.py --records 5000 --rate 50
"""

import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

NUM_SENSORS = 10
STATUSES = ["active", "idle", "error", "maintenance"]
EVENT_TYPE = ['view', 'add_to_cart', 'delete_from_cart', 'quantity', 'transaction', 'refund_request']
EVENT_TYPE_WEIGHTS = [0.7, 0.1, 0.05, 0.1, 0.03, 0.02]
TOPIC_LIST = ['view', 'add_to_cart', 'delete_from_cart', 'quantity', 'transaction', 'refund_request']


def build_record(
        event_id: int, 
        # visitor_id: int, 
        # user_id: int, 
        ts: datetime, 
        # last_record: dict | None
    ):
    """Build one sensor reading, occasionally injecting an anomaly."""
    record = {
        'id' : f'{event_id}',
        "visitor_id": f'visitor_{round(random.uniform(1, 1000))}',
        "user_id": f'user_{round(random.uniform(1, 1000))}',
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "event_type": random.choices(TOPIC_LIST, weights=EVENT_TYPE_WEIGHTS)[0],
        "product_id": f'{round(random.uniform(1, 1000))}',
        "order_id_for_refund": f'{round(random.uniform(1, 900))}',
        'is_recommended' : random.choices(['yes', 'no'], weights=[0.25, 0.75])[0]
    }

    # roll = random.random()
    # if roll < 0.03:
    #     record["temperature"] = None                       # missing value
    # elif roll < 0.05:
    #     record["temperature"] = random.choice([-50.0, 150.0, -25.5, 120.0])  # invalid range
    # elif roll < 0.07:
    #     record["timestamp"] = "NOT_A_TIMESTAMP"             # invalid timestamp
    # elif roll < 0.09:
    #     # For late record implementation from spark question 14
    #     late_ts = ts - timedelta(minutes=random.randint(1, 4))
    #     record["timestamp"] = late_ts.strftime("%Y-%m-%d %H:%M:%S")
    # elif roll < 0.10:
    #     late_ts = ts - timedelta(minutes=random.randint(6, 15))
    #     record["timestamp"] = late_ts.strftime("%Y-%m-%d %H:%M:%S")  # late record
    # elif roll < 0.12 and last_record is not None:
    #     return dict(last_record)                            # exact duplicate

    return record


def main():
    parser = argparse.ArgumentParser(description="Kafka sensor data producer")
    # parser.add_argument("--topic", required=True, help="Kafka topic, e.g. sensor_21f1234567")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--records", type=int, default=2000, help="Total records to publish")
    parser.add_argument("--rate", type=float, default=50.0, help="Target records/sec")
    parser.add_argument("--metrics-out", default="reports/producer_metrics.json")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        api_version=(3, 5, 0),
    )

    sleep_interval = 1.0 / args.rate if args.rate > 0 else 0
    start_time = time.time()
    # last_record = None

    for i in range(args.records):
        ts = datetime.now(timezone.utc)
        event_id = random.randint(1, NUM_SENSORS)
        record = build_record(event_id, ts)

        topic = random.choices(TOPIC_LIST, weights=EVENT_TYPE_WEIGHTS)[0]
        producer.send(topic, key=record["id"], value=record)
        last_record = record

        if sleep_interval:
            time.sleep(sleep_interval)

        if (i + 1) % 500 == 0:
            print(f"Published {i + 1}/{args.records} records...")

    producer.flush()
    elapsed = time.time() - start_time
    throughput = args.records / elapsed if elapsed > 0 else 0

    # print(f"Done. Published {args.records} records to '{topic}' in {elapsed:.2f}s "
    #       f"=> throughput = {throughput:.2f} records/sec")

    metrics = {
        # "topic": topic,
        "records_published": args.records,
        "elapsed_seconds": elapsed,
        "producer_throughput_rps": throughput,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    
    print(metrics)


if __name__ == "__main__":
    main()
