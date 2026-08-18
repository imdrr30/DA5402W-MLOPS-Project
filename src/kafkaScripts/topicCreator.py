import os
import time

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable


BROKERS = os.environ.get("KAFKA_BROKER", "localhost:9094")
TOPICS = ["user-events", "recommendation-actions", "notification-events"]
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1
MAX_RETRIES = 10
RETRY_DELAY_SECONDS = 3


def connect_with_retry():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return KafkaAdminClient(bootstrap_servers=BROKERS, client_id="create-topics-script")
        except NoBrokersAvailable:
            print(f"[{attempt}/{MAX_RETRIES}] Kafka not ready at {BROKERS}, retrying in {RETRY_DELAY_SECONDS}s...")
            time.sleep(RETRY_DELAY_SECONDS)
    raise RuntimeError(f"Could not connect to Kafka at {BROKERS} after {MAX_RETRIES} attempts")




def main():
    print(f"Connecting to Kafka at {BROKERS}...")
    admin = connect_with_retry()

    existing_topics = set(admin.list_topics())
    for name in TOPICS:
        if name in existing_topics:
            print(f"Skipping '{name}'... already exists")
            continue
        try:
            admin.create_topics(new_topics=[
                NewTopic(name=name, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR)
            ])
            print(f"Created '{name}' (partitions={NUM_PARTITIONS}, replication_factor={REPLICATION_FACTOR})")
        except TopicAlreadyExistsError:
            print(f"Skipped '{name}' already exists")

    admin.close()
    print("Topics ready...!")


if __name__ == "__main__":
    main()