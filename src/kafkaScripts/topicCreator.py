"""
create_topics.py
--------------------------------------------------------------------------
Creates the Kafka topics used by eventProducer.py / spark_customer_stream.py.

Plain script, no CLI args -- just edit the config values below and run:

    python create_topics.py

Requires: pip install kafka-python
--------------------------------------------------------------------------
"""

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

# --------------------------------------------------------------------------
# Config -- edit these as needed
# --------------------------------------------------------------------------
BROKERS = "localhost:9094"          # e.g. "kafka:9092" if running inside the Docker network
TOPICS = ["user-events", "recommendation-actions", "notification-events"]
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1
# --------------------------------------------------------------------------


def main():
    print(f"Connecting to Kafka at {BROKERS}...")
    try:
        admin = KafkaAdminClient(bootstrap_servers=BROKERS, client_id="create-topics-script")
    except Exception as e:
        print(f"Could not connect to any brokers at {BROKERS}: {e}\n"
              f"Check the broker address/port and that Kafka is running.")
        return

    existing_topics = set(admin.list_topics())

    for name in TOPICS:
        if name in existing_topics:
            print(f"[SKIP] '{name}' already exists")
            continue
        try:
            admin.create_topics(new_topics=[
                NewTopic(name=name, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR)
            ])
            print(f"[CREATED] '{name}' (partitions={NUM_PARTITIONS}, replication_factor={REPLICATION_FACTOR})")
        except TopicAlreadyExistsError:
            print(f"[SKIP] '{name}' already exists (created concurrently)")

    admin.close()
    print("Done.")


if __name__ == "__main__":
    main()