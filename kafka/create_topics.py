from kafka.admin import KafkaAdminClient, NewTopic

KAFKA_BROKER = 'localhost:9092'
TOPIC_LIST = [
    'view', 
    'add_to_cart', 
    'delete_from_cart', 
    'quantity', 
    'transaction', 
    'refund_request'
]


def create_kafka_topic():
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER,
        client_id='admin_setup'
    )

    # avoid creating exiting topics
    existing_topics = admin.list_topics()

    topics_to_create = []

    for topic in TOPIC_LIST:
        if topic in existing_topics:
            print(f'{topic} alreadys exists...')
        else:
            print(f'Creating new topic {topic}...')
            newtopic = NewTopic(
                name=topic,
                num_partitions=3,
                replication_factor=1,
            )
            topics_to_create.append(newtopic)
    
    if topics_to_create:
        admin.create_topics(new_topics=topics_to_create)
        print('All new topics created...!')
    else:
        print('Nothing to create :/')

    admin.close()


if __name__=='__main__':
    create_kafka_topic()
