#!/usr/bin/env python

import argparse
import time

from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

DATA_FILE = 'data/sample.txt'
TOPIC = "beam-topic"
LIMIT = 100

NUM_PARTITIONS = 4


def main(bootstrap_server: str, data_file: str, topic: str, limit: int, consumer: bool, create: bool):
    if create:
        create_topic(bootstrap_server=bootstrap_server, topic_name=topic)
    elif consumer:
        read_messages(bootstrap_server=bootstrap_server, topic_name=topic)
    else:
        produce_messages(bootstrap_server=bootstrap_server, data_filename=data_file, limit=limit, topic_name=topic)


def produce_messages(bootstrap_server: str, data_filename: str, topic_name: str, limit: int):
    producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
    with open(data_filename, 'r') as f:
        k = 0
        for line in f:
            future = producer.send(topic=topic_name, partition=k % NUM_PARTITIONS, value=line.encode())

            k += 1
            if k % 5 == 0:
                print(f"{k} msgs published")
            if k >= limit:
                break

        print(f"Total: {k} msgs published")


def read_messages(bootstrap_server: str, topic_name: str):
    peeker: KafkaConsumer = KafkaConsumer(topic_name,
                                          group_id='peeker-group',
                                          bootstrap_servers=[bootstrap_server])

    partitions = peeker.partitions_for_topic(topic_name)
    print(f"Found {len(partitions)} partitions")
    consumers = []

    for partition in partitions:
        tp = TopicPartition(topic=topic_name, partition=partition)
        consumer: KafkaConsumer = KafkaConsumer(group_id='consumer-group2',
                                                bootstrap_servers=[bootstrap_server],
                                                auto_offset_reset='earliest',
                                                enable_auto_commit=False)
        consumer.assign([tp])
        consumers.append(consumer)

    while True:
        total = 0
        for consumer in consumers:
            current_partition = consumer.assignment().pop().partition

            batch = consumer.poll()

            last_seen_offset = consumer.committed(TopicPartition(topic=topic_name, partition=current_partition))

            k = 0
            for tp, values in batch.items():
                for v in values:
                    k += 1

            # Commit consumed offsets to ensure exactly once processing
            if batch:
                consumer.commit()

            print(
                f"{k} msgs read from partition {current_partition} (offset last: {consumer.end_offsets([tp])[tp]}, initial: {last_seen_offset})")
            total += k
        print(f"Total: {total}")

        time.sleep(3)


def create_topic(bootstrap_server: str, topic_name: str):
    admin_client: KafkaAdminClient = KafkaAdminClient(
        bootstrap_servers=bootstrap_server,
        client_id='create-topic')

    topics = admin_client.list_topics()
    topic = NewTopic(name=topic_name, num_partitions=NUM_PARTITIONS, replication_factor=1)
    if topic_name in topics:
        print(f"Topic {topic_name} already exists, re-creating")
        admin_client.delete_topics(topics=[topic_name], timeout_ms=50000)
        time.sleep(3)

    admin_client.create_topics(new_topics=[topic], validate_only=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--create", action='store_true')
    parser.add_argument("--consumer", action='store_true')
    parser.add_argument("--bootstrap", type=str, required=True)

    args = parser.parse_args()
    is_consumer = args.consumer
    is_create = args.create
    bootstrap = args.bootstrap

    main(bootstrap_server=bootstrap,
         data_file=DATA_FILE,
         topic=TOPIC,
         limit=LIMIT,
         consumer=is_consumer,
         create=is_create)
