import os
import time
import argparse

from kafka import KafkaProducer
from faker import Faker


class TextGen:
    def __init__(self, bootstrap_servers: list, topic_name: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.kafka_producer = self.create_producer()

    def create_producer(self):
        """
        Returns a KafkaProducer instance
        """
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: v.encode("utf-8"),
        )

    def send_to_kafka(self, text: str):
        """
        Sends text to a Kafka topic.
        """
        try:
            self.kafka_producer.send(self.topic_name, value=text)
            self.kafka_producer.flush()
        except Exception as e:
            raise RuntimeError("fails to send a message") from e


if __name__ == "__main__":
    parser = argparse.ArgumentParser(__file__, description="Text Data Generator")
    parser.add_argument(
        "--bootstrap_servers",
        "-b",
        type=str,
        default="localhost:29092",
        help="Comma separated string of Kafka bootstrap addresses",
    )
    parser.add_argument(
        "--topic_name",
        "-t",
        type=str,
        default="text-input",
        help="Kafka topic name",
    )
    parser.add_argument(
        "--delay_seconds",
        "-d",
        type=float,
        default=1,
        help="The amount of time that a record should be delayed. Only applicable to streaming.",
    )
    args = parser.parse_args()

    data_gen = TextGen(args.bootstrap_servers, args.topic_name)
    fake = Faker()

    num_events = 0
    while True:
        num_events += 1
        text = fake.text()
        data_gen.send_to_kafka(text)
        if num_events % 10 == 0:
            print(f"{num_events} text sent. current - {text}")
        time.sleep(args.delay_seconds)
