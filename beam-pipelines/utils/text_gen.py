import os
import time

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
    BOOTSTRAP_SERVERS = [os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")]
    TOPIC_NAME = os.getenv("TOPIC_NAME", "text-input")

    data_gen = TextGen(BOOTSTRAP_SERVERS, TOPIC_NAME)
    fake = Faker()

    num_events = 0
    while True:
        num_events += 1
        text = fake.text()
        data_gen.send_to_kafka(text)
        if num_events % 10 == 0:
            print(f"{num_events} text sent. current - {text}")
        time.sleep(1)
