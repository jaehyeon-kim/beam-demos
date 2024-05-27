import os
import time

from faker import Faker
from kafka import KafkaProducer


class TextProducer:
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

    def send_to_kafka(self, text: str, timestamp_ms: int = None):
        """
        Sends text to a Kafka topic.
        """
        try:
            args = {"topic": self.topic_name, "value": text}
            if timestamp_ms is not None:
                args = {**args, **{"timestamp_ms": timestamp_ms}}
            self.kafka_producer.send(**args)
            self.kafka_producer.flush()
        except Exception as e:
            raise RuntimeError("fails to send a message") from e


if __name__ == "__main__":
    producer = TextProducer(
        os.getenv("BOOTSTRAP_SERVERS", "localhost:29092"),
        os.getenv("TOPIC_NAME", "input-topic"),
    )
    fake = Faker()

    num_events = 0
    while True:
        num_events += 1
        text = fake.text()
        producer.send_to_kafka(text)
        if num_events % 5 == 0:
            print(f"<<<<<{num_events} text sent... current>>>>\n{text}")
        time.sleep(int(os.getenv("DELAY_SECONDS", "1")))
