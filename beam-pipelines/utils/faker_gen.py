import time
import argparse

from faker import Faker
from producer import TextProducer

if __name__ == "__main__":
    parser = argparse.ArgumentParser(__file__, description="Fake Text Data Generator")
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

    producer = TextProducer(args.bootstrap_servers, args.topic_name)
    fake = Faker()

    num_events = 0
    while True:
        num_events += 1
        text = fake.text()
        producer.send_to_kafka(text)
        if num_events % 10 == 0:
            print(f"{num_events} text sent. current - {text}")
        time.sleep(args.delay_seconds)
