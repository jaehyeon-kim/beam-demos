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
        default="input-topic",
        help="Kafka topic name",
    )
    parser.add_argument(
        "--max_shift_seconds",
        "-m",
        type=float,
        default=15,
        help="The maximum amount of time that a message create stamp is shifted back.",
    )
    parser.add_argument(
        "--delay_seconds",
        "-d",
        type=float,
        default=1,
        help="The amount of time that a record should be delayed.",
    )
    args = parser.parse_args()

    producer = TextProducer(args.bootstrap_servers, args.topic_name)
    fake = Faker()
    Faker.seed(1237)

    num_events = 0
    while True:
        num_events += 1
        text = fake.text(max_nb_chars=10)
        current = int(time.time())
        shift = fake.random_element(range(args.max_shift_seconds))
        shifted = current - shift
        producer.send_to_kafka(text=text, timestamp_ms=shifted * 1000)
        print(
            f"text - {text}, ts - {current}, shift - {shift} secs - shifted ts {shifted}"
        )
        time.sleep(args.delay_seconds)
