import re
import time
import argparse

from producer import TextProducer


def set_timestamp_ms(shift: str):
    timestamp_ms = int(time.time() * 1000)
    if len(shift.split()) == 2:
        digit, unit = tuple(shift.split())
        if len(re.findall(r"m+", unit)) > 0:
            multiplier = 60 * 1000
        elif len(re.findall(r"s+", unit)) > 0:
            multiplier = 1000
        else:
            multiplier = 1
        timestamp_ms += int(digit) * multiplier
    return timestamp_ms


def parse_user_input(user_input: str):
    if len(re.split(r"[:;|]", user_input)) == 2:
        shift, text = tuple(user_input.split(";"))
        timestamp_ms = set_timestamp_ms(shift)
        return {"text": text.lstrip(), "timestamp_ms": timestamp_ms}
    else:
        return {"text": user_input}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        __file__, description="Interactive Text Data Generator"
    )
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
    args = parser.parse_args()

    producer = TextProducer(args.bootstrap_servers, args.topic_name)

    while True:
        user_input = input("Enter text: ")
        args = parse_user_input(user_input)
        producer.send_to_kafka(**args)
