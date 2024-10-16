import re
import time
import argparse

from producer import TextProducer


def get_digit(shift: str, pattern: str = None):
    try:
        return int(re.sub(pattern, "", shift).strip())
    except (TypeError, ValueError):
        return 0


def get_ts_shift(shift: str = None):
    current = int(time.time())
    multiplier = 1
    if shift.find("m") > 0:
        multiplier = 60
        digit = get_digit(shift, r"m.+|m+")
    elif shift.find("s") > 0:
        digit = get_digit(shift, r"s.+|s+")
    else:
        digit = get_digit(shift)
    return {
        "current": current,
        "shift": f"{digit * multiplier} secs",
        "shifted": (current + digit * multiplier),
    }


def parse_user_input(user_input: str):
    if len(user_input.split(";")) == 2:
        shift, text = tuple(user_input.split(";"))
    else:
        shift, text = "", user_input
    shift_info = get_ts_shift(shift)
    msg = " | ".join(
        [f"{k}: {v}" for k, v in {**{"text": text.strip()}, **shift_info}.items()]
    )
    print(f">> {msg}")
    return {"text": text.strip(), "timestamp_ms": shift_info["shifted"] * 1000}


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
        default="input-topic",
        help="Kafka topic name",
    )
    args = parser.parse_args()

    producer = TextProducer(args.bootstrap_servers, args.topic_name)

    while True:
        user_input = input("ENTER TEXT: ")
        args = parse_user_input(user_input)
        producer.send_to_kafka(**args)
