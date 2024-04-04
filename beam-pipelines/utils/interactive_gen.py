import argparse

from producer import TextProducer

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
        text = input("Enter text: ")
        producer.send_to_kafka(text)
