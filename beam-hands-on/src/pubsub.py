import os
import time
import argparse
from typing import List

from google.cloud import pubsub_v1
from google.oauth2 import service_account
from google.api_core import exceptions


class Helper:
    @staticmethod
    def get_credentials():
        current_dir = os.path.dirname(os.path.realpath(__file__))
        sa_key_path = os.path.join(current_dir, "sa_key", "key.json")
        return service_account.Credentials.from_service_account_file(
            sa_key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )


class Publisher:
    def __init__(self, project_id: str, topic_name: str):
        self.topic = f"projects/{project_id}/topics/{topic_name}"
        self.client = pubsub_v1.PublisherClient(credentials=Helper.get_credentials())

    def topic_exists(self):
        try:
            self.client.get_topic(topic=self.topic)
            return True
        except exceptions.NotFound:
            return False
        except Exception as e:
            raise RuntimeError(e) from e

    def create_topic(self):
        try:
            self.client.create_topic(name=self.topic)
        except Exception as e:
            raise RuntimeError(e) from e

    def publish(self, event_data: bytes):
        try:
            self.client.publish(self.topic, event_data)
        except Exception as e:
            raise RuntimeError(e) from e

    def read_lines(self, file_path: str, ignore_header: bool = True):
        with open(file_path, "rb") as lines:
            if ignore_header:
                lines.readline()
            return lines.readlines()

    def publish_items(self, lines: List[bytes]):
        for ind, line in enumerate(lines):
            print(f"publishing {line} to {self.topic}")
            self.publish(line)
            wait_for = 1 if ind % 10 != 0 else 5
            time.sleep(wait_for)


class Subscriber:
    def __init__(self, project_id: str, topic_name: str, sub_name: str):
        self.subscription = f"projects/{project_id}/subscriptions/{sub_name}"
        self.topic = f"projects/{project_id}/topics/{topic_name}"
        self.client = pubsub_v1.SubscriberClient(credentials=Helper.get_credentials())

    def subscription_exists(self):
        try:
            self.client.get_subscription(subscription=self.subscription)
            return True
        except exceptions.NotFound:
            return False
        except Exception as e:
            raise RuntimeError(e) from e

    def create_subscription(self):
        try:
            self.client.create_subscription(name=self.subscription, topic=self.topic)
        except Exception as e:
            raise RuntimeError(e) from e

    def subscribe(self):
        def callback(message):
            print(message)
            message.ack()

        return self.client.subscribe(self.subscription, callback)


if __name__ == "__main__":
    """
        Examples
        > Publisher
            python pubsub --is-pub -p <project-id> -t <topic-name>
        > Subscriber
            python pubsub -p <project-id> -t <topic-name> -s <subscription-name>
    """
    parser = argparse.ArgumentParser(description="Specify pubsub app arguments...")
    parser.add_argument(
        "--is-pub",
        action="store_true",
        help="flag to indicate publisher or subscriber",
    )
    parser.add_argument("-p", "--project", type=str, help="project ID")
    parser.add_argument("-t", "--topic", type=str, default="input", help="topic name")
    parser.add_argument(
        "-s", "--sub", type=str, default="input-sub", help="subscription name"
    )
    parser.add_argument(
        "-i", "--item", default="counts", type=str, help="source item file"
    )

    args = parser.parse_args()
    if args.is_pub:
        pub = Publisher(args.project, args.topic)
        if not pub.topic_exists():
            pub.create_topic()
        current_dir = os.path.dirname(os.path.realpath(__file__))
        items = pub.read_lines(os.path.join(current_dir, f"{args.item}.csv"))
        pub.publish_items(lines=items)

    if not args.is_pub:
        sub = Subscriber(args.project, args.topic, args.sub)
        if not sub.subscription_exists():
            sub.create_subscription()
        future = sub.subscribe()
        try:
            future.result()
        except KeyboardInterrupt:
            future.cancel()  # Trigger the shutdown.
            future.result()  # Block until the shutdown is complete.
