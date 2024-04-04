import time
import argparse
import random
import typing

from producer import TextProducer


class Position(typing.NamedTuple):
    latitude: float
    longitude: float
    timestamp: int

    @classmethod
    def random(cls, stamp: int = int(time.time() * 1000)):
        return cls(
            latitude=(random.random() - 0.5) * 180,
            longitude=(random.random() - 0.5) * 180,
            timestamp=stamp,
        )

    def move(self):
        return Position(
            latitude=self.latitude + random.random() * 0.001,
            longitude=self.longitude + random.random() * 0.001,
            timestamp=int(time.time() * 1000),
        )


class PositionGenerator:
    def __init__(self, num_users: int) -> None:
        self.num_users = num_users
        self.position_by_user = dict(
            zip(
                [f"user{i + 1}" for i in range(self.num_users)],
                [Position.random() for i in range(self.num_users)],
            )
        )

    def update_position(self):
        current_user = f"user{random.randint(1, self.num_users)}"
        self.position_by_user[current_user] = self.position_by_user[current_user].move()
        return "\t".join(
            map(str, (current_user,) + tuple(self.position_by_user[current_user]))
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(__file__, description="Sport Data Generator")
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
        "--num_users",
        "-n",
        type=int,
        default=5,
        help="Number of users",
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
    position_gen = PositionGenerator(args.num_users)

    num_events = 0
    while True:
        num_events += 1
        text = position_gen.update_position()
        producer.send_to_kafka(text)
        if num_events % 10 == 0:
            print(f"{num_events} text sent. current - {text}")
        time.sleep(args.delay_seconds)
