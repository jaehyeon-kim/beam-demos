import time
import argparse
import random
import typing

from producer import TextProducer


class Position(typing.NamedTuple):
    spot: int
    timestamp: float

    @classmethod
    def create(cls, spot: int = random.randint(0, 100), timestamp: float = time.time()):
        return cls(spot=spot, timestamp=timestamp)


class TrackGenerator:
    def __init__(self, num_tracks: int, delay_seconds: int) -> None:
        self.num_tracks = num_tracks
        self.delay_seconds = delay_seconds
        self.positions = [
            Position.create(spot=random.randint(0, 110)) for _ in range(self.num_tracks)
        ]

    def update_positions(self):
        for ind, position in enumerate(self.positions):
            self.positions[ind] = self.move(
                start=position,
                duration=time.time() - position.timestamp,
                delta=random.randint(-10, 10),
            )

    def move(self, start: Position, duration: float, delta: int):
        spot, timestamp = tuple(start)
        return Position(spot=spot + delta, timestamp=timestamp + duration)

    def create_tracks(self):
        tracks = []
        for ind, position in enumerate(self.positions):
            track = f"user{ind}\t{position.spot}\t{position.timestamp}"
            print(track)
            tracks.append(track)
        return tracks


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
        "--num_tracks",
        "-n",
        type=int,
        default=5,
        help="Number of tracks",
    )
    parser.add_argument(
        "--delay_seconds",
        "-d",
        type=float,
        default=2,
        help="The amount of time that a record should be delayed.",
    )
    args = parser.parse_args()

    producer = TextProducer(args.bootstrap_servers, args.topic_name)
    track_gen = TrackGenerator(args.num_tracks, args.delay_seconds)

    while True:
        tracks = track_gen.create_tracks()
        for track in tracks:
            producer.send_to_kafka(text=track)
        track_gen.update_positions()
        time.sleep(random.randint(0, args.delay_seconds))
