import re
import json
import random
import time
import typing

import apache_beam as beam
from apache_beam.io import kafka
from apache_beam import pvalue
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp


class Position(typing.NamedTuple):
    spot: int
    timestamp: float

    def to_bytes(self):
        return json.dumps(self._asdict()).encode("utf-8")

    @classmethod
    def from_bytes(cls, encoded: bytes):
        d = json.loads(encoded.decode("utf-8"))
        return cls(**d)

    @classmethod
    def create(cls, spot: int = random.randint(0, 100), timestamp: float = time.time()):
        return cls(spot=spot, timestamp=timestamp)


class PositionCoder(beam.coders.Coder):
    def encode(self, value: Position):
        return value.to_bytes()

    def decode(self, encoded: bytes):
        return Position.from_bytes(encoded)

    def is_deterministic(self) -> bool:
        return True


class Metric(typing.NamedTuple):
    distance: float
    duration: int

    def to_bytes(self):
        return json.dumps(self._asdict()).encode("utf-8")

    @classmethod
    def from_bytes(cls, encoded: bytes):
        d = json.loads(encoded.decode("utf-8"))
        return cls(**d)


class MetricCoder(beam.coders.Coder):
    def encode(self, value: Metric):
        return value.to_bytes()

    def decode(self, encoded: bytes):
        return Metric.from_bytes(encoded)

    def is_deterministic(self) -> bool:
        return True


beam.coders.registry.register_coder(Position, PositionCoder)
beam.coders.registry.register_coder(Metric, MetricCoder)


class PreProcessInput(beam.PTransform):
    def expand(self, pcoll: pvalue.PCollection):
        def add_timestamp(element: typing.Tuple[str, Position]):
            return TimestampedValue(element, Timestamp.of(element[1].timestamp))

        def to_positions(input: str):
            workout, spot, timestamp = tuple(re.sub("\n", "", input).split("\t"))
            return workout, Position(spot=int(spot), timestamp=float(timestamp))

        return (
            pcoll
            | "ToPositions" >> beam.Map(to_positions)
            | "AddTS" >> beam.Map(add_timestamp)
        )


@beam.typehints.with_output_types(typing.Tuple[str, Position])
class ReadPositionsFromKafka(beam.PTransform):
    def __init__(
        self,
        bootstrap_servers: str,
        topics: typing.List[str],
        group_id: str,
        verbose: bool = False,
        label: str | None = None,
    ):
        super().__init__(label)
        self.boostrap_servers = bootstrap_servers
        self.topics = topics
        self.group_id = group_id
        self.verbose = verbose

    def expand(self, input: pvalue.PBegin):
        def decode_message(kafka_kv: tuple):
            if self.verbose:
                print(kafka_kv)
            return kafka_kv[1].decode("utf-8")

        return (
            input
            | "ReadFromKafka"
            >> kafka.ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": self.boostrap_servers,
                    "auto.offset.reset": "earliest",
                    # "enable.auto.commit": "true",
                    "group.id": self.group_id,
                },
                topics=self.topics,
                timestamp_policy=kafka.ReadFromKafka.create_time_policy,
            )
            | "DecodeMessage" >> beam.Map(decode_message)
            | "PreProcessInput" >> PreProcessInput()
        )


@beam.typehints.with_input_types(typing.Tuple[str, Metric])
class WriteMetricsToKafka(beam.PTransform):
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        verbose: bool = False,
        label: str | None = None,
    ):
        super().__init__(label)
        self.boostrap_servers = bootstrap_servers
        self.topic = topic
        self.verbose = verbose

    def expand(self, pcoll: pvalue.PCollection):
        def create_message(element: typing.Tuple[str, Metric]):
            msg = json.dumps(
                dict(zip(["user", "distance", "duration"], (element[0], *element[1])))
            )
            if self.verbose:
                print(msg)
            return "".encode("utf-8"), msg.encode("utf-8")

        return (
            pcoll
            | "CreateMsg"
            >> beam.Map(create_message).with_output_types(typing.Tuple[bytes, bytes])
            | "Write to Kafka"
            >> kafka.WriteToKafka(
                producer_config={"bootstrap.servers": self.boostrap_servers},
                topic=self.topic,
            )
        )
