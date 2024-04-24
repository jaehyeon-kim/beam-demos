import re
import json
import random
import time
import typing

import apache_beam as beam
from apache_beam.io import kafka
from apache_beam import pvalue
from apache_beam.transforms.util import Reify
from apache_beam.transforms.window import GlobalWindows, TimestampedValue, BoundedWindow
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec,
    BagStateSpec,
    TimerSpec,
    on_timer,
)
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


# class MetricCoder(beam.coders.Coder):
#     def encode(self, value: Metric):
#         return value.to_bytes()

#     def decode(self, encoded: bytes):
#         return Metric.from_bytes(encoded)

#     def is_deterministic(self) -> bool:
#         return True


beam.coders.registry.register_coder(Position, PositionCoder)
# beam.coders.registry.register_coder(Metric, MetricCoder)


class ToMetricFn(beam.DoFn):
    MIN_TIMESTAMP = ReadModifyWriteStateSpec("min_timestamp", beam.coders.FloatCoder())
    BUFFER = BagStateSpec("buffer", PositionCoder())
    FLUSH_TIMER = TimerSpec("flush", TimeDomain.WATERMARK)

    def __init__(self, verbose: bool = False):
        self.verbose = verbose

    def process(
        self,
        element: typing.Tuple[str, Position],
        timestamp=beam.DoFn.TimestampParam,
        buffer=beam.DoFn.StateParam(BUFFER),
        min_timestamp=beam.DoFn.StateParam(MIN_TIMESTAMP),
        flush_timer=beam.DoFn.TimerParam(FLUSH_TIMER),
    ):
        min_ts: Timestamp = min_timestamp.read()
        if min_ts is None:
            min_timestamp.write(timestamp)
            flush_timer.set(timestamp)
        buffer.add(element[1])
        if self.verbose and element[0] == "user0":
            print(
                f">>>>ToMetricFn.process<<<< key {element[0]} ts {element[1].timestamp}"
            )

    @on_timer(FLUSH_TIMER)
    def flush(
        self,
        key=beam.DoFn.KeyParam,
        buffer=beam.DoFn.StateParam(BUFFER),
        min_timestamp=beam.DoFn.StateParam(MIN_TIMESTAMP),
    ):
        items: typing.List[Position] = []
        for item in buffer.read():
            items.append(item)
            if self.verbose and key == "user0":
                print(f">>>>ToMetricFn.flush  <<<< key {key} ts {item.timestamp}")

        items = list(sorted(items, key=lambda p: p.timestamp))
        outputs = list(self.flush_metrics(items, key))

        buffer.clear()
        buffer.add(items[-1])
        min_timestamp.clear()
        return outputs

    def flush_metrics(self, items: typing.List[Position], key: str):
        i = 1
        while i < len(items):
            last = items[i - 1]
            next = items[i]
            distance = abs(next.spot - last.spot)
            duration = next.timestamp - last.timestamp
            if duration > 0:
                yield TimestampedValue(
                    (key, Metric(distance, duration)),
                    Timestamp.of(last.timestamp),
                )
            i += 1


@beam.typehints.with_input_types(typing.Tuple[str, Position])
class ComputeBoxedMetrics(beam.PTransform):
    def __init__(self, verbose: bool = False, label: str | None = None):
        super().__init__(label)
        self.verbose = verbose

    def expand(self, pcoll: pvalue.PCollection):
        return (
            pcoll
            | beam.WindowInto(GlobalWindows())
            | beam.ParDo(ToMetricFn(verbose=self.verbose))
        )


class MeanPaceCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return Metric(0, 0)

    def add_input(self, mutable_accumulator: Metric, element: Metric):
        return Metric(*tuple(map(sum, zip(mutable_accumulator, element))))

    def merge_accumulators(self, accumulators: typing.List[Metric]):
        return Metric(*tuple(map(sum, zip(*accumulators))))

    def extract_output(self, accumulator: Metric):
        if accumulator.duration == 0:
            return float("NaN")
        return accumulator.distance / (accumulator.duration)

    def get_accumulator_coder(self):
        return beam.coders.registry.get_coder(Metric)


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

        def add_timestamp(element: typing.Tuple[str, Position]):
            return TimestampedValue(element, Timestamp.of(element[1].timestamp))

        def to_positions(input: str):
            workout, spot, timestamp = tuple(re.sub("\n", "", input).split("\t"))
            return workout, Position(spot=int(spot), timestamp=float(timestamp))

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
            )
            | "DecodeMsg" >> beam.Map(decode_message)
            | "ToPositions" >> beam.Map(to_positions)
            | "AddTS" >> beam.Map(add_timestamp)
        )


class WriteNotificationsToKafka(beam.PTransform):
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
        def create_message(element: tuple):
            msg = json.dumps({"track": element[0], "notification": element[1]})
            if self.verbose:
                print(msg)
            return element[0].encode("utf-8"), msg.encode("utf-8")

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


class ElemToKV(beam.PTransform):
    def __init__(self, indicator: str, verbose: bool, label: str | None = None):
        super().__init__(label)
        self.indicator = indicator
        self.verbose = verbose

    def expand(self, pcoll: pvalue.PCollection):
        def elem_to_kv(
            element: typing.Tuple[typing.Tuple[str, Metric], Timestamp, BoundedWindow],
        ) -> typing.Tuple[str, Metric]:
            value, timestamp, window = element
            if self.verbose and value[0] == "user0":
                print(
                    f">>>>ElemToKV.{self.indicator}<<<<{str(window)} {timestamp} {value}"
                )
            return value

        return pcoll | Reify.Window() | beam.Map(elem_to_kv)


class MetricToKV(beam.PTransform):
    def __init__(self, indicator: str, verbose: bool, label: str | None = None):
        super().__init__(label)
        self.indicator = indicator
        self.verbose = verbose

    def expand(self, pcoll: pvalue.PCollection):
        def metric_to_kv(
            element,  #: typing.Tuple[typing.Tuple[str, float], Timestamp, BoundedWindow],
        ) -> typing.Tuple[str, float]:
            value, timestamp, window = element
            if self.verbose and value[0] == "user1":
                print(
                    f">>>>MetricToKV.{self.indicator}<<<<{str(window)} {timestamp} {value}"
                )
            return value

        return pcoll | Reify.Window() | beam.Map(metric_to_kv)
