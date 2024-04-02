import os
import argparse
import datetime
import json
import re
import logging
import typing
import math

import apache_beam as beam
from apache_beam.io import kafka
from apache_beam.transforms.window import (
    GlobalWindows,
    TimestampCombiner,
    TimestampedValue,
)
from apache_beam.transforms.trigger import (
    Repeatedly,
    AccumulationMode,
    AfterProcessingTime,
)
from apache_beam.transforms.util import Reify
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class Position(typing.NamedTuple):
    latitude: float
    longitude: float
    timestamp: int


class Metric(typing.NamedTuple):
    length: float
    duration: int


beam.coders.registry.register_coder(Position, beam.coders.RowCoder)
beam.coders.registry.register_coder(Metric, beam.coders.RowCoder)


def decode_message(kafka_kv: tuple):
    print(kafka_kv)
    return kafka_kv[1].decode("utf-8")


def create_message(element: typing.Tuple[datetime.datetime, str]):
    msg = json.dumps({"created_at": element[0].isoformat(), "word": element[1]})
    print(msg)
    return "".encode("utf-8"), msg.encode("utf-8")


def to_positions(input: str):
    workout, latitude, longitude, timestamp = tuple(re.sub("\n", "", input).split("\t"))
    return workout, Position(
        latitude=float(latitude), longitude=float(longitude), timestamp=int(timestamp)
    )


def distance(first: Position, second: Position):
    EARTH_DIAMETER = 6_371_000
    delta_latitude = (first.latitude - second.latitude) * math.pi / 180
    delta_longitude = (first.longitude - second.longitude) * math.pi / 180
    latitude_inc_in_meters = math.sqrt(2 * (1 - math.cos(delta_latitude)))
    longitude_inc_in_meters = math.sqrt(2 * (1 - math.cos(delta_longitude)))
    return EARTH_DIAMETER * math.sqrt(
        latitude_inc_in_meters * latitude_inc_in_meters
        + longitude_inc_in_meters * longitude_inc_in_meters
    )


def compute_matrics(key: str, positions: typing.List[Position]):
    last: Position = None
    total_time = 0
    total_distance = 0
    for p in sorted(positions, key=lambda p: p.timestamp):
        if last is not None:
            total_distance += distance(last, p)
            total_time += p.timestamp - last.timestamp
        last = p
    return key, total_time, total_distance


def assign_timestamp(element: typing.Tuple[str, Position]):
    return TimestampedValue(element, element[1].timestamp)


class AddTS(beam.DoFn):
    def process(self, word: str, ts_param=beam.DoFn.TimestampParam):
        yield ts_param.to_utc_datetime(), word


def run():
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument("--runner", default="FlinkRunner", help="Apache Beam runner")
    parser.add_argument(
        "--use_own",
        action="store_true",
        default="Flag to indicate whether to use an own local cluster",
    )
    parser.add_argument("--input", default="text-input", help="Input topic")
    parser.add_argument(
        "--job_name",
        default=re.sub("_", "-", re.sub(".py$", "", os.path.basename(__file__))),
        help="Job name",
    )
    opts = parser.parse_args()
    print(opts)

    pipeline_opts = {
        "runner": opts.runner,
        "job_name": opts.job_name,
        "environment_type": "LOOPBACK",
        "streaming": True,
        "parallelism": 3,
        "experiments": [
            "use_deprecated_read"
        ],  ## https://github.com/apache/beam/issues/20979
        "checkpointing_interval": "60000",
    }
    if opts.use_own is True:
        pipeline_opts = {**pipeline_opts, **{"flink_master": "localhost:8081"}}
    print(pipeline_opts)
    options = PipelineOptions([], **pipeline_opts)
    # Required, else it will complain that when importing worker functions
    options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=options)
    (
        p
        | "Read from Kafka"
        >> kafka.ReadFromKafka(
            consumer_config={
                "bootstrap.servers": os.getenv(
                    "BOOTSTRAP_SERVERS",
                    "host.docker.internal:29092",
                ),
                "auto.offset.reset": "earliest",
                # "enable.auto.commit": "true",
                "group.id": opts.job_name,
            },
            topics=[opts.input],
            timestamp_policy=kafka.ReadFromKafka.create_time_policy,
        )
        | "Decode messages" >> beam.Map(decode_message)
        | "To positions" >> beam.Map(to_positions)
        | "With timestamps" >> beam.Map(assign_timestamp)
        | "Windowing"
        >> beam.WindowInto(
            GlobalWindows(),
            trigger=Repeatedly(AfterProcessingTime(10)),
            allowed_lateness=60 * 60,
            timestamp_combiner=TimestampCombiner.OUTPUT_AT_LATEST,
            accumulation_mode=AccumulationMode.ACCUMULATING,
        )
        | "Group by workout" >> beam.GroupByKey()
        | "Compute metrics" >> beam.Map(lambda e: compute_matrics(*e))
        | "Create messages"
        >> beam.Map(create_message).with_output_types(typing.Tuple[bytes, bytes])
        | "Write to Kafka"
        >> kafka.WriteToKafka(
            producer_config={
                "bootstrap.servers": os.getenv(
                    "BOOTSTRAP_SERVERS",
                    "host.docker.internal:29092",
                )
            },
            topic=opts.job_name,
        )
    )

    logging.getLogger().setLevel(logging.WARN)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
