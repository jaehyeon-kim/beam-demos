import os
import datetime
import json
import argparse
import re
import logging
import typing

import apache_beam as beam
from apache_beam.io import kafka
from apache_beam.transforms.window import GlobalWindows, TimestampCombiner
from apache_beam.transforms.trigger import AfterCount, AccumulationMode, Repeatedly
from apache_beam.transforms.util import Reify
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class AverageAccumulator(typing.NamedTuple):
    length: int
    count: int


beam.coders.registry.register_coder(AverageAccumulator, beam.coders.RowCoder)


def decode_message(kafka_kv: tuple):
    print(kafka_kv)
    return kafka_kv[1].decode("utf-8")


def tokenize(element: str):
    return re.findall(r"[A-Za-z\']+", element)


def create_message(element: typing.Tuple[datetime.datetime, float]):
    msg = json.dumps({"created_at": element[0].isoformat(), "avg_len": element[1]})
    print(msg)
    return "".encode("utf-8"), msg.encode("utf-8")


class AddTS(beam.DoFn):
    def process(self, avg_len: float, ts_param=beam.DoFn.TimestampParam):
        yield ts_param.to_utc_datetime(), avg_len


class AverageFn(beam.CombineFn):
    def create_accumulator(self):
        return AverageAccumulator(length=0, count=0)

    def add_input(self, mutable_accumulator: AverageAccumulator, element: str):
        length, count = tuple(mutable_accumulator)
        return AverageAccumulator(length=length + len(element), count=count + 1)

    def merge_accumulators(self, accumulators: typing.List[AverageAccumulator]):
        lengths, counts = zip(*accumulators)
        return AverageAccumulator(length=sum(lengths), count=sum(counts))

    def extract_output(self, accumulator: AverageAccumulator):
        length, count = tuple(accumulator)
        return length / count if count else float("NaN")

    def get_accumulator_coder(self):
        return beam.coders.registry.get_coder(AverageAccumulator)


def run():
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument("--runner", default="FlinkRunner", help="Apache Beam runner")
    parser.add_argument(
        "--use_own",
        action="store_true",
        default="Flag to indicate whether to use an own local cluster",
    )
    parser.add_argument("--input", default="input-topic", help="Input topic")
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
        )
        | "Decode messages" >> beam.Map(decode_message)
        | "Windowing"
        >> beam.WindowInto(
            GlobalWindows(),
            trigger=Repeatedly(AfterCount(1)),
            allowed_lateness=0,
            timestamp_combiner=TimestampCombiner.OUTPUT_AT_LATEST,
            accumulation_mode=AccumulationMode.ACCUMULATING,
            # closing behaviour - EMIT_ALWAYS, on_time_behavior - FIRE_ALWAYS
        )
        | "Extract words" >> beam.FlatMap(tokenize)
        | "Get avg word length" >> beam.CombineGlobally(AverageFn()).without_defaults()
        | "Reify" >> Reify.Timestamp()
        | "Add timestamp" >> beam.ParDo(AddTS())
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
