import os
import argparse
import datetime
import json
import re
import logging
import typing

import apache_beam as beam
from apache_beam.io import kafka
from apache_beam.transforms.window import GlobalWindows, TimestampCombiner
from apache_beam.transforms.trigger import AfterCount, AccumulationMode, AfterWatermark
from apache_beam.transforms.util import Reify
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def decode_message(kafka_kv: tuple):
    print(kafka_kv)
    return kafka_kv[1].decode("utf-8")


def tokenize(element: str):
    return re.findall(r"[A-Za-z\']+", element)


def create_message(element: typing.Tuple[datetime.datetime, str]):
    msg = json.dumps({"created_at": element[0].isoformat(), "word": element[1]})
    print(msg)
    return "".encode("utf-8"), msg.encode("utf-8")


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
        "--output", default="max-word-length-output", help="Ouput topic"
    )
    opts = parser.parse_args()
    print(opts)

    pipeline_opts = {
        "runner": opts.runner,
        "job_name": "max-word-length",
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
                "group.id": "max-word-length",
            },
            topics=[opts.input],
        )
        | "Decode messages" >> beam.Map(decode_message)
        | "Windowing"
        >> beam.WindowInto(
            GlobalWindows(),
            trigger=AfterWatermark(early=AfterCount(1)),
            allowed_lateness=0,
            timestamp_combiner=TimestampCombiner.OUTPUT_AT_LATEST,
            accumulation_mode=AccumulationMode.ACCUMULATING,
        )
        | "Extract words" >> beam.FlatMap(tokenize)
        | "Get longest word" >> beam.combiners.Top.Of(1, key=len).without_defaults()
        | "Flatten" >> beam.FlatMap(lambda e: e)
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
            topic=opts.output,
        )
    )

    logging.getLogger().setLevel(logging.WARN)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
