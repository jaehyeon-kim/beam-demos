import os
import argparse
import json
import re
import typing
import logging

import apache_beam as beam
from apache_beam.io import kafka
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def decode_message(kafka_kv: tuple):
    print(kafka_kv)
    return kafka_kv[1].decode("utf-8")


def tokenize(element: str):
    return re.findall(r"[A-Za-z\']+", element)


def create_message(element: dict):
    print(element)
    return element["word"].encode("utf-8"), json.dumps(element).encode("utf-8")


class AddWindowTS(beam.DoFn):
    def process(self, element: tuple, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().isoformat(timespec="seconds")
        window_end = window.end.to_utc_datetime().isoformat(timespec="seconds")
        output = {
            "word": element[0],
            "count": element[1],
            "window_start": window_start,
            "window_end": window_end,
        }
        yield output


def run():
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument("--runner", default="FlinkRunner", help="Apache Beam runner")
    parser.add_argument(
        "--use_own",
        action="store_true",
        default="Flag to indicate whether to use an own local cluster",
    )
    parser.add_argument("--length", default="5", type=int, help="Window length")
    parser.add_argument("--top", default="3", type=int, help="Top k")
    parser.add_argument("--input", default="top-k-words-input", help="Input topic")
    parser.add_argument("--output", default="top-k-words-output", help="Ouput topic")
    opts = parser.parse_args()
    print(opts)

    pipeline_opts = {
        "runner": opts.runner,
        "job_name": "top-k-words",
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
                "group.id": "top-k-words",
            },
            topics=[opts.input],
        )
        | "Decode messages" >> beam.Map(decode_message)
        | "Windowing" >> beam.WindowInto(beam.window.FixedWindows(opts.length))
        | "Extract words" >> beam.FlatMap(tokenize)
        | "Count per word" >> beam.combiners.Count.PerElement()
        | "Top k words"
        >> beam.combiners.Top.Of(opts.top, lambda e: e[1]).without_defaults()
        | "Flatten" >> beam.FlatMap(lambda e: e)
        | "Add window timestamp" >> beam.ParDo(AddWindowTS())
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

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
