import os
import json
import argparse
import re
import logging
import typing

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import kafka
from apache_beam.transforms.window import FixedWindows
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class WordAccum(typing.NamedTuple):
    length: int
    count: int


beam.coders.registry.register_coder(WordAccum, beam.coders.RowCoder)


def decode_message(kafka_kv: tuple, verbose: bool = False):
    if verbose:
        print(kafka_kv)
    return kafka_kv[1].decode("utf-8")


def tokenize(element: str):
    return re.findall(r"[A-Za-z\']+", element)


def create_message(element: typing.Tuple[str, str, float]):
    msg = json.dumps(dict(zip(["window_start", "window_end", "avg_len"], element)))
    print(msg)
    return "".encode("utf-8"), msg.encode("utf-8")


class AverageFn(beam.CombineFn):
    def create_accumulator(self):
        return WordAccum(length=0, count=0)

    def add_input(self, mutable_accumulator: WordAccum, element: str):
        length, count = tuple(mutable_accumulator)
        return WordAccum(length=length + len(element), count=count + 1)

    def merge_accumulators(self, accumulators: typing.List[WordAccum]):
        lengths, counts = zip(*accumulators)
        return WordAccum(length=sum(lengths), count=sum(counts))

    def extract_output(self, accumulator: WordAccum):
        length, count = tuple(accumulator)
        return length / count if count else float("NaN")

    def get_accumulator_coder(self):
        return beam.coders.registry.get_coder(WordAccum)


class AddWindowTS(beam.DoFn):
    def process(self, avg_len: float, win_param=beam.DoFn.WindowParam):
        yield (
            win_param.start.to_rfc3339(),
            win_param.end.to_rfc3339(),
            avg_len,
        )


class ReadWordsFromKafka(beam.PTransform):
    def __init__(
        self,
        bootstrap_servers: str,
        topics: typing.List[str],
        group_id: str,
        verbose: bool = False,
        expansion_service: typing.Any = None,
        label: str | None = None,
    ) -> None:
        super().__init__(label)
        self.boostrap_servers = bootstrap_servers
        self.topics = topics
        self.group_id = group_id
        self.verbose = verbose
        self.expansion_service = expansion_service

    def expand(self, input: pvalue.PBegin):
        return (
            input
            | "ReadFromKafka"
            >> kafka.ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": self.boostrap_servers,
                    "auto.offset.reset": "latest",
                    # "enable.auto.commit": "true",
                    "group.id": self.group_id,
                },
                topics=self.topics,
                timestamp_policy=kafka.ReadFromKafka.create_time_policy,
                commit_offset_in_finalize=True,
                expansion_service=self.expansion_service,
            )
            | "DecodeMessage" >> beam.Map(decode_message)
            | "Tokenize" >> beam.FlatMap(tokenize)
        )


class CalculateAvgWordLen(beam.PTransform):
    def expand(self, input: pvalue.PCollection):
        return (
            input
            | "Windowing" >> beam.WindowInto(FixedWindows(size=5))
            | "GetAvgWordLength" >> beam.CombineGlobally(AverageFn()).without_defaults()
        )


class WriteWordLenToKafka(beam.PTransform):
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        expansion_service: typing.Any = None,
        label: str | None = None,
    ) -> None:
        super().__init__(label)
        self.boostrap_servers = bootstrap_servers
        self.topic = topic
        self.expansion_service = expansion_service

    def expand(self, input: pvalue.PCollection):
        return (
            input
            | "AddWindowTS" >> beam.ParDo(AddWindowTS())
            | "CreateMessages"
            >> beam.Map(create_message).with_output_types(typing.Tuple[bytes, bytes])
            | "WriteToKafka"
            >> kafka.WriteToKafka(
                producer_config={"bootstrap.servers": self.boostrap_servers},
                topic=self.topic,
                expansion_service=self.expansion_service,
            )
        )


def run(args=None):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument("--runner", default="FlinkRunner", help="Apache Beam runner")
    parser.add_argument(
        "--deploy",
        action="store_true",
        default="Flag to indicate whether to use an own local cluster",
    )
    opts, _ = parser.parse_known_args(args)

    pipeline_opts = {
        "runner": opts.runner,
        "job_name": "avg-word-length-beam",
        "streaming": True,
        "environment_type": "EXTERNAL" if opts.deploy is True else "LOOPBACK",
        "checkpointing_interval": "60000",
    }

    expansion_service = None
    if opts.deploy is True:
        pipeline_opts = {
            **pipeline_opts,
            **{
                "environment_config": json.dumps(
                    {"command": "/opt/apache/beam_python/boot"}
                ),
                "flink_submit_uber_jar": True,
            },
        }
        expansion_service = kafka.default_io_expansion_service(
            append_args=[
                "--defaultEnvironmentType=PROCESS",
                '--defaultEnvironmentConfig={"command": "/opt/apache/beam/boot"}',
                "--experiments=use_deprecated_read",  # https://github.com/apache/beam/issues/20979
            ]
        )
    print(pipeline_opts)
    options = PipelineOptions([], **pipeline_opts)
    # Required, else it will complain that when importing worker functions
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadWordsFromKafka"
            >> ReadWordsFromKafka(
                bootstrap_servers=os.getenv(
                    "BOOTSTRAP_SERVERS",
                    "host.docker.internal:29092",
                ),
                topics=[os.getenv("INPUT_TOPIC", "input-topic")],
                group_id=os.getenv("GROUP_ID", "beam-word-len"),
                expansion_service=expansion_service,
            )
            | "CalculateAvgWordLen" >> CalculateAvgWordLen()
            | "WriteWordLenToKafka"
            >> WriteWordLenToKafka(
                bootstrap_servers=os.getenv(
                    "BOOTSTRAP_SERVERS",
                    "host.docker.internal:29092",
                ),
                topic=os.getenv("OUTPUT_TOPIC", "output-topic-beam"),
                expansion_service=expansion_service,
            )
        )

        logging.getLogger().setLevel(logging.INFO)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
