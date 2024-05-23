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


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--deploy",
        dest="deploy",
        action="store_true",
        default="Flag to indicate whether to deploy to a cluster",
    )
    parser.add_argument(
        "--bootstrap_servers",
        dest="bootstrap",
        default="host.docker.internal:29092",
        help="Kafka bootstrap server addresses",
    )
    parser.add_argument(
        "--input_topic",
        dest="input",
        default="input-topic",
        help="Kafka input topic name",
    )
    parser.add_argument(
        "--output_topic",
        dest="output",
        default="output-topic-beam",
        help="Kafka output topic name",
    )
    parser.add_argument(
        "--group_id",
        dest="group",
        default="beam-word-len",
        help="Kafka output group ID",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    print(known_args)
    print(pipeline_args)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    expansion_service = None
    if known_args.deploy is True:
        expansion_service = kafka.default_io_expansion_service(
            append_args=[
                "--defaultEnvironmentType=PROCESS",
                '--defaultEnvironmentConfig={"command": "/opt/apache/beam/boot"}',
                "--experiments=use_deprecated_read",  # https://github.com/apache/beam/issues/20979
            ]
        )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadWordsFromKafka"
            >> ReadWordsFromKafka(
                bootstrap_servers=known_args.bootstrap,
                topics=[known_args.input],
                group_id=known_args.group,
                expansion_service=expansion_service,
            )
            | "CalculateAvgWordLen" >> CalculateAvgWordLen()
            | "WriteWordLenToKafka"
            >> WriteWordLenToKafka(
                bootstrap_servers=known_args.bootstrap,
                topic=known_args.output,
                expansion_service=expansion_service,
            )
        )

        logging.getLogger().setLevel(logging.DEBUG)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
