import os
import argparse
import json
import re
import typing
import logging

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms.window import FixedWindows
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from word_process_utils import tokenize, ReadWordsFromKafka, WriteProcessOutputsToKafka


class CalculateTopKWords(beam.PTransform):
    def __init__(
        self, window_length: int, top_k: int, label: str | None = None
    ) -> None:
        self.window_length = window_length
        self.top_k = top_k
        super().__init__(label)

    def expand(self, pcoll: pvalue.PCollection):
        return (
            pcoll
            | "Windowing" >> beam.WindowInto(FixedWindows(size=self.window_length))
            | "Tokenize" >> beam.FlatMap(tokenize)
            | "CountPerWord" >> beam.combiners.Count.PerElement()
            | "TopKWords"
            >> beam.combiners.Top.Of(self.top_k, lambda e: e[1]).without_defaults()
            | "Flatten" >> beam.FlatMap(lambda e: e)
        )


def create_message(element: typing.Tuple[str, str, str, int]):
    msg = json.dumps(dict(zip(["window_start", "window_end", "word", "freq"], element)))
    print(msg)
    return "".encode("utf-8"), msg.encode("utf-8")


class AddWindowTS(beam.DoFn):
    def process(self, top_k: typing.Tuple[str, int], win_param=beam.DoFn.WindowParam):
        yield (
            win_param.start.to_rfc3339(),
            win_param.end.to_rfc3339(),
            top_k[0],
            top_k[1],
        )


class CreateMessags(beam.PTransform):
    def expand(self, pcoll: pvalue.PCollection):
        return (
            pcoll
            | "AddWindowTS" >> beam.ParDo(AddWindowTS())
            | "CreateMessages"
            >> beam.Map(create_message).with_output_types(typing.Tuple[bytes, bytes])
        )


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--bootstrap_servers",
        default="host.docker.internal:29092",
        help="Kafka bootstrap server addresses",
    )
    parser.add_argument("--input_topic", default="input-topic", help="Input topic")
    parser.add_argument(
        "--output_topic",
        default=re.sub("_", "-", re.sub(".py$", "", os.path.basename(__file__))),
        help="Output topic",
    )
    parser.add_argument("--window_length", default="10", type=int, help="Window length")
    parser.add_argument("--top_k", default="3", type=int, help="Top k")
    parser.add_argument(
        "--deprecated_read",
        action="store_true",
        default="Whether to use a deprecated read. See https://github.com/apache/beam/issues/20979",
    )
    parser.set_defaults(deprecated_read=False)

    known_args, pipeline_args = parser.parse_known_args(argv)

    # # We use the save_main_session option because one or more DoFn's in this
    # # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    print(f"known args - {known_args}")
    print(f"pipeline options - {pipeline_options.display_data()}")

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadInputsFromKafka"
            >> ReadWordsFromKafka(
                bootstrap_servers=known_args.bootstrap_servers,
                topics=[known_args.input_topic],
                group_id=f"{known_args.output_topic}-group",
                deprecated_read=known_args.deprecated_read,
            )
            | "CalculateTopKWords"
            >> CalculateTopKWords(
                window_length=known_args.window_length,
                top_k=known_args.top_k,
            )
            | "CreateMessags" >> CreateMessags()
            | "WriteOutputsToKafka"
            >> WriteProcessOutputsToKafka(
                bootstrap_servers=known_args.bootstrap_servers,
                topic=known_args.output_topic,
                deprecated_read=known_args.deprecated_read,
            )
        )

        logging.getLogger().setLevel(logging.WARN)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
