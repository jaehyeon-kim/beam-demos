import os
import argparse
import json
import re
import logging
import typing

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms.window import GlobalWindows, TimestampCombiner
from apache_beam.transforms.trigger import AfterCount, AccumulationMode, AfterWatermark
from apache_beam.transforms.util import Reify
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


from word_process_utils import tokenize, ReadWordsFromKafka, WriteProcessOutputsToKafka


class CalculateMaxWordLength(beam.PTransform):
    def expand(self, pcoll: pvalue.PCollection):
        return (
            pcoll
            | "Windowing"
            >> beam.WindowInto(
                GlobalWindows(),
                trigger=AfterWatermark(early=AfterCount(1)),
                allowed_lateness=0,
                timestamp_combiner=TimestampCombiner.OUTPUT_AT_LATEST,
                accumulation_mode=AccumulationMode.ACCUMULATING,
            )
            | "Tokenize" >> beam.FlatMap(tokenize)
            | "GetLongestWord" >> beam.combiners.Top.Of(1, key=len).without_defaults()
            | "Flatten" >> beam.FlatMap(lambda e: e)
        )


def create_message(element: typing.Tuple[str, str]):
    msg = json.dumps(dict(zip(["created_at", "word"], element)))
    print(msg)
    return "".encode("utf-8"), msg.encode("utf-8")


class AddTS(beam.DoFn):
    def process(self, word: str, ts_param=beam.DoFn.TimestampParam):
        yield ts_param.to_rfc3339(), word


class CreateMessags(beam.PTransform):
    def expand(self, pcoll: pvalue.PCollection):
        return (
            pcoll
            | "ReifyTimestamp" >> Reify.Timestamp()
            | "AddTimestamp" >> beam.ParDo(AddTS())
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

    known_args, pipeline_args = parser.parse_known_args(argv)
    print(f"known args - {known_args}")
    print(f"pipeline args - {pipeline_args}")

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadInputsFromKafka"
            >> ReadWordsFromKafka(
                bootstrap_servers=known_args.bootstrap_servers,
                topics=[known_args.input_topic],
                group_id=f"{known_args.output_topic}-group",
            )
            | "CalculateMaxWordLength" >> CalculateMaxWordLength()
            | "CreateMessags" >> CreateMessags()
            | "WriteOutputsToKafka"
            >> WriteProcessOutputsToKafka(
                bootstrap_servers=known_args.bootstrap_servers,
                topic=known_args.output_topic,
            )
        )

        logging.getLogger().setLevel(logging.WARN)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
