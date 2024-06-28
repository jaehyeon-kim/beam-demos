import os
import json
import argparse
import re
import logging
import typing

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms.window import GlobalWindows, TimestampCombiner
from apache_beam.transforms.trigger import AfterCount, AccumulationMode, Repeatedly
from apache_beam.transforms.util import Reify
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from word_process_utils import tokenize, ReadWordsFromKafka, WriteProcessOutputsToKafka


class AvgAccum(typing.NamedTuple):
    length: int
    count: int


beam.coders.registry.register_coder(AvgAccum, beam.coders.RowCoder)


class AverageFn(beam.CombineFn):
    def create_accumulator(self):
        return AvgAccum(length=0, count=0)

    def add_input(self, mutable_accumulator: AvgAccum, element: str):
        length, count = tuple(mutable_accumulator)
        return AvgAccum(length=length + len(element), count=count + 1)

    def merge_accumulators(self, accumulators: typing.List[AvgAccum]):
        lengths, counts = zip(*accumulators)
        return AvgAccum(length=sum(lengths), count=sum(counts))

    def extract_output(self, accumulator: AvgAccum):
        length, count = tuple(accumulator)
        return length / count if count else float("NaN")

    def get_accumulator_coder(self):
        return beam.coders.registry.get_coder(AvgAccum)


class CalculateAverageWordLength(beam.PTransform):
    def expand(self, pcoll: pvalue.PCollection):
        return (
            pcoll
            | "Windowing"
            >> beam.WindowInto(
                GlobalWindows(),
                trigger=Repeatedly(AfterCount(1)),
                allowed_lateness=0,
                timestamp_combiner=TimestampCombiner.OUTPUT_AT_LATEST,
                accumulation_mode=AccumulationMode.ACCUMULATING,
                # closing behaviour - EMIT_ALWAYS, on_time_behavior - FIRE_ALWAYS
            )
            | "Tokenize" >> beam.FlatMap(tokenize)
            | "GetAvgWordLength"
            >> beam.CombineGlobally(
                AverageFn()
            ).without_defaults()  # DAG gets complicated if with_default()
        )


def create_message(element: typing.Tuple[str, float]):
    msg = json.dumps(dict(zip(["created_at", "avg_len"], element)))
    print(msg)
    return "".encode("utf-8"), msg.encode("utf-8")


class AddTS(beam.DoFn):
    def process(self, avg_len: float, ts_param=beam.DoFn.TimestampParam):
        yield ts_param.to_rfc3339(), avg_len


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
            | "CalculateAverageWordLength" >> CalculateAverageWordLength()
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
