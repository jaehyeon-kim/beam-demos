import os
import datetime
import argparse
import logging
import re
import typing

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.transforms.trigger import (
    AccumulationMode,
    AfterProcessingTime,
    Repeatedly,
)
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import (
    GlobalWindows,
    TimestampedValue,
    TimestampCombiner,
    Duration,
)
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions


class Position(typing.NamedTuple):
    latitude: float
    longitude: float
    timestamp: int


class Metric(typing.NamedTuple):
    length: float
    duration: int


beam.coders.registry.register_coder(Position, beam.coders.RowCoder)
beam.coders.registry.register_coder(Metric, beam.coders.RowCoder)


def read_file(filename: str, inputpath: str):
    with open(os.path.join(inputpath, filename), "r") as f:
        return f.readlines()


def to_positions(input: str):
    workout, latitude, longitude, timestamp = tuple(re.sub("\n", "", input).split("\t"))
    return workout, Position(
        latitude=float(latitude), longitude=float(longitude), timestamp=int(timestamp)
    )


class AddTimestampDoFn(beam.DoFn):
    def process(self, element: typing.Tuple[str, Position]):
        yield TimestampedValue(element, element[1].timestamp)


def run():
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--inputs",
        default="inputs",
        help="Specify folder name that event records are saved",
    )
    parser.add_argument(
        "--runner", default="DirectRunner", help="Specify Apache Beam Runner"
    )
    opts = parser.parse_args()
    PARENT_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    options = PipelineOptions()
    options.view_as(StandardOptions).runner = opts.runner

    records = read_file("test-tracker-data-200.txt", os.path.join(PARENT_DIR, "inputs"))
    test_stream = (
        TestStream(coder=coders.StrUtf8Coder())
        .with_output_types(str)
        .add_elements(records)
        .advance_watermark_to_infinity()
    )

    p = beam.Pipeline(options=options)
    (
        p
        | "Read stream" >> test_stream
        | "To positions" >> beam.Map(to_positions)
        | "Add timestamp" >> beam.ParDo(AddTimestampDoFn())
        | "Windowing"
        >> beam.WindowInto(
            GlobalWindows(),
            trigger=Repeatedly(AfterProcessingTime(10)),
            timestamp_combiner=TimestampCombiner.OUTPUT_AT_LATEST,
            accumulation_mode=AccumulationMode.ACCUMULATING,
            allowed_lateness=Duration.of(seconds=60 * 60),
        )
        | "Group by workout" >> beam.GroupByKey()
        # | beam.Map(print)
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
