import os
import datetime
import argparse
import logging
import re

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.transforms import window
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions


def read_file(filename: str, inputpath: str):
    with open(os.path.join(inputpath, filename), "r") as f:
        lines = f.readlines()
        return [(i, lines[i]) for i in range(len(lines))]


def tokenize(element: str):
    return re.findall(r"[A-Za-z\']+", element)


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

    lines = read_file("lorem.txt", os.path.join(PARENT_DIR, "inputs"))
    now = int(datetime.datetime.now().timestamp() * 1000)
    test_stream = (
        TestStream(coder=coders.StrUtf8Coder())
        .with_output_types(str)
        .add_elements(
            [
                TimestampedValue(lines[i][1], now if lines[i][0] == 0 else now + 1000)
                for i in range(len(lines))
            ]
        )
        .advance_watermark_to_infinity()
    )

    p = beam.Pipeline(options=options)
    (
        p
        | "Read stream" >> test_stream
        | "Windowing"
        >> beam.WindowInto(
            window.FixedWindows(1),
            accumulation_mode=AccumulationMode.DISCARDING,
        )
        | "Extract words" >> beam.FlatMap(tokenize)
        | "Conver to tuple" >> beam.Map(lambda e: ("w", 1))
        | "Count words" >> beam.CombinePerKey(sum)
        | beam.Map(print)
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
