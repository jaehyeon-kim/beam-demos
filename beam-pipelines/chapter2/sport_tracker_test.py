import sys
import os
import re
import typing
import unittest

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.trigger import (
    AfterWatermark,
    AccumulationMode,
    AfterProcessingTime,
)
from apache_beam.transforms.window import GlobalWindows, TimestampCombiner
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from sport_tracker_utils import Position, PreProcessInput
from sport_tracker import ComputeMetrics


def read_file(filename: str, inputpath: str):
    with open(os.path.join(inputpath, filename), "r") as f:
        return f.readlines()


def compute_matrics(key: str, positions: typing.List[Position]):
    last: Position = None
    distance = 0
    duration = 0
    for p in sorted(positions, key=lambda p: p.timestamp):
        if last is not None:
            distance += abs(int(p.spot) - int(last.spot))
            duration += float(p.timestamp) - float(last.timestamp)
        last = p
    return key, distance / duration if duration > 0 else 0


def compute_expected_metrics(lines: list):
    ones, twos = [], []
    for line in lines:
        workout, spot, timestamp = tuple(re.sub("\n", "", line).split("\t"))
        position = Position(spot, timestamp)
        if workout == "user0":
            ones.append(position)
        else:
            twos.append(position)
    return [
        compute_matrics(key="user0", positions=ones),
        compute_matrics(key="user1", positions=twos),
    ]


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


class SportTrackerTest(unittest.TestCase):
    def test_windowing_behaviour(self):
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            PARENT_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
            lines = read_file(
                "test-tracker-data.txt", os.path.join(PARENT_DIR, "inputs")
            )
            test_stream = TestStream(coder=coders.StrUtf8Coder()).with_output_types(str)
            for line in lines:
                test_stream.add_elements([line])
            test_stream.advance_watermark_to_infinity()

            output = (
                p
                | test_stream
                | "PreProcessInput" >> PreProcessInput()
                | "Windowing"
                >> beam.WindowInto(
                    GlobalWindows(),
                    trigger=AfterWatermark(early=AfterProcessingTime(3)),
                    allowed_lateness=0,
                    timestamp_combiner=TimestampCombiner.OUTPUT_AT_LATEST,
                    accumulation_mode=AccumulationMode.ACCUMULATING,
                )
                | "ComputeMetrics" >> ComputeMetrics()
            )

            EXPECTED_OUTPUT = compute_expected_metrics(lines)

            assert_that(output, equal_to(EXPECTED_OUTPUT))


if __name__ == "__main__":
    main(out=None)
