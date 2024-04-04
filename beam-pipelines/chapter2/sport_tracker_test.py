import sys
import os
import time
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

from sport_tracker import (
    Position,
    to_positions,
    assign_timestamp,
    get_distance,
    compute_matrics,
)


def read_file(filename: str, inputpath: str):
    with open(os.path.join(inputpath, filename), "r") as f:
        return f.readlines()


def compute_expected_metrics(lines: list):
    ones, twos = [], []
    for line in lines:
        position = to_positions(line)
        if position[0] == "track1":
            ones.append(position[1])
        else:
            twos.append(position[1])
    return [
        compute_matrics(key="track1", positions=ones),
        compute_matrics(key="track2", positions=twos),
    ]


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


class SportTrackerTest(unittest.TestCase):
    def test_gps_delta_length_calculation(self):
        now = int(time.time() * 1000)
        p1 = Position(
            latitude=64.53165930148285, longitude=-17.3172239914558, timestamp=now
        )
        p2 = Position(
            latitude=64.53168645625584, longitude=-17.316554613536688, timestamp=now
        )
        distance = get_distance(p1, p2)
        self.assertTrue(distance < 80, f"Expected less then 80 nmeters, got {distance}")
        self.assertTrue(distance > 30, f"Expected more then 80 nmeters, got {distance}")
        self.assertAlmostEqual(distance, get_distance(p2, p1), places=0.001)
        self.assertAlmostEqual(0.0, get_distance(p1, p1), places=0.001)
        self.assertAlmostEqual(0.0, get_distance(p2, p2), places=0.001)

        p3 = Position(latitude=0, longitude=0, timestamp=now)
        p4 = Position(latitude=0, longitude=0.1, timestamp=now)
        self.assertAlmostEqual(11119, get_distance(p3, p4), delta=1)
        self.assertAlmostEqual(11119, get_distance(p4, p3), delta=1)

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
                | "To positions" >> beam.Map(to_positions)
                | "With timestamps" >> beam.Map(assign_timestamp)
                | "Windowing"
                >> beam.WindowInto(
                    GlobalWindows(),
                    trigger=AfterWatermark(early=AfterProcessingTime(3)),
                    allowed_lateness=0,
                    timestamp_combiner=TimestampCombiner.OUTPUT_AT_LATEST,
                    accumulation_mode=AccumulationMode.ACCUMULATING,
                )
                | "Group by workout" >> beam.GroupByKey()
                | "Compute metrics" >> beam.Map(lambda e: compute_matrics(*e))
            )

            EXPECTED_OUTPUT = compute_expected_metrics(lines)

            assert_that(output, equal_to(EXPECTED_OUTPUT))


if __name__ == "__main__":
    main(out=None)
