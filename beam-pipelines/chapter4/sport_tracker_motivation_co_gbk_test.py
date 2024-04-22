import sys
import math
import time
import typing
import random
import unittest
from itertools import chain

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.utils.timestamp import Timestamp
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from sport_tracker_utils import Position
from sport_tracker_motivation_co_gbk import SportTrackerMotivation


def move(start: Position, delta: int, duration: float):
    spot, timestamp = tuple(start)
    return Position(spot=spot + delta, timestamp=timestamp + duration)


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


class SportTrackerMotivationTest(unittest.TestCase):
    def test_pipeline_bounded(self):
        options = PipelineOptions()
        with TestPipeline(options=options) as p:
            # now = time.time()
            now = 0
            user0s = [
                ("user0", Position.create(spot=0, timestamp=now + 30)),
                ("user0", Position.create(spot=25, timestamp=now + 60)),
                ("user0", Position.create(spot=22, timestamp=now + 75)),
            ]
            user1s = [
                ("user1", Position.create(spot=0, timestamp=now + 30)),
                ("user1", Position.create(spot=-20, timestamp=now + 60)),
                ("user1", Position.create(spot=80, timestamp=now + 75)),
            ]
            inputs = chain(*zip(user0s, user1s))

            test_stream = TestStream()
            for input in inputs:
                test_stream.add_elements([input], event_timestamp=input[1].timestamp)
            test_stream.advance_watermark_to_infinity()

            output = (
                p
                | test_stream.with_output_types(typing.Tuple[str, Position])
                | SportTrackerMotivation(short_duration=20, long_duration=100)
            )

            EXPECTED_OUTPUT = [
                ("user0", "pacing"),
                ("user1", "pacing"),
                ("user0", "underperforming"),
                ("user1", "outperforming"),
            ]

            assert_that(output, equal_to(EXPECTED_OUTPUT))

    def test_pipeline_unbounded(self):
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            # now = time.time()
            now = 0
            user0s = [
                ("user0", Position.create(spot=0, timestamp=now + 30)),
                ("user0", Position.create(spot=25, timestamp=now + 60)),
                ("user0", Position.create(spot=22, timestamp=now + 75)),
            ]
            user1s = [
                ("user1", Position.create(spot=0, timestamp=now + 30)),
                ("user1", Position.create(spot=-20, timestamp=now + 60)),
                ("user1", Position.create(spot=80, timestamp=now + 75)),
            ]
            inputs = chain(*zip(user0s, user1s))
            watermarks = [now + 5, now + 10, now + 15, now + 20, now + 29, now + 30]

            test_stream = TestStream()
            test_stream.advance_watermark_to(Timestamp.of(now))
            for input in inputs:
                test_stream.add_elements([input], event_timestamp=input[1].timestamp)
                if watermarks:
                    test_stream.advance_watermark_to(Timestamp.of(watermarks.pop(0)))
            test_stream.advance_watermark_to_infinity()

            output = (
                p
                | test_stream.with_output_types(typing.Tuple[str, Position])
                | SportTrackerMotivation(short_duration=30, long_duration=90)
            )

            EXPECTED_OUTPUT = [
                ("user0", "pacing"),
                ("user1", "pacing"),
                ("user0", "underperforming"),
                ("user1", "outperforming"),
            ]

            assert_that(output, equal_to(EXPECTED_OUTPUT))


if __name__ == "__main__":
    main(out=None)
