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
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from sport_tracker_utils import Position
from sport_tracker_motivation_co_gbk import SportTrackerMotivation, MeanPaceCombineFn


def move(start: Position, duration: float, delta: int):
    spot, timestamp = tuple(start)
    return Position(spot=spot + delta, timestamp=timestamp + duration)


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


class SportTrackerMotivationTest(unittest.TestCase):
    def test_windowing_behaviour(self):
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            # current_ms = int(time.time() * 1000)
            current_ms = 0
            init_position = Position.create(spot=0, timestamp=current_ms)
            user0s = [
                ("user0", init_position),
                ("user0", move(init_position, duration=180, delta=10)),
                ("user0", move(init_position, duration=180, delta=20)),
            ]
            user1s = [
                ("user1", init_position),
                ("user1", move(init_position, duration=240, delta=-10)),
                ("user1", move(init_position, duration=120, delta=-20)),
            ]
            inputs = chain(*zip(user0s, user1s))

            test_stream = TestStream()
            for input in inputs:
                test_stream.add_elements(
                    [TimestampedValue(input, Timestamp.of(input[1].timestamp))]
                )
            test_stream.advance_watermark_to_infinity()

            output = (
                p
                | test_stream.with_output_types(typing.Tuple[str, Position])
                # | SportTrackerMotivation(short_duration=60, long_duration=300)
                # | beam.CombinePerKey(MeanPaceCombineFn())
                | beam.Map(print)
            )

    # def test_windowing_behaviour(self):
    #     options = PipelineOptions()
    #     options.view_as(StandardOptions).streaming = True
    #     with TestPipeline(options=options) as p:
    #         current_ms = int(time.time() * 1000)
    #         inputs = [
    #             ("user0", random_position(timestamp_ms=current_ms)),
    #             ("user1", random_position(timestamp_ms=current_ms)),
    #         ]
    #         inputs.append(("user0", move(inputs[0][1], (1.0, 1.0), 3, 1000)))
    #         inputs.append(("user1", move(inputs[1][1], (1.0, 1.0), 2, 1000)))
    #         inputs.append(("user0", move(inputs[2][1], (1.0, 1.0), 2.5, 3000)))
    #         inputs.append(("user1", move(inputs[3][1], (1.0, 1.0), 2.5, 3000)))

    #         watermarks = [
    #             current_ms / 1000 + 1000,
    #             current_ms / 1000 + 1100,
    #             current_ms / 1000 + 1200,
    #             current_ms / 1000 + 1300,
    #         ]

    #         test_stream = TestStream()
    #         test_stream.advance_watermark_to(Timestamp.of(current_ms / 1000))
    #         for input in inputs:
    #             test_stream.add_elements(
    #                 [
    #                     TimestampedValue(
    #                         input, Timestamp.of(input[1].timestamp_ms / 1000)
    #                     )
    #                 ]
    #             )
    #             if len(watermarks) > 0:
    #                 test_stream.advance_watermark_to(Timestamp.of(watermarks.pop(0)))
    #         test_stream.advance_watermark_to_infinity()

    #         output = (
    #             p
    #             | test_stream.with_output_types(typing.Tuple[str, Position])
    #             | SportTrackerMotivation(short_duration=60, long_duration=300)
    #             | beam.Map(print)
    #         )

    #         # EXPECTED_OUTPUT = compute_expected_metrics(lines)

    #         # assert_that(output, equal_to(EXPECTED_OUTPUT))


if __name__ == "__main__":
    main(out=None)
