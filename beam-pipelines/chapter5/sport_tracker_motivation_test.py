import re
import sys
import typing
import unittest

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.utils.timestamp import Timestamp
from apache_beam.options.pipeline_options import PipelineOptions

from sport_tracker_utils import Position, PreProcessInput
from sport_tracker_motivation import SportTrackerMotivation


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


class SportTrackerMotivationTest(unittest.TestCase):
    def test_pipeline_bounded(self):
        pipeline_opts = {"runner": "FlinkRunner", "parallelism": 1, "streaming": True}
        options = PipelineOptions([], **pipeline_opts)
        with TestPipeline(options=options) as p:
            lines = [
                "user0\t0\t30",
                "user1\t0\t30",
                "user0\t25\t60",
                "user1\t20\t60",
                "user0\t22\t75",
                "user1\t80\t75",
            ]

            test_stream = TestStream(coder=coders.StrUtf8Coder()).with_output_types(str)
            for line in lines:
                _, _, timestamp = tuple(re.sub("\n", "", line).split("\t"))
                test_stream.add_elements([line], event_timestamp=float(timestamp))
            test_stream.advance_watermark_to_infinity()

            output = (
                p
                | test_stream
                | "PreProcessInput"
                >> PreProcessInput().with_output_types(typing.Tuple[str, Position])
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
        pipeline_opts = {"runner": "FlinkRunner", "parallelism": 1, "streaming": True}
        options = PipelineOptions([], **pipeline_opts)
        with TestPipeline(options=options) as p:
            lines = [
                "user0\t0\t30",
                "user1\t0\t30",
                "user0\t25\t60",
                "user1\t20\t60",
                "user0\t22\t75",
                "user1\t80\t75",
            ]
            now = 0
            watermarks = [now + 5, now + 10, now + 15, now + 20, now + 29, now + 30]

            test_stream = TestStream(coder=coders.StrUtf8Coder()).with_output_types(str)
            test_stream.advance_watermark_to(Timestamp.of(now))
            for line in lines:
                _, _, timestamp = tuple(re.sub("\n", "", line).split("\t"))
                test_stream.add_elements([line], event_timestamp=float(timestamp))
                if watermarks:
                    test_stream.advance_watermark_to(Timestamp.of(watermarks.pop(0)))
            test_stream.advance_watermark_to_infinity()

            output = (
                p
                | test_stream
                | "PreProcessInput"
                >> PreProcessInput().with_output_types(typing.Tuple[str, Position])
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
