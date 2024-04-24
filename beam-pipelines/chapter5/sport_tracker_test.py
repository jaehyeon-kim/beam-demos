import sys
import unittest

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampCombiner, FixedWindows
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.options.pipeline_options import PipelineOptions

from sport_tracker_utils import PreProcessInput
from sport_tracker import ComputeMetrics


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


class SportTrackerTest(unittest.TestCase):
    def test_windowing_behaviour(self):
        pipeline_opts = {"runner": "FlinkRunner", "parallelism": 1, "streaming": True}
        options = PipelineOptions([], **pipeline_opts)
        with TestPipeline(options=options) as p:
            lines = [
                "user0	0	0",
                "user1	10	2",
                "user0	5	4",
                "user1	3	3",
                "user0	10	6",
                "user1	2	7",
                "user0	4	9",
                "user1	10	9",
            ]
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
                    FixedWindows(5),
                    allowed_lateness=0,
                    timestamp_combiner=TimestampCombiner.OUTPUT_AT_LATEST,
                    accumulation_mode=AccumulationMode.ACCUMULATING,
                )
                | "ComputeMetrics" >> ComputeMetrics()
            )

            assert_that(
                output,
                equal_to(
                    [("user0", 1.25), ("user1", 7.0), ("user0", 2.0), ("user1", 4.0)]
                ),
            )


if __name__ == "__main__":
    main(out=None)
