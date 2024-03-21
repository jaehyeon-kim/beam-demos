import sys
import unittest

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.trigger import AfterCount, AccumulationMode, AfterWatermark
from apache_beam.transforms.window import GlobalWindows, TimestampedValue
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from top_k_words import tokenize


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


class MaxWordLengthTest(unittest.TestCase):
    def test_windowing_behaviour(self):
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            test_stream = (
                TestStream(coder=coders.StrUtf8Coder())
                .with_output_types(str)
                .add_elements([TimestampedValue("a", 0)])
                .add_elements([TimestampedValue("bb", 10)])
                .add_elements([TimestampedValue("ccc", 20)])
                .add_elements([TimestampedValue("d", 30)])
                .advance_watermark_to_infinity()
            )

            output = (
                p
                | test_stream
                | "Windowing"
                >> beam.WindowInto(
                    GlobalWindows(),
                    trigger=AfterWatermark(early=AfterCount(1)),
                    allowed_lateness=0,
                    accumulation_mode=AccumulationMode.ACCUMULATING,
                )
                | "Extract words" >> beam.FlatMap(tokenize)
                | "Get longest word"
                >> beam.combiners.Top.Of(1, key=len).without_defaults()
                | "Flatten" >> beam.FlatMap(lambda e: e)
            )

            EXPECTED_OUTPUT = ["a", "bb", "ccc", "ccc", "ccc"]

            assert_that(output, equal_to(EXPECTED_OUTPUT))


if __name__ == "__main__":
    main(out=None)
