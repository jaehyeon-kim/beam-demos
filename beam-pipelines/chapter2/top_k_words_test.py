import sys
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from top_k_words import tokenize


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


class TopKWordsText(unittest.TestCase):
    def test_windowing_behaviour(self):
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            test_stream = (
                TestStream()
                .advance_watermark_to(0)
                .add_elements(
                    [
                        TimestampedValue("This is the first line.", 0),
                        TimestampedValue("This is second line in the first window", 1),
                        TimestampedValue("Last line in the first window", 2),
                        TimestampedValue(
                            "This is another line, but in different window.", 10
                        ),
                        TimestampedValue(
                            "Last line, in the same window as previous line.", 11
                        ),
                    ]
                )
                .advance_watermark_to_infinity()
            )

            output = (
                p
                | test_stream
                | "Windowing" >> beam.WindowInto(beam.window.FixedWindows(10))
                | "Extract words" >> beam.FlatMap(tokenize)
                | "Count per word" >> beam.combiners.Count.PerElement()
                | "Top k words"
                >> beam.combiners.Top.Of(3, lambda e: e[1]).without_defaults()
                | "Flatten" >> beam.FlatMap(lambda e: e)
            )

            EXPECTED_OUTPUT = [
                ("line", 3),
                ("first", 3),
                ("the", 3),
                ("line", 3),
                ("in", 2),
                ("window", 2),
            ]

            assert_that(output, equal_to(EXPECTED_OUTPUT))


if __name__ == "__main__":
    main(out=None)
