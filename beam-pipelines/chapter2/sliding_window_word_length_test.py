import sys
import datetime
import unittest

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import SlidingWindows, TimestampedValue
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from sliding_window_word_length import tokenize, AverageFn, AddWindowTS


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


def create_dt(index: int):
    return datetime.datetime.fromtimestamp(index, tz=datetime.timezone.utc).replace(
        tzinfo=None
    )


class SlidingWindowWordLengthTest(unittest.TestCase):
    def test_windowing_behaviour(self):
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            now = 8  # window start becomes datetime.datetime(1970, 1, 1, 0, 0) if 8, not 0??
            test_stream = (
                TestStream(coder=coders.StrUtf8Coder())
                .with_output_types(str)
                .add_elements([TimestampedValue("a", now + 0)])
                .add_elements([TimestampedValue("bb", now + 1.99)])
                .add_elements([TimestampedValue("ccc", now + 5)])
                .add_elements([TimestampedValue("dddd", now + 10)])
                .advance_watermark_to_infinity()
            )

            output = (
                p
                | test_stream
                | "Windowing" >> beam.WindowInto(SlidingWindows(size=10, period=2))
                | "Extract words" >> beam.FlatMap(tokenize)
                | "Get avg word length"
                >> beam.CombineGlobally(AverageFn()).without_defaults()
                | "Add window timestamp" >> beam.ParDo(AddWindowTS())
            )

            EXPECTED_VALUES = [1.5, 1.5, 2.0, 2.0, 2.0, 3.5, 3.5, 4.0, 4.0, 4.0]
            EXPECTED_OUTPUT = [
                (
                    create_dt(i * 2),
                    create_dt(i * 2) + datetime.timedelta(seconds=10),
                    EXPECTED_VALUES[i],
                )
                for i in range(len(EXPECTED_VALUES))
            ]

            assert_that(output, equal_to(EXPECTED_OUTPUT))


if __name__ == "__main__":
    main(out=None)
