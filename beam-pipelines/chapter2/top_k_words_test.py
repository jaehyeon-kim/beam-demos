import unittest

from apache_beam.coders import coders
from apache_beam.utils.timestamp import Timestamp
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from top_k_words import CalculateTopKWords


class TopKWordsTest(unittest.TestCase):
    def test_windowing_behaviour(self):
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            test_stream = (
                TestStream(coder=coders.StrUtf8Coder())
                .with_output_types(str)
                .add_elements(
                    [
                        TimestampedValue("This is the first line.", Timestamp.of(0)),
                        TimestampedValue(
                            "This is second line in the first window", Timestamp.of(1)
                        ),
                        TimestampedValue(
                            "Last line in the first window", Timestamp.of(2)
                        ),
                        TimestampedValue(
                            "This is another line, but in different window.",
                            Timestamp.of(10),
                        ),
                        TimestampedValue(
                            "Last line, in the same window as previous line.",
                            Timestamp.of(11),
                        ),
                    ]
                )
                .advance_watermark_to_infinity()
            )

            output = (
                p
                | test_stream
                | "CalculateTopKWords"
                >> CalculateTopKWords(
                    window_length=10,
                    top_k=3,
                )
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
    unittest.main()
