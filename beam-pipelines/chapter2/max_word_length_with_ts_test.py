import unittest

from apache_beam.coders import coders
from apache_beam.utils.timestamp import Timestamp
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, TestWindowedValue
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import GlobalWindow, TimestampedValue
from apache_beam.transforms.util import Reify
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from max_word_length_with_ts import CalculateMaxWordLength


class MaxWordLengthTest(unittest.TestCase):
    def test_windowing_behaviour(self):
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            """
               We should put each element separately. The reason we do this is to ensure 
               that our trigger will be invoked in between each element. Because the trigger 
               invocation is optional, a runner might skip a particular firing. Putting each element 
               separately into addElements makes DirectRunner (our testing runner) invokes a trigger 
               for each input element.
            """
            test_stream = (
                TestStream(coder=coders.StrUtf8Coder())
                .with_output_types(str)
                .add_elements([TimestampedValue("a", Timestamp.of(0))])
                .add_elements([TimestampedValue("bb", Timestamp.of(10))])
                .add_elements([TimestampedValue("ccc", Timestamp.of(20))])
                .add_elements([TimestampedValue("d", Timestamp.of(30))])
                .advance_watermark_to_infinity()
            )

            output = (
                p
                | test_stream
                | "CalculateMaxWordLength" >> CalculateMaxWordLength()
                | "ReifyTimestamp" >> Reify.Timestamp()
            )

            EXPECTED_OUTPUT = [
                TestWindowedValue(
                    value="a", timestamp=Timestamp(0), windows=[GlobalWindow()]
                ),
                TestWindowedValue(
                    value="bb", timestamp=Timestamp(10), windows=[GlobalWindow()]
                ),
                TestWindowedValue(
                    value="ccc", timestamp=Timestamp(20), windows=[GlobalWindow()]
                ),
                TestWindowedValue(
                    value="ccc", timestamp=Timestamp(30), windows=[GlobalWindow()]
                ),
                TestWindowedValue(
                    value="ccc", timestamp=Timestamp(30), windows=[GlobalWindow()]
                ),
            ]

            assert_that(output, equal_to(EXPECTED_OUTPUT), reify_windows=True)


if __name__ == "__main__":
    unittest.main()
