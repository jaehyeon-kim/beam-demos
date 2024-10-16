import unittest

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.transforms.window import IntervalWindow
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, equal_to_per_window
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.utils.timestamp import Timestamp

from io_utils import tokenize
from droppable_data_filter import (
    SplitDroppable,
    MAIN_OUTPUT,
    DROPPABLE_OUTPUT,
)


class DroppableDataFilterTest(unittest.TestCase):
    def test_windowing_behaviour(self):
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        now = 0
        # now = int(time.time())
        with TestPipeline(options=options) as p:
            test_stream = (
                TestStream(coder=coders.StrUtf8Coder())
                .with_output_types(str)
                .advance_watermark_to(Timestamp(seconds=now))
                .add_elements(
                    [TimestampedValue("a", Timestamp(seconds=now + 3))]
                )  # fine, before watermark - on time
                .advance_watermark_to(Timestamp(seconds=now + 6.999))
                .add_elements(
                    [TimestampedValue("b", Timestamp(seconds=now + 4))]
                )  # late, but within allowed lateness
                .advance_watermark_to(Timestamp(seconds=now + 7))
                .add_elements([TimestampedValue("c", now)])  # droppable
                .advance_watermark_to_infinity()
            )

            outputs = (
                p
                | test_stream
                | "ExtractWords" >> beam.FlatMap(tokenize)
                | "Windowing"
                >> beam.WindowInto(
                    FixedWindows(5),
                    allowed_lateness=2,
                    accumulation_mode=AccumulationMode.DISCARDING,
                )
                | "SpiltDroppable" >> SplitDroppable()
            )

            main_expected = {
                IntervalWindow(Timestamp(seconds=now), Timestamp(seconds=now + 5)): [
                    "a",
                    "b",
                ],
            }

            assert_that(
                outputs[MAIN_OUTPUT],
                equal_to_per_window(main_expected),
                reify_windows=True,
                label="assert_main",
            )

            assert_that(
                outputs[DROPPABLE_OUTPUT], equal_to(["c"]), label="assert_droppable"
            )


class DroppableDataFilterTestFail(unittest.TestCase):
    @unittest.expectedFailure
    def test_windowing_behaviour(self):
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        now = 0
        # now = int(time.time())
        with TestPipeline(options=options) as p:
            test_stream = (
                TestStream(coder=coders.StrUtf8Coder())
                .with_output_types(str)
                .advance_watermark_to(Timestamp(seconds=now + 7.5))
                .add_elements(
                    [TimestampedValue("c", now)]
                )  # should be dropped but not!
                .advance_watermark_to_infinity()
            )

            outputs = (
                p
                | test_stream
                | "Extract words" >> beam.FlatMap(tokenize)
                | "Windowing"
                >> beam.WindowInto(
                    FixedWindows(5),
                    allowed_lateness=2,
                    accumulation_mode=AccumulationMode.DISCARDING,
                )
                | "SpiltDroppable" >> SplitDroppable()
            )

            assert_that(
                outputs[DROPPABLE_OUTPUT], equal_to(["c"]), label="assert_droppable"
            )


if __name__ == "__main__":
    unittest.main()
