import datetime
import unittest

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.utils.timestamp import Timestamp
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from sliding_window_word_length import CalculateAverageWordLength, AddWindowTS


def create_dt(index: int):
    return datetime.datetime.fromtimestamp(index - 8, tz=datetime.timezone.utc).replace(
        tzinfo=None
    )


def to_rfc3339(dt: datetime.datetime):
    return f'{dt.isoformat(sep="T", timespec="seconds")}Z'


class SlidingWindowWordLengthTest(unittest.TestCase):
    def test_windowing_behaviour(self):
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            now = 0
            test_stream = (
                TestStream(coder=coders.StrUtf8Coder())
                .with_output_types(str)
                .add_elements([TimestampedValue("a", Timestamp.of(now + 0))])
                .add_elements([TimestampedValue("bb", Timestamp.of(now + 1.99))])
                .add_elements([TimestampedValue("ccc", Timestamp.of(now + 5))])
                .add_elements([TimestampedValue("dddd", Timestamp.of(now + 10))])
                .advance_watermark_to_infinity()
            )

            output = (
                p
                | test_stream
                | "CalculateAverageWordLength"
                >> CalculateAverageWordLength(size=10, period=2)
                | "AddWindowTS" >> beam.ParDo(AddWindowTS())
            )

            EXPECTED_VALUES = [1.5, 1.5, 2.0, 2.0, 2.0, 3.5, 3.5, 4.0, 4.0, 4.0]
            EXPECTED_OUTPUT = [
                (
                    to_rfc3339(create_dt(i * 2)),
                    to_rfc3339(create_dt(i * 2) + datetime.timedelta(seconds=10)),
                    EXPECTED_VALUES[i],
                )
                for i in range(len(EXPECTED_VALUES))
            ]

            assert_that(output, equal_to(EXPECTED_OUTPUT))


if __name__ == "__main__":
    unittest.main()
