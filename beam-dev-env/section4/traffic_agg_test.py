import datetime
import sys
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from traffic_agg import EventLog, parse_json, assign_timestamp


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


class TrafficWindowingTest(unittest.TestCase):
    def get_ts(self, event: dict):
        return datetime.datetime.strptime(
            parse_json(event).event_datetime, "%Y-%m-%dT%H:%M:%S.%f"
        ).timestamp()

    def test_windowing_behaviour(self):
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            EVENTS = [
                '{"ip": "138.201.212.70", "id": "462520009613048791", "lat": 50.4779, "lng": 12.3713, "user_agent": "Mozilla/5.0 (iPod; U; CPU iPhone OS 3_1 like Mac OS X; ks-IN) AppleWebKit/532.30.7 (KHTML, like Gecko) Version/3.0.5 Mobile/8B115 Safari/6532.30.7", "age_bracket": "18-25", "opted_into_marketing": false, "http_request": "GET eucharya.html HTTP/1.0", "http_response": 200, "file_size_bytes": 207, "event_datetime": "2024-03-01T05:51:22.083", "event_ts": 1709232682083}',
                '{"ip": "138.201.212.70", "id": "462520009613048791", "lat": 50.4779, "lng": 12.3713, "user_agent": "Mozilla/5.0 (iPod; U; CPU iPhone OS 3_1 like Mac OS X; ks-IN) AppleWebKit/532.30.7 (KHTML, like Gecko) Version/3.0.5 Mobile/8B115 Safari/6532.30.7", "age_bracket": "18-25", "opted_into_marketing": false, "http_request": "GET eucharya.html HTTP/1.0", "http_response": 200, "file_size_bytes": 207, "event_datetime": "2024-03-01T05:51:32.083", "event_ts": 1709232682083}',
                '{"ip": "138.201.212.70", "id": "462520009613048791", "lat": 50.4779, "lng": 12.3713, "user_agent": "Mozilla/5.0 (iPod; U; CPU iPhone OS 3_1 like Mac OS X; ks-IN) AppleWebKit/532.30.7 (KHTML, like Gecko) Version/3.0.5 Mobile/8B115 Safari/6532.30.7", "age_bracket": "18-25", "opted_into_marketing": false, "http_request": "GET eucharya.html HTTP/1.0", "http_response": 200, "file_size_bytes": 207, "event_datetime": "2024-03-01T05:51:52.083", "event_ts": 1709232682083}',
            ]

            test_stream = (
                TestStream()
                .advance_watermark_to(0)
                .add_elements([EVENTS[0], EVENTS[1], EVENTS[2]])
                .advance_watermark_to_infinity()
            )

            output = (
                p
                | test_stream
                | beam.Map(parse_json).with_output_types(EventLog)
                | beam.Map(assign_timestamp)
                | beam.Map(lambda e: (e.id, 1))
                | beam.WindowInto(beam.window.FixedWindows(20))
                | beam.CombinePerKey(sum)
            )

            EXPECTED_OUTPUT = [("462520009613048791", 2), ("462520009613048791", 1)]

            assert_that(output, equal_to(EXPECTED_OUTPUT))


if __name__ == "__main__":
    main(out=None)
