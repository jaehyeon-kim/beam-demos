import sys
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from user_traffic import EventLog, UserTraffic, parse_json, Aggregate


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


class ParseJsonTest(unittest.TestCase):
    def test_parse_json(self):
        with TestPipeline() as p:
            LINES = [
                '{"ip": "138.201.212.70", "id": "462520009613048791", "lat": 50.4779, "lng": 12.3713, "user_agent": "Mozilla/5.0 (iPod; U; CPU iPhone OS 3_1 like Mac OS X; ks-IN) AppleWebKit/532.30.7 (KHTML, like Gecko) Version/3.0.5 Mobile/8B115 Safari/6532.30.7", "age_bracket": "18-25", "opted_into_marketing": false, "http_request": "GET eucharya.html HTTP/1.0", "http_response": 200, "file_size_bytes": 207, "event_datetime": "2024-03-01T05:51:22.083", "event_ts": 1709232682083}',
                '{"ip": "105.100.237.193", "id": "5135574965990269004", "lat": 36.7323, "lng": 3.0875, "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_9 rv:2.0; wo-SN) AppleWebKit/531.18.2 (KHTML, like Gecko) Version/4.0.4 Safari/531.18.2", "age_bracket": "26-40", "opted_into_marketing": false, "http_request": "GET coniferophyta.html HTTP/1.0", "http_response": 200, "file_size_bytes": 427, "event_datetime": "2024-03-01T05:48:52.985", "event_ts": 1709232532985}',
            ]

            EXPECTED_OUTPUT = [
                EventLog(
                    ip="138.201.212.70",
                    id="462520009613048791",
                    lat=50.4779,
                    lng=12.3713,
                    user_agent="Mozilla/5.0 (iPod; U; CPU iPhone OS 3_1 like Mac OS X; ks-IN) AppleWebKit/532.30.7 (KHTML, like Gecko) Version/3.0.5 Mobile/8B115 Safari/6532.30.7",
                    age_bracket="18-25",
                    opted_into_marketing=False,
                    http_request="GET eucharya.html HTTP/1.0",
                    http_response=200,
                    file_size_bytes=207,
                    event_datetime="2024-03-01T05:51:22.083",
                    event_ts=1709232682083,
                ),
                EventLog(
                    ip="105.100.237.193",
                    id="5135574965990269004",
                    lat=36.7323,
                    lng=3.0875,
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_9 rv:2.0; wo-SN) AppleWebKit/531.18.2 (KHTML, like Gecko) Version/4.0.4 Safari/531.18.2",
                    age_bracket="26-40",
                    opted_into_marketing=False,
                    http_request="GET coniferophyta.html HTTP/1.0",
                    http_response=200,
                    file_size_bytes=427,
                    event_datetime="2024-03-01T05:48:52.985",
                    event_ts=1709232532985,
                ),
            ]

            output = (
                p
                | beam.Create(LINES)
                | beam.Map(parse_json).with_output_types(EventLog)
            )

            assert_that(output, equal_to(EXPECTED_OUTPUT))

    def test_parse_null_lat_lng(self):
        with TestPipeline() as p:
            LINES = [
                '{"ip": "138.201.212.70", "id": "462520009613048791", "lat": null, "lng": null, "user_agent": "Mozilla/5.0 (iPod; U; CPU iPhone OS 3_1 like Mac OS X; ks-IN) AppleWebKit/532.30.7 (KHTML, like Gecko) Version/3.0.5 Mobile/8B115 Safari/6532.30.7", "age_bracket": "18-25", "opted_into_marketing": false, "http_request": "GET eucharya.html HTTP/1.0", "http_response": 200, "file_size_bytes": 207, "event_datetime": "2024-03-01T05:51:22.083", "event_ts": 1709232682083}',
            ]

            EXPECTED_OUTPUT = [
                EventLog(
                    ip="138.201.212.70",
                    id="462520009613048791",
                    lat=-1,
                    lng=-1,
                    user_agent="Mozilla/5.0 (iPod; U; CPU iPhone OS 3_1 like Mac OS X; ks-IN) AppleWebKit/532.30.7 (KHTML, like Gecko) Version/3.0.5 Mobile/8B115 Safari/6532.30.7",
                    age_bracket="18-25",
                    opted_into_marketing=False,
                    http_request="GET eucharya.html HTTP/1.0",
                    http_response=200,
                    file_size_bytes=207,
                    event_datetime="2024-03-01T05:51:22.083",
                    event_ts=1709232682083,
                ),
            ]

            output = (
                p
                | beam.Create(LINES)
                | beam.Map(parse_json).with_output_types(EventLog)
            )

            assert_that(output, equal_to(EXPECTED_OUTPUT))


class AggregateTest(unittest.TestCase):
    def test_aggregate(self):
        with TestPipeline() as p:
            LINES = [
                '{"ip": "138.201.212.70", "id": "462520009613048791", "lat": 50.4779, "lng": 12.3713, "user_agent": "Mozilla/5.0 (iPod; U; CPU iPhone OS 3_1 like Mac OS X; ks-IN) AppleWebKit/532.30.7 (KHTML, like Gecko) Version/3.0.5 Mobile/8B115 Safari/6532.30.7", "age_bracket": "18-25", "opted_into_marketing": false, "http_request": "GET eucharya.html HTTP/1.0", "http_response": 200, "file_size_bytes": 207, "event_datetime": "2024-03-01T05:51:22.083", "event_ts": 1709232682083}',
                '{"ip": "138.201.212.70", "id": "462520009613048791", "lat": 50.4779, "lng": 12.3713, "user_agent": "Mozilla/5.0 (iPod; U; CPU iPhone OS 3_1 like Mac OS X; ks-IN) AppleWebKit/532.30.7 (KHTML, like Gecko) Version/3.0.5 Mobile/8B115 Safari/6532.30.7", "age_bracket": "18-25", "opted_into_marketing": false, "http_request": "GET blastocladiomycota.html HTTP/1.0", "http_response": 200, "file_size_bytes": 446, "event_datetime": "2024-03-01T05:51:48.719", "event_ts": 1709232708719}',
                '{"ip": "138.201.212.70", "id": "462520009613048791", "lat": 50.4779, "lng": 12.3713, "user_agent": "Mozilla/5.0 (iPod; U; CPU iPhone OS 3_1 like Mac OS X; ks-IN) AppleWebKit/532.30.7 (KHTML, like Gecko) Version/3.0.5 Mobile/8B115 Safari/6532.30.7", "age_bracket": "18-25", "opted_into_marketing": false, "http_request": "GET home.html HTTP/1.0", "http_response": 200, "file_size_bytes": 318, "event_datetime": "2024-03-01T05:51:35.181", "event_ts": 1709232695181}',
            ]

            EXPECTED_OUTPUT = [
                UserTraffic(
                    id="462520009613048791",
                    page_views=3,
                    total_bytes=971,
                    max_bytes=446,
                    min_bytes=207,
                )
            ]

            output = (
                p
                | beam.Create(LINES)
                | beam.Map(parse_json).with_output_types(EventLog)
                | beam.Map(lambda e: (e.id, e.file_size_bytes))
                | beam.GroupByKey()
                | beam.ParDo(Aggregate()).with_output_types(UserTraffic)
            )

            assert_that(output, equal_to(EXPECTED_OUTPUT))


if __name__ == "__main__":
    main(out=None)
