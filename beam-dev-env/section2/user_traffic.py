import os
import datetime
import argparse
import json
import logging
import typing

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions


class EventLog(typing.NamedTuple):
    ip: str
    id: str
    lat: float
    lng: float
    user_agent: str
    age_bracket: str
    opted_into_marketing: bool
    http_request: str
    http_response: int
    file_size_bytes: int
    event_datetime: str
    event_ts: int


class UserTraffic(typing.NamedTuple):
    id: str
    page_views: int
    total_bytes: int
    max_bytes: int
    min_bytes: int


beam.coders.registry.register_coder(EventLog, beam.coders.RowCoder)
beam.coders.registry.register_coder(UserTraffic, beam.coders.RowCoder)


def parse_json(element: str):
    row = json.loads(element)
    # lat/lng sometimes empty string
    if not row["lat"] or not row["lng"]:
        row = {**row, **{"lat": -1, "lng": -1}}
    return EventLog(**row)


class Aggregate(beam.DoFn):
    def process(self, element: typing.Tuple[str, typing.Iterable[int]]):
        key, values = element
        yield UserTraffic(
            id=key,
            page_views=len(values),
            total_bytes=sum(values),
            max_bytes=max(values),
            min_bytes=min(values),
        )


def run():
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--inputs",
        default="inputs",
        help="Specify folder name that event records are saved",
    )
    parser.add_argument(
        "--runner", default="DirectRunner", help="Specify Apache Beam Runner"
    )
    opts = parser.parse_args()
    PARENT_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    options = PipelineOptions()
    options.view_as(StandardOptions).runner = opts.runner

    p = beam.Pipeline(options=options)
    (
        p
        | "Read from files"
        >> beam.io.ReadFromText(
            file_pattern=os.path.join(PARENT_DIR, opts.inputs, "*.out")
        )
        | "Parse elements" >> beam.Map(parse_json).with_output_types(EventLog)
        | "Form key value pair" >> beam.Map(lambda e: (e.id, e.file_size_bytes))
        | "Group by key" >> beam.GroupByKey()
        | "Aggregate by id" >> beam.ParDo(Aggregate()).with_output_types(UserTraffic)
        | "To dict" >> beam.Map(lambda e: e._asdict())
        | "Write to file"
        >> beam.io.WriteToText(
            file_path_prefix=os.path.join(
                PARENT_DIR,
                "outputs",
                f"{int(datetime.datetime.now().timestamp() * 1000)}",
            ),
            file_name_suffix=".out",
        )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    if opts.runner != "FlinkRunner":
        p.run()
    else:
        p.run().wait_until_finish()


if __name__ == "__main__":
    run()
