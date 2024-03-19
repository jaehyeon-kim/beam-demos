import os
import datetime
import argparse
import json
import logging
import typing

import apache_beam as beam
from apache_beam.transforms.sql import SqlTransform
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


beam.coders.registry.register_coder(EventLog, beam.coders.RowCoder)


def parse_json(element: str):
    row = json.loads(element)
    # lat/lng sometimes empty string
    if not row["lat"] or not row["lng"]:
        row = {**row, **{"lat": -1, "lng": -1}}
    return EventLog(**row)


def format_timestamp(element: EventLog):
    event_ts = datetime.datetime.fromisoformat(element.event_datetime)
    temp_dict = element._asdict()
    temp_dict["event_datetime"] = datetime.datetime.strftime(
        event_ts, "%Y-%m-%d %H:%M:%S"
    )
    return EventLog(**temp_dict)


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

    calcite_query = """
    WITH cte AS (
        SELECT CAST(event_datetime AS TIMESTAMP) AS ts
        FROM PCOLLECTION
    )
    SELECT
        CAST(TUMBLE_START(ts, INTERVAL '1' MINUTE) AS VARCHAR) AS window_start,
        CAST(TUMBLE_END(ts, INTERVAL '1' MINUTE) AS VARCHAR) AS window_end,
        COUNT(*) AS page_view
    FROM cte
    GROUP BY
        TUMBLE(ts, INTERVAL '1' MINUTE)
    """

    zeta_query = """
    SELECT
        STRING(window_start) AS start_time,
        STRING(window_end) AS end_time,
        COUNT(*) AS page_views
    FROM
        TUMBLE(
            (SELECT TIMESTAMP(event_datetime) AS ts FROM PCOLLECTION),
            DESCRIPTOR(ts),
            'INTERVAL 1 MINUTE')
    GROUP BY
        window_start, window_end
    """

    p = beam.Pipeline(options=options)
    transformed = (
        p
        | "Read from files"
        >> beam.io.ReadFromText(
            file_pattern=os.path.join(os.path.join(PARENT_DIR, "inputs", "*.out"))
        )
        | "Parse elements" >> beam.Map(parse_json).with_output_types(EventLog)
        | "Format timestamp" >> beam.Map(format_timestamp).with_output_types(EventLog)
    )

    ## calcite sql output
    (
        transformed
        | "Count per minute via Caltice" >> SqlTransform(calcite_query)
        | "To Dict via Caltice" >> beam.Map(lambda e: e._asdict())
        | "Write to file via Caltice"
        >> beam.io.WriteToText(
            file_path_prefix=os.path.join(
                PARENT_DIR,
                "outputs",
                f"{int(datetime.datetime.now().timestamp() * 1000)}-calcite",
            ),
            file_name_suffix=".out",
        )
    )

    ## zeta sql output
    (
        transformed
        | "Count per minute via Zeta" >> SqlTransform(zeta_query, dialect="zetasql")
        | "To Dict via Zeta" >> beam.Map(lambda e: e._asdict())
        | "Write to file via Zeta"
        >> beam.io.WriteToText(
            file_path_prefix=os.path.join(
                PARENT_DIR,
                "outputs",
                f"{int(datetime.datetime.now().timestamp() * 1000)}-zeta",
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
