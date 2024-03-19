import os
import datetime
import argparse
import json
import logging
import typing

import apache_beam as beam
from apache_beam.io import kafka
from apache_beam.transforms.sql import SqlTransform
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


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


def decode_message(kafka_kv: tuple):
    # Incoming Kafka records must have a key associated.
    # Otherwise, Beam throws an exception with null keys.
    #   Example: (b'key', b'value')
    return kafka_kv[1].decode("utf-8")


def create_message(element: dict):
    key = {"event_id": element["event_id"], "window_start": element["window_start"]}
    print(element)
    return json.dumps(key).encode("utf-8"), json.dumps(element).encode("utf-8")


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
        "--runner", default="FlinkRunner", help="Specify Apache Beam Runner"
    )
    parser.add_argument(
        "--use_own",
        action="store_true",
        default="Flag to indicate whether to use an own local cluster",
    )
    opts = parser.parse_args()

    options = PipelineOptions()
    pipeline_opts = {
        "runner": opts.runner,
        "job_name": "traffic-agg-sql",
        "environment_type": "LOOPBACK",
        "streaming": True,
        "parallelism": 3,
        "experiments": [
            "use_deprecated_read"
        ],  ## https://github.com/apache/beam/issues/20979
        "checkpointing_interval": "60000",
    }
    if opts.use_own is True:
        pipeline_opts = {**pipeline_opts, **{"flink_master": "localhost:8081"}}
    print(pipeline_opts)
    options = PipelineOptions([], **pipeline_opts)
    # Required, else it will complain that when importing worker functions
    options.view_as(SetupOptions).save_main_session = True

    query = """
    WITH cte AS (
        SELECT
            id, 
            CAST(event_datetime AS TIMESTAMP) AS ts
        FROM PCOLLECTION
    )
    SELECT
        id AS event_id,
        CAST(TUMBLE_START(ts, INTERVAL '10' SECOND) AS VARCHAR) AS window_start,
        CAST(TUMBLE_END(ts, INTERVAL '10' SECOND) AS VARCHAR) AS window_end,
        COUNT(*) AS page_view
    FROM cte
    GROUP BY
        TUMBLE(ts, INTERVAL '10' SECOND), id
    """

    p = beam.Pipeline(options=options)
    (
        p
        | "Read from Kafka"
        >> kafka.ReadFromKafka(
            consumer_config={
                "bootstrap.servers": os.getenv(
                    "BOOTSTRAP_SERVERS",
                    "host.docker.internal:29092",
                ),
                "auto.offset.reset": "earliest",
                # "enable.auto.commit": "true",
                "group.id": "traffic-agg-sql",
            },
            topics=["website-visit"],
        )
        | "Decode messages" >> beam.Map(decode_message)
        | "Parse elements" >> beam.Map(parse_json).with_output_types(EventLog)
        | "Format timestamp" >> beam.Map(format_timestamp).with_output_types(EventLog)
        | "Count per minute" >> SqlTransform(query)
        | "To dictionary" >> beam.Map(lambda e: e._asdict())
        | "Create messages"
        >> beam.Map(create_message).with_output_types(typing.Tuple[bytes, bytes])
        | "Write to Kafka"
        >> kafka.WriteToKafka(
            producer_config={
                "bootstrap.servers": os.getenv(
                    "BOOTSTRAP_SERVERS",
                    "host.docker.internal:29092",
                )
            },
            topic="traffic-agg-sql",
        )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
