import os
import argparse
import re
import logging
import typing

import apache_beam as beam
from apache_beam import pvalue, coders
from apache_beam.transforms.sql import SqlTransform
from apache_beam.transforms.window import TimestampCombiner, FixedWindows
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from sport_tracker_utils import (
    Position,
    ReadPositionsFromKafka,
    WriteMetricsToKafka,
)


class Track(typing.NamedTuple):
    user: str
    spot: int
    timestamp: float


coders.registry.register_coder(Track, coders.RowCoder)


def to_track(element: typing.Tuple[str, Position]):
    return Track(element[0], element[1].spot, element[1].timestamp)


class ComputeMetrics(beam.PTransform):
    def expand(self, pcoll: pvalue.PCollection):
        return (
            pcoll
            | "ToTrack" >> beam.Map(to_track).with_output_types(Track)
            | "Compute"
            >> SqlTransform(
                """
                WITH cte AS (
                    SELECT
                        `user`,
                        MIN(`spot`) - MAX(`spot`) AS distance,
                        MIN(`timestamp`) - MAX(`timestamp`) AS duration
                    FROM PCOLLECTION
                    GROUP BY `user`
                )
                SELECT 
                    `user`,
                    CASE WHEN duration = 0 THEN 0 ELSE distance / duration END AS speed
                FROM cte
                """
            )
            | "ToTuple"
            >> beam.Map(lambda e: tuple(e)).with_output_types(typing.Tuple[str, float])
        )


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--bootstrap_servers",
        default="host.docker.internal:29092",
        help="Kafka bootstrap server addresses",
    )
    parser.add_argument("--input_topic", default="input-topic", help="Input topic")
    parser.add_argument(
        "--output_topic",
        default=re.sub("_", "-", re.sub(".py$", "", os.path.basename(__file__))),
        help="Output topic",
    )
    parser.add_argument(
        "--deprecated_read",
        action="store_true",
        default="Whether to use a deprecated read. See https://github.com/apache/beam/issues/20979",
    )
    parser.set_defaults(deprecated_read=False)

    known_args, pipeline_args = parser.parse_known_args(argv)

    # # We use the save_main_session option because one or more DoFn's in this
    # # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    print(f"known args - {known_args}")
    print(f"pipeline options - {pipeline_options.display_data()}")

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadPositions"
            >> ReadPositionsFromKafka(
                bootstrap_servers=known_args.bootstrap_servers,
                topics=[known_args.input_topic],
                group_id=f"{known_args.output_topic}-group",
                deprecated_read=known_args.deprecated_read,
            )
            | "Windowing" >> beam.WindowInto(FixedWindows(5), allowed_lateness=0)
            | "ComputeMetrics" >> ComputeMetrics()
            | "WriteNotifications"
            >> WriteMetricsToKafka(
                bootstrap_servers=known_args.bootstrap_servers,
                topic=known_args.output_topic,
                deprecated_read=known_args.deprecated_read,
            )
        )

        logging.getLogger().setLevel(logging.WARN)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
