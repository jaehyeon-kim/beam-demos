import os
import argparse
import re
import typing
import logging

import apache_beam as beam
from apache_beam import pvalue, coders
from apache_beam.transforms.sql import SqlTransform
from apache_beam.transforms.window import FixedWindows, SlidingWindows
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from sport_tracker_utils import (
    Metric,
    ReadPositionsFromKafka,
    WriteNotificationsToKafka,
    ComputeBoxedMetrics,
)


class Workout(typing.NamedTuple):
    user: str
    distance: int
    duration: float


coders.registry.register_coder(Workout, coders.RowCoder)


class SportTrackerMotivation(beam.PTransform):
    def __init__(
        self,
        short_duration: int,
        long_duration: int,
        verbose: bool = False,
        label: str | None = None,
    ):
        super().__init__(label)
        self.short_duration = short_duration
        self.long_duration = long_duration
        self.verbose = verbose

    def expand(self, pcoll: pvalue.PCollection):
        def to_workout(element: typing.Tuple[str, Metric]):
            return Workout(element[0], element[1].distance, element[1].duration)

        def qry_speed(col_name: str):
            return f"""
            SELECT
                `user`,
                CASE WHEN SUM(duration) = 0 THEN 0 ELSE SUM(distance) / SUM(duration) END AS {col_name}
            FROM PCOLLECTION
            GROUP BY `user`
                """

        boxed = pcoll | "ComputeMetrics" >> ComputeBoxedMetrics(verbose=self.verbose)
        workout = boxed | beam.Map(to_workout).with_output_types(Workout)

        short_average = (
            workout
            | "ShortWindow" >> beam.WindowInto(FixedWindows(self.short_duration))
            | "ShortAverage" >> SqlTransform(qry_speed("short_avg"))
        )

        long_average = (
            workout
            | "LongWindow"
            >> beam.WindowInto(SlidingWindows(self.long_duration, self.short_duration))
            | "LongAverage" >> SqlTransform(qry_speed("long_avg"))
            | "MatchToShortWindow" >> beam.WindowInto(FixedWindows(self.short_duration))
        )

        return (
            {"short": short_average, "long": long_average}
            | SqlTransform(
                """
                WITH cte AS (
                    SELECT short.`user` AS `user`, short_avg / long_avg AS `diff`
                    FROM short
                    JOIN long ON short.`user` = long.`user`
                )
                SELECT 
                    `user`, 
                    CASE WHEN `diff` < 0.9 THEN 'underperforming'
                         WHEN `diff` < 1.1 THEN 'pacing'
                    ELSE 'outperforming' END AS performance
                FROM cte
                """
            )
            | "ToTuple" >> beam.Map(lambda e: tuple(e))
        )


def run():
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument("--runner", default="FlinkRunner", help="Apache Beam runner")
    parser.add_argument(
        "--use_own",
        action="store_true",
        default="Flag to indicate whether to use an own local cluster",
    )
    parser.add_argument("--input", default="input-topic", help="Input topic")
    parser.add_argument(
        "--job_name",
        default=re.sub("_", "-", re.sub(".py$", "", os.path.basename(__file__))),
        help="Job name",
    )
    opts = parser.parse_args()
    print(opts)

    pipeline_opts = {
        "runner": opts.runner,
        "job_name": opts.job_name,
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

    p = beam.Pipeline(options=options)
    (
        p
        | "ReadPositions"
        >> ReadPositionsFromKafka(
            bootstrap_servers=os.getenv(
                "BOOTSTRAP_SERVERS",
                "host.docker.internal:29092",
            ),
            topics=[opts.input],
            group_id=opts.job_name,
        )
        | "SportsTrackerMotivation"
        >> SportTrackerMotivation(short_duration=20, long_duration=100)
        | "WriteNotifications"
        >> WriteNotificationsToKafka(
            bootstrap_servers=os.getenv(
                "BOOTSTRAP_SERVERS",
                "host.docker.internal:29092",
            ),
            topic=opts.job_name,
        )
    )

    logging.getLogger().setLevel(logging.WARN)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
