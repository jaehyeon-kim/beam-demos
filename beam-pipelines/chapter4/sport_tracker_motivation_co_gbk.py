import os
import argparse
import re
import typing
import logging

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms.window import FixedWindows, SlidingWindows
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from sport_tracker_utils import (
    ReadPositionsFromKafka,
    WriteNotificationsToKafka,
    ComputeBoxedMetrics,
    MeanPaceCombineFn,
    ElemToKV,
    MetricToKV,
)


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
        def as_motivations(
            element: typing.Tuple[
                str, typing.Tuple[typing.Iterable[float], typing.Iterable[float]]
            ],
        ):
            shorts, longs = element[1]
            short_avg = next(iter(shorts), None)
            long_avg = next(iter(longs), None)
            if long_avg in [None, 0] or short_avg in [None, 0]:
                status = None
            else:
                diff = short_avg / long_avg
                if diff < 0.9:
                    status = "underperforming"
                elif diff < 1.1:
                    status = "pacing"
                else:
                    status = "outperforming"
            if self.verbose and element[0] == "user0":
                print(
                    f"key {element[0]} short {short_avg}, long {long_avg}, status {status}"
                )
            if status is None:
                return []
            return [(element[0], status)]

        boxed = pcoll | "ComputeMetrics" >> ComputeBoxedMetrics(verbose=self.verbose)
        short_average = (
            boxed
            | "ShortWindow" >> beam.WindowInto(FixedWindows(self.short_duration))
            # | "ShortElemToKV" >> ElemToKV(indicator="short", verbose=self.verbose)
            | "ShortAverage" >> beam.CombinePerKey(MeanPaceCombineFn())
            # | "SortMericToKV"
            # >> MetricToKV(indicator="short", verbose=self.verbose).with_input_types(
            #     typing.Tuple[str, float]
            # )
        )
        long_average = (
            boxed
            | "LongWindow"
            >> beam.WindowInto(SlidingWindows(self.long_duration, self.short_duration))
            # | "LongElemToKV" >> ElemToKV(indicator="long", verbose=self.verbose)
            | "LongAverage" >> beam.CombinePerKey(MeanPaceCombineFn())
            | "MatchToShortWindow" >> beam.WindowInto(FixedWindows(self.short_duration))
            # | "LongMericToKV"
            # >> MetricToKV(indicator="long", verbose=self.verbose).with_input_types(
            #     typing.Tuple[str, float]
            # )
        )
        return (
            (short_average, long_average)
            | beam.CoGroupByKey()
            | beam.FlatMap(as_motivations)
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
