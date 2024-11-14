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
        def cross_join(
            left: typing.Tuple[str, float],
            rights: typing.Iterable[typing.Tuple[str, float]],
        ):
            for x in rights:
                if left[0] == x[0]:
                    yield left[0], ([left[1]], [x[1]])

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
                logging.info(
                    f"SportTrackerMotivation track {element[0]}, short average {short_avg}, long average {long_avg}, status - {status}"
                )
            if status is None:
                return []
            return [(element[0], status)]

        boxed = pcoll | "ComputeMetrics" >> ComputeBoxedMetrics(verbose=self.verbose)
        short_average = (
            boxed
            | "ShortWindow" >> beam.WindowInto(FixedWindows(self.short_duration))
            | "ShortAverage" >> beam.CombinePerKey(MeanPaceCombineFn())
        )
        long_average = (
            boxed
            | "LongWindow"
            >> beam.WindowInto(SlidingWindows(self.long_duration, self.short_duration))
            | "LongAverage" >> beam.CombinePerKey(MeanPaceCombineFn())
            | "MatchToShortWindow" >> beam.WindowInto(FixedWindows(self.short_duration))
        )
        return (
            short_average
            | "ApplyCrossJoin"
            >> beam.FlatMap(cross_join, rights=beam.pvalue.AsIter(long_average))
            | beam.FlatMap(as_motivations)
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
    parser.add_argument("--short_duration", default=20, type=int, help="Input topic")
    parser.add_argument("--long_duration", default=100, type=int, help="Input topic")
    parser.add_argument(
        "--verbose", action="store_true", default="Whether to enable log messages"
    )
    parser.set_defaults(verbose=False)
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
            | "SportsTrackerMotivation"
            >> SportTrackerMotivation(
                short_duration=known_args.short_duration,
                long_duration=known_args.long_duration,
                verbose=known_args.verbose,
            )
            | "WriteNotifications"
            >> WriteNotificationsToKafka(
                bootstrap_servers=known_args.bootstrap_servers,
                topic=known_args.output_topic,
            )
        )

        logging.getLogger().setLevel(logging.INFO)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
