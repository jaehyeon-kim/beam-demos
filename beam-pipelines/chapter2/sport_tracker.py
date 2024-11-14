import os
import argparse
import re
import logging
import typing

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms.window import GlobalWindows, TimestampCombiner
from apache_beam.transforms.trigger import (
    AfterWatermark,
    AccumulationMode,
    AfterProcessingTime,
)
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from sport_tracker_utils import (
    Position,
    ReadPositionsFromKafka,
    WriteMetricsToKafka,
)


def compute(element: typing.Tuple[str, typing.Iterable[Position]]):
    last: Position = None
    distance = 0
    duration = 0
    for p in sorted(element[1], key=lambda p: p.timestamp):
        if last is not None:
            distance += abs(p.spot - last.spot)
            duration += p.timestamp - last.timestamp
        last = p
    return element[0], distance / duration if duration > 0 else 0


class ComputeMetrics(beam.PTransform):
    def expand(self, pcoll: pvalue.PCollection):
        return (
            pcoll | "GroupByKey" >> beam.GroupByKey() | "Compute" >> beam.Map(compute)
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
            | "Windowing"
            >> beam.WindowInto(
                GlobalWindows(),
                trigger=AfterWatermark(early=AfterProcessingTime(3)),
                # impossible to allow late data using default timestamp policies
                # unless manually specifying timestamp by producer
                allowed_lateness=0,
                timestamp_combiner=TimestampCombiner.OUTPUT_AT_LATEST,
                accumulation_mode=AccumulationMode.ACCUMULATING,
            )
            | "ComputeMetrics" >> ComputeMetrics()
            | "WriteNotifications"
            >> WriteMetricsToKafka(
                bootstrap_servers=known_args.bootstrap_servers,
                topic=known_args.output_topic,
                deprecated_read=known_args.deprecated_read,  # TO DO: remove as it applies only to ReadFromKafka
            )
        )

        logging.getLogger().setLevel(logging.WARN)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
