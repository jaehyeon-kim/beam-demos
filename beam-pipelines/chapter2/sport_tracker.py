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


class ComputeMetrics(beam.PTransform):
    def expand(self, pcoll: pvalue.PCollection):
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

        return (
            pcoll | "GroupByKey" >> beam.GroupByKey() | "Compute" >> beam.Map(compute)
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
