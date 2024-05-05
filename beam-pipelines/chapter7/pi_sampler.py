import os
import random
import logging
import argparse

import apache_beam as beam
from apache_beam import RestrictionProvider
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class GenerateExperiments(beam.DoFn):
    def __init__(self, parallelism: int, num_samples: int):
        self.parallelism = parallelism
        self.num_samples = num_samples

    def process(self, ignored_element):
        for _ in range(self.parallelism):
            yield self.num_samples


class PiSamplerDoFn(beam.DoFn, RestrictionProvider):
    def process(
        self,
        element: int,
        tracker: OffsetRestrictionTracker = beam.DoFn.RestrictionParam(),
    ):
        restriction = tracker.current_restriction()
        for current_position in range(restriction.start, restriction.stop + 1):
            if tracker.try_claim(current_position):
                x, y = random.random(), random.random()
                if x * x + y * y > 1:
                    yield 1
            else:
                return

    def create_tracker(self, restriction: OffsetRange) -> OffsetRestrictionTracker:
        return OffsetRestrictionTracker(restriction)

    def initial_restriction(self, element: int) -> OffsetRange:
        return OffsetRange(start=0, stop=element)

    def restriction_size(self, element: int, restriction: OffsetRange) -> int:
        return restriction.size()


def run():
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument("--runner", default="DirectRunner", help="Apache Beam runner")
    parser.add_argument(
        "-p", "--parallelism", type=int, default=100, help="Number of parallelism"
    )
    parser.add_argument(
        "-n", "--num_samples", type=int, default=10000, help="Number of samples"
    )
    opts = parser.parse_args()
    print(opts)

    pipeline_opts = {
        "runner": opts.runner,
        "environment_type": "LOOPBACK",
        "streaming": False,
    }
    print(pipeline_opts)
    options = PipelineOptions([], **pipeline_opts)
    # Required, else it will complain that when importing worker functions
    options.view_as(SetupOptions).save_main_session = True

    PARENT_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    p = beam.Pipeline(options=options)
    (
        p
        | beam.Create([os.path.join(PARENT_DIR, "inputs")])
        | beam.ParDo(
            GenerateExperiments(
                parallelism=opts.parallelism, num_samples=opts.num_samples
            )
        )
        | beam.ParDo(PiSamplerDoFn())
        | beam.CombineGlobally(sum)
        | beam.Map(
            lambda e: round(4 * (1 - e / (opts.num_samples * opts.parallelism)), 2)
        )
        | beam.Map(print)
    )

    logging.getLogger().setLevel(logging.WARN)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
