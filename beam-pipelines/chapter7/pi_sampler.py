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


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "-p", "--parallelism", type=int, default=100, help="Number of parallelism"
    )
    parser.add_argument(
        "-n", "--num_samples", type=int, default=10000, help="Number of samples"
    )

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
            | beam.Create([0])
            | beam.ParDo(
                GenerateExperiments(
                    parallelism=known_args.parallelism,
                    num_samples=known_args.num_samples,
                )
            )
            | beam.ParDo(PiSamplerDoFn())
            | beam.CombineGlobally(sum)
            | beam.Map(
                lambda e: 4
                * (1 - e / (known_args.num_samples * known_args.parallelism))
            )
            | beam.Map(print)
        )

        logging.getLogger().setLevel(logging.INFO)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
