import os
import logging
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from directory_watch import DirectoryWatchFn
from file_read import ProcessFilesFn


def run():
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument("--runner", default="DirectRunner", help="Apache Beam runner")
    opts = parser.parse_args()
    print(opts)

    pipeline_opts = {
        "runner": opts.runner,
        "environment_type": "LOOPBACK",
        "streaming": True,
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
        | beam.ParDo(DirectoryWatchFn())
        | beam.ParDo(ProcessFilesFn())
        # | beam.Map(print)
    )

    logging.getLogger().setLevel(logging.WARN)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
