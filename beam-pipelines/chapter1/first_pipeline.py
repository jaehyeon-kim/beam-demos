import os
import datetime
import argparse
import logging
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions


def read_file(filename: str, inputpath: str):
    with open(os.path.join(inputpath, filename), "r") as f:
        return f.readlines()


def tokenize(element: str):
    return re.findall(r"[A-Za-z\']+", element)


def run():
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--inputs",
        default="inputs",
        help="Specify folder name that event records are saved",
    )
    parser.add_argument(
        "--runner", default="DirectRunner", help="Specify Apache Beam Runner"
    )
    opts = parser.parse_args()
    PARENT_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    options = PipelineOptions()
    options.view_as(StandardOptions).runner = opts.runner

    p = beam.Pipeline(options=options)
    (
        p
        | "Read file"
        >> beam.Create([read_file("lorem.txt", os.path.join(PARENT_DIR, "inputs"))])
        | "to string" >> beam.ToString.Iterables()
        | "Extract words" >> beam.FlatMap(tokenize)
        | "Count per word" >> beam.combiners.Count.PerElement()
        # | beam.Map(print)
        | "Write to file"
        >> beam.io.WriteToText(
            file_path_prefix=os.path.join(
                PARENT_DIR,
                "outputs",
                f"{opts.runner.lower()}-{int(datetime.datetime.now().timestamp() * 1000)}",
            ),
            file_name_suffix=".out",
        )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
