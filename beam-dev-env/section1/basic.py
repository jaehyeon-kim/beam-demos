import os
import datetime
import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions


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
        | "Read from files"
        >> beam.io.ReadFromText(
            file_pattern=os.path.join(PARENT_DIR, opts.inputs, "*.out")
        )
        | "Parse Json" >> beam.Map(lambda line: json.loads(line))
        | "Filter status" >> beam.Filter(lambda d: d["opted_into_marketing"] is True)
        | "Select columns"
        >> beam.Map(
            lambda d: {
                k: v
                for k, v in d.items()
                if k in ["ip", "id", "lat", "lng", "age_bracket"]
            }
        )
        | "Write to file"
        >> beam.io.WriteToText(
            file_path_prefix=os.path.join(
                PARENT_DIR,
                "outputs",
                f"{int(datetime.datetime.now().timestamp() * 1000)}",
            ),
            file_name_suffix=".out",
        )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    if opts.runner != "FlinkRunner":
        p.run()
    else:
        p.run().wait_until_finish()


if __name__ == "__main__":
    run()
