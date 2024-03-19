import argparse
import logging
import typing

import apache_beam as beam
from apache_beam.transforms.sql import SqlTransform
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class MyItem(typing.NamedTuple):
    id: int
    name: str
    value: float


beam.coders.registry.register_coder(MyItem, beam.coders.RowCoder)


def convert_to_item(row: list):
    cols = ["id", "name", "value"]
    return MyItem(**dict(zip(cols, row)))


def run():
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--runner", default="FlinkRunner", help="Specify Apache Beam Runner"
    )
    parser.add_argument(
        "--use_own",
        action="store_true",
        default="Flag to indicate whether to use an own local cluster",
    )
    opts = parser.parse_args()

    options = PipelineOptions()
    pipeline_opts = {
        "runner": opts.runner,
        "job_name": "sql-transform",
        "environment_type": "LOOPBACK",
    }
    if opts.use_own is True:
        pipeline_opts = {**pipeline_opts, **{"flink_master": "localhost:8081"}}
    print(pipeline_opts)
    options = PipelineOptions([], **pipeline_opts)
    # Required, else it will complain that when importing worker functions
    options.view_as(SetupOptions).save_main_session = True

    query = """
    SELECT * FROM PCOLLECTION WHERE name = 'jack'
    """

    p = beam.Pipeline(options=options)
    (
        p
        | beam.Create([[1, "john", 123], [2, "jane", 234], [3, "jack", 345]])
        | beam.Map(convert_to_item).with_output_types(MyItem)
        | SqlTransform(query)
        | beam.Map(print)
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
