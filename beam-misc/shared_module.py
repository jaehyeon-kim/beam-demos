import argparse
import random
import logging
from datetime import datetime
from uuid import uuid4

import apache_beam as beam
from apache_beam.utils import shared
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.options.pipeline_options import PipelineOptions


def gen_customers(version: int):
    d = dict()
    for r in range(10):
        d[r] = {"version": version}
    return d


def gen_orders(e: float):
    orders = [
        {
            "order_id": str(uuid4()),
            "customer_id": random.randrange(1, 10),
            "timestamp": e,
        }
        for _ in range(1)
    ]
    for o in orders:
        yield o


class WeakRefDict(dict):
    pass


class GetNthStringFn(beam.DoFn):
    def __init__(self, shared_handle):
        self._version = 1
        self._customers = None
        self._shared_handle = shared_handle

    def setup(self):
        def load_customers():
            self._customers = gen_customers(version=self._version)
            return WeakRefDict(self._customers)

        self._customer_lookup = self._shared_handle.acquire(load_customers)

    def process(self, element):
        attr = self._customer_lookup.get(element["customer_id"], {})
        yield {**element, **attr}


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    known_args, pipeline_args = parser.parse_known_args(argv)

    # # We use the save_main_session option because one or more DoFn's in this
    # # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    # pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    print(f"known args - {known_args}")
    print(f"pipeline options - {pipeline_options.display_data()}")

    with beam.Pipeline(options=pipeline_options) as p:
        shared_handle = shared.Shared()

        (
            p
            | PeriodicImpulse(fire_interval=3, apply_windowing=False)
            | "GenerateOrders" >> beam.FlatMap(gen_orders)
            | beam.ParDo(GetNthStringFn(shared_handle))
            | beam.Map(print)
        )

        logging.getLogger().setLevel(logging.INFO)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
