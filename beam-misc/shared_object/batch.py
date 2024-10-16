import argparse
import random
import string
import logging
import time
from datetime import datetime

import apache_beam as beam
from apache_beam.utils import shared
from apache_beam.options.pipeline_options import PipelineOptions


def gen_customers(version: int, num_cust: int = 1000):
    d = dict()
    for r in range(num_cust):
        d[r] = {"version": version}
    return d


def gen_orders(ts: float, num_ord: int = 5, num_cust: int = 1000):
    orders = [
        {
            "order_id": "".join(random.choices(string.ascii_lowercase, k=5)),
            "customer_id": random.randrange(1, num_cust),
            "timestamp": int(ts),
        }
        for _ in range(num_ord)
    ]
    for o in orders:
        yield o


# The wrapper class is needed for a dictionary, because it does not support weak references.
class WeakRefDict(dict):
    pass


class EnrichOrderFn(beam.DoFn):
    def __init__(self, shared_handle):
        self._version = 1
        self._customers = {}
        self._shared_handle = shared_handle

    def setup(self):
        self._customer_lookup = self._shared_handle.acquire(self.load_customers)

    def load_customers(self):
        time.sleep(2)
        self._customers = gen_customers(version=self._version)
        return WeakRefDict(self._customers)

    def process(self, element):
        attr = self._customer_lookup.get(element["customer_id"], {})
        yield {**element, **attr}


def run(argv=None):
    parser = argparse.ArgumentParser(
        description="Shared class demo with a bounded PCollection"
    )
    _, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        shared_handle = shared.Shared()
        (
            p
            | beam.Create(gen_orders(ts=datetime.now().timestamp()))
            | beam.ParDo(EnrichOrderFn(shared_handle))
            | beam.Map(print)
        )

        logging.getLogger().setLevel(logging.INFO)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
