import argparse
import random
import logging
from datetime import datetime
from uuid import uuid4

import apache_beam as beam
from apache_beam.utils import shared
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.options.pipeline_options import PipelineOptions


def gen_customers(version: int, num_cust: int = 1000):
    d = dict()
    for r in range(num_cust):
        d[r] = {"version": version}
    d["timestamp"] = datetime.now().timestamp()
    return d


def gen_orders(e: float, num_ord: int = 5, num_cust: int = 1000):
    orders = [
        {
            "order_id": str(uuid4()),
            "customer_id": random.randrange(1, num_cust),
            "timestamp": e,
        }
        for _ in range(num_ord)
    ]
    for o in orders:
        yield o


class WeakRefDict(dict):
    pass


class GetNthStringFn(beam.DoFn):
    def __init__(self, shared_handle):
        self._max_stale_sec = 5
        self._version = 1
        self._customers = {}
        self._shared_handle = shared_handle

    def setup(self):
        self._customer_lookup = self._shared_handle.acquire(
            self.load_customers, datetime.now().timestamp()
        )

    def load_customers(self):
        self._customers = gen_customers(version=self._version)
        return WeakRefDict(self._customers)

    def start_bundle(self):
        ts_diff = datetime.now().timestamp() - self._customers["timestamp"]
        if ts_diff > self._max_stale_sec:
            logging.info(f"refresh customer cache after {ts_diff} seconds...")
            current_ts = datetime.now().timestamp()
            self._version += 1
            self._customer_lookup = self._shared_handle.acquire(
                self.load_customers, current_ts
            )

    def process(self, element):
        attr = self._customer_lookup.get(element["customer_id"], {})
        yield {**element, **attr}


def run(argv=None):
    parser = argparse.ArgumentParser(
        description="Shared class demo with an unbounded PCollection"
    )
    _, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        shared_handle = shared.Shared()

        (
            p
            | PeriodicImpulse(fire_interval=2, apply_windowing=False)
            | "GenerateOrders" >> beam.FlatMap(gen_orders)
            | beam.ParDo(GetNthStringFn(shared_handle))
            | beam.Map(print)
        )

        logging.getLogger().setLevel(logging.INFO)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
