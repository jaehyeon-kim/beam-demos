import argparse
import random
import string
import logging
import time
from datetime import datetime

import apache_beam as beam
from apache_beam.utils import shared
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.options.pipeline_options import PipelineOptions


def gen_customers(version: int, tag: float, num_cust: int = 1000):
    d = dict()
    for r in range(num_cust):
        d[r] = {"version": version}
    d["tag"] = tag
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


# wrapper class needed for a dictionary since it does not support weak references
class WeakRefDict(dict):
    pass


class EnrichOrderFn(beam.DoFn):
    def __init__(self, shared_handle, max_stale_sec):
        self._max_stale_sec = max_stale_sec
        self._version = 1
        self._customers = {}
        self._shared_handle = shared_handle

    def setup(self):
        self._customer_lookup = self._shared_handle.acquire(
            self.load_customers, self.set_tag()
        )

    def set_tag(self):
        current_ts = datetime.now().timestamp()
        return current_ts - (current_ts % self._max_stale_sec)

    def load_customers(self):
        time.sleep(2)
        self._customers = gen_customers(version=self._version, tag=self.set_tag())
        return WeakRefDict(self._customers)

    def start_bundle(self):
        if self.set_tag() > self._customers["tag"]:
            logging.info(
                f"refresh customer cache, current tag {self.set_tag()}, existing tag {self._customers['tag']}..."
            )
            self._version += 1
            self._customer_lookup = self._shared_handle.acquire(
                self.load_customers, self.set_tag()
            )

    def process(self, element):
        attr = self._customer_lookup.get(element["customer_id"], {})
        yield {**element, **attr}


def run(argv=None):
    parser = argparse.ArgumentParser(
        description="Shared class demo with an unbounded PCollection"
    )
    parser.add_argument(
        "--fire_interval",
        "-f",
        type=int,
        default=2,
        help="Interval at which to output elements.",
    )
    parser.add_argument(
        "--max_stale_sec",
        "-m",
        type=int,
        default=5,
        help="Maximum second of staleness.",
    )
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        shared_handle = shared.Shared()
        (
            p
            | PeriodicImpulse(
                fire_interval=known_args.fire_interval, apply_windowing=False
            )
            | beam.FlatMap(gen_orders)
            | beam.ParDo(EnrichOrderFn(shared_handle, known_args.max_stale_sec))
            | beam.Map(print)
        )

        logging.getLogger().setLevel(logging.INFO)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
