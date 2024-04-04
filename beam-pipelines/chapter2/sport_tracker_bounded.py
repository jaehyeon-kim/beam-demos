import os
import argparse
import logging
import re
import typing
import math

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions


class Position(typing.NamedTuple):
    latitude: float
    longitude: float
    timestamp: int


def read_file(filename: str, inputpath: str):
    with open(os.path.join(inputpath, filename), "r") as f:
        return f.readlines()


def to_positions(input: str):
    workout, latitude, longitude, timestamp = tuple(re.sub("\n", "", input).split("\t"))
    return workout, Position(
        latitude=float(latitude), longitude=float(longitude), timestamp=int(timestamp)
    )


def distance(first: Position, second: Position):
    EARTH_DIAMETER = 6_371_000
    delta_latitude = (first.latitude - second.latitude) * math.pi / 180
    delta_longitude = (first.longitude - second.longitude) * math.pi / 180
    latitude_inc_in_meters = math.sqrt(2 * (1 - math.cos(delta_latitude)))
    longitude_inc_in_meters = math.sqrt(2 * (1 - math.cos(delta_longitude)))
    return EARTH_DIAMETER * math.sqrt(
        latitude_inc_in_meters * latitude_inc_in_meters
        + longitude_inc_in_meters * longitude_inc_in_meters
    )


def compute_matrics(key: str, positions: typing.List[Position]):
    last: Position = None
    total_time = 0
    total_distance = 0
    for p in sorted(positions, key=lambda p: p.timestamp):
        if last is not None:
            total_distance += distance(last, p)
            total_time += p.timestamp - last.timestamp
        last = p
    return key, total_time, total_distance


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
        >> beam.Create(
            read_file("test-tracker-data-200.txt", os.path.join(PARENT_DIR, "inputs"))
        )
        | "To positions" >> beam.Map(to_positions)
        | "Group by workout" >> beam.GroupByKey()
        | "Compute metrics" >> beam.Map(lambda e: compute_matrics(*e))
        | beam.Map(print)
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
