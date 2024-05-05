import os
import typing

import apache_beam as beam
from apache_beam import RestrictionProvider
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker


class MyFile(typing.NamedTuple):
    name: str
    start: int
    end: int


class GenerateFilesFn(beam.DoFn):
    def process(self, element: str) -> typing.Iterable[MyFile]:
        for file in os.listdir(element):
            if os.path.isfile(os.path.join(element, file)):
                num_lines = sum(1 for _ in open(os.path.join(element, file)))
                new_file = MyFile(file, 0, num_lines)
                print(new_file)
                yield new_file


class ProcessFilesFn(beam.DoFn, RestrictionProvider):
    def process(
        self,
        element: MyFile,
        tracker: OffsetRestrictionTracker = beam.DoFn.RestrictionParam(),
    ):
        restriction = tracker.current_restriction()
        for current_position in range(restriction.start, restriction.stop + 1):
            if tracker.try_claim(current_position):
                m = f"file: {element.name}, position: {current_position}"
                print(m)
                yield m
            else:
                return

    def create_tracker(self, restriction: OffsetRange) -> OffsetRestrictionTracker:
        return OffsetRestrictionTracker(restriction)

    def initial_restriction(self, element: MyFile) -> OffsetRange:
        return OffsetRange(start=element.start, stop=element.end)

    def restriction_size(self, element: MyFile, restriction: OffsetRange) -> int:
        return restriction.size()
