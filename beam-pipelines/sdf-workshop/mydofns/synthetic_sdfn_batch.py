import random

import apache_beam as beam
import typing

from apache_beam import RestrictionProvider
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker
# from apache_beam.io.iobase import RestrictionTracker


class MyFile(typing.NamedTuple):
    id: int
    start: int
    end: int


class GenerateFilesDoFn(beam.DoFn):
    NUM_FILES = 20
    MAX_FILE_SIZE = 30
    MIN_FILE_SIZE = 2

    def process(self, ignored_element, *args, **kwargs) -> typing.Iterable[MyFile]:
        # create NUM_FILES of random size between MIN_FILE_SIZE and MAX_FILE_SIZE
        for k in range(self.NUM_FILES):
            size = random.randint(self.MIN_FILE_SIZE, self.MAX_FILE_SIZE + 1)
            f = MyFile(id=k, start=0, end=size)
            yield f


class ProcessFilesSplittableDoFn(beam.DoFn, RestrictionProvider):
    # Create a DoFn with the process method, and the methods of a RestrictionProvider,
    # to process files by chunks/restrictions.
    def process(
        self,
        element: MyFile,
        tracker: OffsetRestrictionTracker = beam.DoFn.RestrictionParam(),
        *args,
        **kwargs,
    ):
        restriction = tracker.current_restriction()
        for current_position in range(restriction.start, restriction.stop + 1):
            if tracker.try_claim(current_position):
                # process this chunk
                m = f"File: {element.id}, position: {current_position}"
                yield m
            else:
                return

    def create_tracker(self, restriction: OffsetRange) -> OffsetRestrictionTracker:
        return OffsetRestrictionTracker(restriction)

    def initial_restriction(self, element: MyFile) -> OffsetRange:
        return OffsetRange(start=element.start, stop=element.end)

    def restriction_size(self, element: MyFile, restriction: OffsetRange) -> int:
        return restriction.size()
