import random

import apache_beam as beam
import typing

from apache_beam import RestrictionProvider


class MyFile(typing.NamedTuple):
    id: int
    start: int
    end: int


class GenerateFilesDoFn(beam.DoFn):
    NUM_FILES = 20
    MAX_FILE_SIZE = 30
    MIN_FILE_SIZE = 2

    def process(self, ignored_element, *args, **kwargs) -> typing.Iterable[MyFile]:
        # TODO: create NUM_FILES of random size between MIN_FILE_SIZE and MAX_FILE_SIZE
        pass


class ProcessFilesSplittableDoFn(beam.DoFn, RestrictionProvider):
    # TODO
    # Create a DoFn with the process method, and the methods of a RestrictionProvider,
    # to process files by chunks/restrictions.
    pass
