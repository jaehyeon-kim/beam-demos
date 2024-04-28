import logging
import random
import sys
import time


import apache_beam as beam
import typing

from apache_beam import RestrictionProvider
from apache_beam.io.iobase import RestrictionTracker
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker
from apache_beam.io.watermark_estimators import WalltimeWatermarkEstimator
from apache_beam.runners.sdf_utils import RestrictionTrackerView


class MyPartition:
    def __init__(self, id: int, last_offset: int, committed_offset: int = 0):
        self.id = id
        self._last_offset = last_offset
        self._committed_offset = committed_offset

    def poll(self) -> typing.Optional[int]:
        """Get new messages from this topic and partition."""
        offset = self._committed_offset + 1
        if offset > self._last_offset:
            return

        return offset

    def commit(self):
        """Commit all the polled messages."""
        self._committed_offset += 1

    def size(self) -> int:
        """Return the size of the partition."""
        return self._last_offset + 1  # offsets start at 0

    def get_committed_position(self) -> int:
        """Get the latest committed position in this partition."""
        return self._committed_offset

    def add_new_messages(self, n: int):
        """Add new messages to this partition. Used mostly for simulation purposes."""
        self._last_offset += n


class MyPartitionRestrictionTracker(OffsetRestrictionTracker):
    """This restriction tracker works with Kafka-style partitions."""

    def try_split(self, fraction_of_remainder):
        """Split the restriction in two restrictions: one with all the processed data, one with the remaining."""
        if self._last_claim_attempt is None:
            cur = self._range.start - 1
        else:
            cur = self._last_claim_attempt
        split_point = cur + 1  # for partitions Kafka-style
        if split_point <= self._range.stop:
            self._range, residual_range = self._range.split_at(split_point)
            return self._range, residual_range

    def is_bounded(self):
        """Is this a batch tracker? No, return False since this is streaming."""
        return False


class GeneratePartitionsDoFn(beam.DoFn):
    NUM_PARTITIONS = 4
    INITIAL_MAX_SIZE = 120
    MAX_INITIAL_COMMITTED = 20

    def process(self, ignored_element, *args, **kwargs) -> typing.Iterable[MyPartition]:
        # TODO
        # Calculate a random latest committed offset, between 0 and MAX_INITIAL_COMMITTED.
        # Then emit NUM_PARTIIONS partitions, with the last offset between the above number and INITIAL_MAX_SIZE
        pass


class ProcessPartitionsSplittableDoFn(beam.DoFn, RestrictionProvider):
    POLL_TIMEOUT = 0.1
    MIN_ADD_NEW_MSGS = 20
    MAX_ADD_NEW_MSGS = 100
    PROB_NEW_MSGS = 0.01
    MAX_EMPTY_POLLS = 10

    @beam.DoFn.unbounded_per_element()
    def process(self,
                element: MyPartition,
                tracker: RestrictionTrackerView = beam.DoFn.RestrictionParam(),
                wm_estim=beam.DoFn.WatermarkEstimatorParam(WalltimeWatermarkEstimator.default_provider()),
                **unused_kwargs) -> typing.Iterable[typing.Tuple[int, str]]:
        n_times_empty = 0
        while True:
            # Poll for new messages and process them
            offset_to_process = None  # TODO: Grab some offsets from the partition
            # TODO

            # Code to add more messages to simulate real word scenarios
            if offset_to_process is None:
                logging.info(f" ** Partition {element.id}: Empty poll. Waiting")
                n_times_empty += 1

            # Simulate different scenarios:

            # If the partition is exhausted and starving, add more messages
            if n_times_empty > self.MAX_EMPTY_POLLS:
                logging.info(f" ** Partition {element.id}: Waiting for too long. Adding more messages")
                self._add_new_messages(element)
                n_times_empty = 0
            # Every once in a while (with probability PROB_NEW_MSGS), the partition will get new msgs, even if it
            # is not fully processed yet.
            elif random.random() <= self.PROB_NEW_MSGS:
                logging.info(f" ** Partition {element.id}: Bingo! Adding more messages")
                self._add_new_messages(element)

            time.sleep(self.POLL_TIMEOUT)

    def _add_new_messages(self, element: MyPartition):
        element.add_new_messages(random.randint(self.MIN_ADD_NEW_MSGS, self.MAX_ADD_NEW_MSGS))

    def create_tracker(self, restriction: OffsetRange) -> MyPartitionRestrictionTracker:
        # TODO: create a tracker here
        pass

    def initial_restriction(self, element: MyPartition) -> OffsetRange:
        # TODO: what's the initial restriction for a Kafka partition?
        pass

    def restriction_size(self, element: MyPartition, restriction: OffsetRange) -> int:
        # TODO: what's the size of this partition?
        pass
