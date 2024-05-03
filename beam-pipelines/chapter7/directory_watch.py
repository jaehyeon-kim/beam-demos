import os
import json
import typing

import apache_beam as beam
from apache_beam import RestrictionProvider
from apache_beam.utils.timestamp import Duration
from apache_beam.io.iobase import RestrictionTracker
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.transforms.core import WatermarkEstimatorProvider
from apache_beam.runners.sdf_utils import RestrictionTrackerView
from apache_beam.utils.timestamp import Timestamp, MIN_TIMESTAMP, MAX_TIMESTAMP

from file_read import MyFile


class DirectoryWatchRestriction:
    def __init__(self, already_processed: typing.Set[str], finished: bool):
        self.already_processed = already_processed
        self.finished = finished

    def as_primary(self):
        self.finished = True
        return self

    def as_residual(self):
        return DirectoryWatchRestriction(self.already_processed, False)

    def add_new(self, file: str):
        self.already_processed.add(file)

    def size(self) -> int:
        return 1

    @classmethod
    def from_json(cls, value: str):
        d = json.loads(value)
        return cls(
            already_processed=set(d["already_processed"]), finished=d["finished"]
        )

    def to_json(self):
        return json.dumps(
            {
                "already_processed": list(self.already_processed),
                "finished": self.finished,
            }
        )


class DirectoryWatchRestrictionCoder(beam.coders.Coder):
    def encode(self, value: DirectoryWatchRestriction) -> bytes:
        return value.to_json().encode("utf-8")

    def decode(self, encoded: bytes) -> DirectoryWatchRestriction:
        return DirectoryWatchRestriction.from_json(encoded.decode("utf-8"))

    def is_deterministic(self) -> bool:
        return True


class DirectoryWatchRestrictionTracker(RestrictionTracker):
    def __init__(self, restriction: DirectoryWatchRestriction):
        self.restriction = restriction

    def current_restriction(self):
        return self.restriction

    def try_claim(self, new_file: str):
        if self.restriction.finished:
            return False
        self.restriction.add_new(new_file)
        return True

    def check_done(self):
        return

    def is_bounded(self):
        return False

    def try_split(self, fraction_of_remainder):
        return self.restriction.as_primary(), self.restriction.as_residual()


class DirectoryWatchRestrictionProvider(RestrictionProvider):
    def initial_restriction(self, element: str) -> DirectoryWatchRestriction:
        return DirectoryWatchRestriction(set(), False)

    def create_tracker(
        self, restriction: DirectoryWatchRestriction
    ) -> DirectoryWatchRestrictionTracker:
        return DirectoryWatchRestrictionTracker(restriction)

    def restriction_size(self, element: str, restriction: DirectoryWatchRestriction):
        return restriction.size()

    def restriction_coder(self):
        return DirectoryWatchRestrictionCoder()


class DirectoryWatchWatermarkEstimatorProvider(WatermarkEstimatorProvider):
    def initial_estimator_state(self, element, restriction):
        return MIN_TIMESTAMP

    def create_watermark_estimator(self, watermark: Timestamp):
        return ManualWatermarkEstimator(watermark)

    def estimator_state_coder(self):
        return beam.coders.TimestampCoder()


class DirectoryWatchFn(beam.DoFn):
    POLL_TIMEOUT = 10

    @beam.DoFn.unbounded_per_element()
    def process(
        self,
        element: str,
        tracker: RestrictionTrackerView = beam.DoFn.RestrictionParam(
            DirectoryWatchRestrictionProvider()
        ),
        watermark_estimater: WatermarkEstimatorProvider = beam.DoFn.WatermarkEstimatorParam(
            DirectoryWatchWatermarkEstimatorProvider()
        ),
    ) -> typing.Iterable[MyFile]:
        new_files = self._get_new_files_if_any(element, tracker)
        if self._process_new_files(tracker, watermark_estimater, new_files):
            # return [new_file[0] for new_file in new_files]
            for new_file in new_files:
                yield new_file[0]
        else:
            return
        tracker.defer_remainder(Duration.of(self.POLL_TIMEOUT))

    def _get_new_files_if_any(
        self, element: str, tracker: DirectoryWatchRestrictionTracker
    ) -> typing.List[typing.Tuple[MyFile, Timestamp]]:
        new_files = []
        for file in os.listdir(element):
            if (
                os.path.isfile(os.path.join(element, file))
                and file not in tracker.current_restriction().already_processed
            ):
                num_lines = sum(1 for _ in open(os.path.join(element, file)))
                new_file = MyFile(file, 0, num_lines)
                print(new_file)
                new_files.append(
                    (
                        new_file,
                        Timestamp.of(os.path.getmtime(os.path.join(element, file))),
                    )
                )
        return new_files

    def _process_new_files(
        self,
        tracker: DirectoryWatchRestrictionTracker,
        watermark_estimater: ManualWatermarkEstimator,
        new_files: typing.List[typing.Tuple[MyFile, Timestamp]],
    ):
        max_instance = watermark_estimater.current_watermark()
        for new_file in new_files:
            if tracker.try_claim(new_file[0].name) is False:
                watermark_estimater.set_watermark(max_instance)
                return False
            if max_instance < new_file[1]:
                max_instance = new_file[1]
        watermark_estimater.set_watermark(max_instance)
        return max_instance < MAX_TIMESTAMP
