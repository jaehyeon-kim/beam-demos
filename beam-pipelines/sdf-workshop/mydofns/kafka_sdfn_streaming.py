import logging
import sys
import time
import typing
from typing import Optional

import apache_beam as beam
from apache_beam import RestrictionProvider
from apache_beam.io.iobase import RestrictionTracker
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.watermark_estimators import WalltimeWatermarkEstimator
from apache_beam.runners.sdf_utils import RestrictionTrackerView
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.consumer.fetcher import ConsumerRecord

from mydofns.synthetic_sdfn_streaming import MyPartitionRestrictionTracker


class ReadPartitionsDoFn(beam.DoFn):
    def __init__(self, topic: str, bootstrap_server: str, *unused_args, **unused_kwargs):
        self._topic = topic
        self._bootstrap = bootstrap_server
        self._kafka_client: Optional[KafkaConsumer] = None
        super().__init__(*unused_args, **unused_kwargs)

    def setup(self):
        self._kafka_client = KafkaConsumer(self._topic,
                                           group_id='get-partitions-group',
                                           bootstrap_servers=[self._bootstrap])

    def process(self, unused_element: int, *args, **kwargs) -> typing.Iterable[int]:
        # TODO: return the ids of the partitions contained in this topic
        pass


class ProcessKafkaPartitionsDoFn(beam.DoFn, RestrictionProvider):
    POLL_TIMEOUT = 0.1

    def __init__(self, topic: str, bootstrap_server: str, *unused_args, **unused_kwargs):
        self._topic = topic
        self._bootstrap = bootstrap_server
        self._kafka_client: Optional[KafkaConsumer] = None
        super().__init__(*unused_args, **unused_kwargs)

    def _create_consumer(self, partition):
        tp = TopicPartition(topic=self._topic, partition=partition)
        self._kafka_client = KafkaConsumer(group_id='my-beam-consumer-group',
                                           bootstrap_servers=[self._bootstrap],
                                           auto_offset_reset='earliest',
                                           enable_auto_commit=False)
        self._kafka_client.assign([tp])

    @beam.DoFn.unbounded_per_element()
    def process(self,
                partition: int,
                tracker: RestrictionTrackerView = beam.DoFn.RestrictionParam(),
                wm_estim=beam.DoFn.WatermarkEstimatorParam(WalltimeWatermarkEstimator.default_provider()),
                **unused_kwargs) -> typing.Iterable[str]:
        if self._kafka_client is None:
            self._create_consumer(partition)

        # TODO: Implement a while loop to poll Kafka and process any new message in the partition
        pass

    def create_tracker(self, restriction: OffsetRange) -> MyPartitionRestrictionTracker:
        # TODO: create the restriction tracker
        pass

    def initial_restriction(self, partition: int) -> OffsetRange:
        if self._kafka_client is None:
            self._create_consumer(partition)

        # TODO: Find the latest committed offset, and create a range from that to "infinity"
        pass

    def restriction_size(self, element: int, restriction: OffsetRange):
        # TODO: return the size of the current partition
        pass
