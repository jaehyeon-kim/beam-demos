import os
import argparse
import json
import re
import typing
import logging

import apache_beam as beam
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec,
    BagStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.windowed_value import WindowedValue
from apache_beam.utils.timestamp import Timestamp, Duration
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from io_utils import ReadWordsFromKafka, WriteOutputsToKafka


class ValueCoder(beam.coders.Coder):
    def encode(self, e: typing.Tuple[int, str]):
        """Encode to bytes with a trace that coder was used."""
        return f"x:{e[0]}:{e[1]}".encode("utf-8")

    def decode(self, b: bytes):
        s = b.decode("utf-8")
        assert s[0:2] == "x:"
        return tuple(s.split(":")[1:])

    def is_deterministic(self):
        return True


beam.coders.registry.register_coder(typing.Tuple[int, str], ValueCoder)


def create_message(element: typing.Tuple[str, int]):
    msg = json.dumps({"word": element[0], "length": element[1]})
    print(msg)
    return element[0].encode("utf-8"), msg.encode("utf-8")


def to_buckets(e: str):
    return (ord(e[0]) % 10, e)


class BatchRpcDoFnStateful(beam.DoFn):
    channel = None
    stub = None
    hostname = "localhost"
    port = "50051"

    BATCH_SIZE = ReadModifyWriteStateSpec("batch_size", beam.coders.VarIntCoder())
    BATCH = BagStateSpec(
        "batch",
        beam.coders.WindowedValueCoder(wrapped_value_coder=ValueCoder()),
    )
    FLUSH_TIMER = TimerSpec("flush_timer", TimeDomain.REAL_TIME)
    EOW_TIMER = TimerSpec("end_of_time", TimeDomain.WATERMARK)

    def __init__(self, batch_size: int, max_wait_secs: int):
        self.batch_size = batch_size
        self.max_wait_secs = max_wait_secs

    def setup(self):
        import grpc
        import service_pb2_grpc

        self.channel: grpc.Channel = grpc.insecure_channel(
            f"{self.hostname}:{self.port}"
        )
        self.stub = service_pb2_grpc.RpcServiceStub(self.channel)

    def teardown(self):
        if self.channel is not None:
            self.channel.close()

    def process(
        self,
        element: typing.Tuple[int, str],
        batch=beam.DoFn.StateParam(BATCH),
        batch_size=beam.DoFn.StateParam(BATCH_SIZE),
        flush_timer=beam.DoFn.TimerParam(FLUSH_TIMER),
        eow_timer=beam.DoFn.TimerParam(EOW_TIMER),
        timestamp=beam.DoFn.TimestampParam,
        win_param=beam.DoFn.WindowParam,
    ):
        current_size = batch_size.read() or 0
        if current_size == 0:
            flush_timer.set(Timestamp.now() + Duration(seconds=self.max_wait_secs))
            eow_timer.set(GlobalWindow().max_timestamp())
        current_size += 1
        batch_size.write(current_size)
        batch.add(
            WindowedValue(value=element, timestamp=timestamp, windows=(win_param,))
        )
        if current_size >= self.batch_size:
            return self.flush(batch, batch_size, flush_timer, eow_timer)

    @on_timer(FLUSH_TIMER)
    def on_flush_timer(
        self,
        batch=beam.DoFn.StateParam(BATCH),
        batch_size=beam.DoFn.StateParam(BATCH_SIZE),
        flush_timer=beam.DoFn.TimerParam(FLUSH_TIMER),
        eow_timer=beam.DoFn.TimerParam(EOW_TIMER),
    ):
        return self.flush(batch, batch_size, flush_timer, eow_timer)

    @on_timer(EOW_TIMER)
    def on_eow_timer(
        self,
        batch=beam.DoFn.StateParam(BATCH),
        batch_size=beam.DoFn.StateParam(BATCH_SIZE),
        flush_timer=beam.DoFn.TimerParam(FLUSH_TIMER),
        eow_timer=beam.DoFn.TimerParam(EOW_TIMER),
    ):
        return self.flush(batch, batch_size, flush_timer, eow_timer)

    def flush(
        self,
        batch=beam.DoFn.StateParam(BATCH),
        batch_size=beam.DoFn.StateParam(BATCH_SIZE),
        flush_timer=beam.DoFn.TimerParam(FLUSH_TIMER),
        eow_timer=beam.DoFn.TimerParam(EOW_TIMER),
    ):
        import service_pb2

        elements = list(batch.read())

        batch.clear()
        batch_size.clear()
        if flush_timer:
            flush_timer.clear()
        if eow_timer:
            eow_timer.clear()

        unqiue_values = set([e.value for e in elements])
        request_list = service_pb2.RequestList()
        request_list.request.extend(
            [service_pb2.Request(input=e[1]) for e in unqiue_values]
        )
        response = self.stub.resolveBatch(request_list)
        resolved = dict(
            zip([e[1] for e in unqiue_values], [r.output for r in response.response])
        )

        return [
            WindowedValue(
                value=(e.value[1], resolved[e.value[1]]),
                timestamp=e.timestamp,
                windows=e.windows,
            )
            for e in elements
        ]


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--bootstrap_servers",
        default="host.docker.internal:29092",
        help="Kafka bootstrap server addresses",
    )
    parser.add_argument("--input_topic", default="input-topic", help="Input topic")
    parser.add_argument(
        "--output_topic",
        default=re.sub("_", "-", re.sub(".py$", "", os.path.basename(__file__))),
        help="Output topic",
    )
    parser.add_argument(
        "--batch_size", type=int, default=10, help="Batch size to process"
    )
    parser.add_argument(
        "--max_wait_secs",
        type=int,
        default=4,
        help="Maximum wait seconds before processing",
    )
    parser.add_argument(
        "--deprecated_read",
        action="store_true",
        default="Whether to use a deprecated read. See https://github.com/apache/beam/issues/20979",
    )
    parser.set_defaults(deprecated_read=False)

    known_args, pipeline_args = parser.parse_known_args(argv)

    # # We use the save_main_session option because one or more DoFn's in this
    # # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    print(f"known args - {known_args}")
    print(f"pipeline options - {pipeline_options.display_data()}")

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadInputsFromKafka"
            >> ReadWordsFromKafka(
                bootstrap_servers=known_args.bootstrap_servers,
                topics=[known_args.input_topic],
                group_id=f"{known_args.output_topic}-group",
                deprecated_read=known_args.deprecated_read,
            )
            | "ToBuckets"
            >> beam.Map(to_buckets).with_output_types(typing.Tuple[int, str])
            | "RequestRPC"
            >> beam.ParDo(
                BatchRpcDoFnStateful(
                    batch_size=known_args.batch_size,
                    max_wait_secs=known_args.max_wait_secs,
                )
            )
            | "CreateMessags"
            >> beam.Map(create_message).with_output_types(typing.Tuple[bytes, bytes])
            | "WriteOutputsToKafka"
            >> WriteOutputsToKafka(
                bootstrap_servers=known_args.bootstrap_servers,
                topic=known_args.output_topic,
                deprecated_read=known_args.deprecated_read,  # TO DO: remove as it applies only to ReadFromKafka
            )
        )

        logging.getLogger().setLevel(logging.WARN)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
