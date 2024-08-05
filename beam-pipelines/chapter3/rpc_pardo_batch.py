import os
import argparse
import json
import re
import typing
import logging

import apache_beam as beam
from apache_beam.utils.windowed_value import WindowedValue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from io_utils import ReadWordsFromKafka, WriteOutputsToKafka


def create_message(element: typing.Tuple[str, int]):
    msg = json.dumps({"word": element[0], "length": element[1]})
    print(msg)
    return element[0].encode("utf-8"), msg.encode("utf-8")


class BatchRpcDoFn(beam.DoFn):
    channel = None
    stub = None
    elements: typing.List[WindowedValue] = None
    hostname = "localhost"
    port = "50051"

    def setup(self):
        import grpc
        import service_pb2_grpc

        self.channel: grpc.Channel = grpc.insecure_channel(
            f"{self.hostname}:{self.port}"
        )
        self.stub = service_pb2_grpc.RcpServiceStub(self.channel)

    def teardown(self):
        if self.channel is not None:
            self.channel.close()

    def start_bundle(self):
        self.elements = []

    def finish_bundle(self):
        import service_pb2

        unqiue_values = set([e.value for e in self.elements])
        request_list = service_pb2.RequestList()
        request_list.request.extend(
            [service_pb2.Request(input=e) for e in unqiue_values]
        )
        response = self.stub.resolveBatch(request_list)
        resolved = dict(zip(unqiue_values, [r.output for r in response.response]))

        return [
            WindowedValue(
                value=(e.value, resolved[e.value]),
                timestamp=e.timestamp,
                windows=e.windows,
            )
            for e in self.elements
        ]

    def process(
        self,
        element: str,
        timestamp=beam.DoFn.TimestampParam,
        win_param=beam.DoFn.WindowParam,
    ):
        self.elements.append(
            WindowedValue(value=element, timestamp=timestamp, windows=(win_param,))
        )


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
            | "RequestRPC" >> beam.ParDo(BatchRpcDoFn())
            | "CreateMessags"
            >> beam.Map(create_message).with_output_types(typing.Tuple[bytes, bytes])
            | "WriteOutputsToKafka"
            >> WriteOutputsToKafka(
                bootstrap_servers=known_args.bootstrap_servers,
                topic=known_args.output_topic,
                deprecated_read=known_args.deprecated_read,
            )
        )

        logging.getLogger().setLevel(logging.WARN)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
