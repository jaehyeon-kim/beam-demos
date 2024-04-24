import os
import argparse
import json
import re
import typing
import logging

import apache_beam as beam
from apache_beam.io import kafka
from apache_beam.utils.windowed_value import WindowedValue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def decode_message(kafka_kv: tuple):
    print(kafka_kv)
    return kafka_kv[1].decode("utf-8")


def tokenize(element: str):
    return re.findall(r"[A-Za-z\']+", element)


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

        uqe_vals = set([e.value for e in self.elements])
        request_list = service_pb2.RequestList()
        request_list.request.extend([service_pb2.Request(input=e) for e in uqe_vals])
        response = self.stub.resolveBatch(request_list)
        resolved = dict(zip(uqe_vals, [r.output for r in response.response]))

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


def run():
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument("--runner", default="FlinkRunner", help="Apache Beam runner")
    parser.add_argument(
        "--use_own",
        action="store_true",
        default="Flag to indicate whether to use an own local cluster",
    )
    parser.add_argument("--input", default="input-topic", help="Input topic")
    parser.add_argument(
        "--job_name",
        default=re.sub("_", "-", re.sub(".py$", "", os.path.basename(__file__))),
        help="Job name",
    )
    opts = parser.parse_args()
    print(opts)

    pipeline_opts = {
        "runner": opts.runner,
        "job_name": opts.job_name,
        "environment_type": "LOOPBACK",
        "streaming": True,
        "parallelism": 3,
        "experiments": [
            "use_deprecated_read"
        ],  ## https://github.com/apache/beam/issues/20979
        "checkpointing_interval": "60000",
    }
    if opts.use_own is True:
        pipeline_opts = {**pipeline_opts, **{"flink_master": "localhost:8081"}}
    print(pipeline_opts)
    options = PipelineOptions([], **pipeline_opts)
    # Required, else it will complain that when importing worker functions
    options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=options)
    (
        p
        | "Read from Kafka"
        >> kafka.ReadFromKafka(
            consumer_config={
                "bootstrap.servers": os.getenv(
                    "BOOTSTRAP_SERVERS",
                    "host.docker.internal:29092",
                ),
                "auto.offset.reset": "earliest",
                # "enable.auto.commit": "true",
                "group.id": opts.job_name,
            },
            topics=[opts.input],
        )
        | "Decode messages" >> beam.Map(decode_message)
        | "Extract words" >> beam.FlatMap(tokenize)
        | "Request RPC" >> beam.ParDo(BatchRpcDoFn())
        | "Create messages"
        >> beam.Map(create_message).with_output_types(typing.Tuple[bytes, bytes])
        | "Write to Kafka"
        >> kafka.WriteToKafka(
            producer_config={
                "bootstrap.servers": os.getenv(
                    "BOOTSTRAP_SERVERS",
                    "host.docker.internal:29092",
                )
            },
            topic=opts.job_name,
        )
    )

    logging.getLogger().setLevel(logging.WARN)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
