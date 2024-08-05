import os
import unittest
import typing
from concurrent import futures

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.options.pipeline_options import PipelineOptions

import grpc
import service_pb2_grpc
import server

from rpc_pardo_stateful import to_buckets, BatchRpcDoFnStateful
from io_utils import tokenize


class MyItem(typing.NamedTuple):
    word: str
    length: int


beam.coders.registry.register_coder(MyItem, beam.coders.RowCoder)


def read_file(filename: str, inputpath: str):
    with open(os.path.join(inputpath, filename), "r") as f:
        return f.readlines()


def compute_expected_output(lines: list):
    output = []
    for line in lines:
        words = [(w, len(w)) for w in tokenize(line)]
        output = output + words
    return output


class RcpParDooStatefulTest(unittest.TestCase):
    server_class = server.RcpServiceServicer
    port = 50051

    def setUp(self):
        self.server = grpc.server(futures.ThreadPoolExecutor())
        service_pb2_grpc.add_RcpServiceServicer_to_server(
            self.server_class(), self.server
        )
        self.server.add_insecure_port(f"[::]:{self.port}")
        self.server.start()

    def tearDown(self):
        self.server.stop(None)

    def test_pipeline(self):
        pipeline_opts = {"runner": "FlinkRunner", "parallelism": 1, "streaming": True}
        options = PipelineOptions([], **pipeline_opts)
        with TestPipeline(options=options) as p:
            PARENT_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
            lines = read_file("lorem-short.txt", os.path.join(PARENT_DIR, "inputs"))
            test_stream = TestStream(coder=coders.StrUtf8Coder()).with_output_types(str)
            for line in lines:
                test_stream.add_elements([line])
            test_stream.advance_watermark_to_infinity()

            output = (
                p
                | test_stream
                | "ExtractWords" >> beam.FlatMap(tokenize)
                | "ToBuckets"
                >> beam.Map(to_buckets).with_output_types(typing.Tuple[int, str])
                | "RequestRPC"
                >> beam.ParDo(BatchRpcDoFnStateful(batch_size=10, max_wait_secs=5))
            )

            EXPECTED_OUTPUT = compute_expected_output(lines)

            assert_that(output, equal_to(EXPECTED_OUTPUT))


if __name__ == "__main__":
    unittest.main()
