import os
import unittest
from concurrent import futures

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

import grpc
import service_pb2_grpc
import server

from rpc_pardo import RpcDoFn
from io_utils import tokenize


def read_file(filename: str, inputpath: str):
    with open(os.path.join(inputpath, filename), "r") as f:
        return f.readlines()


def compute_expected_output(lines: list):
    output = []
    for line in lines:
        words = [(w, len(w)) for w in tokenize(line)]
        output = output + words
    return output


class RpcParDooTest(unittest.TestCase):
    server_class = server.RpcServiceServicer
    port = 50051

    def setUp(self):
        self.server = grpc.server(futures.ThreadPoolExecutor())
        service_pb2_grpc.add_RpcServiceServicer_to_server(
            self.server_class(), self.server
        )
        self.server.add_insecure_port(f"[::]:{self.port}")
        self.server.start()

    def tearDown(self):
        self.server.stop(None)

    def test_pipeline(self):
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            PARENT_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
            lines = read_file("lorem.txt", os.path.join(PARENT_DIR, "inputs"))
            test_stream = TestStream(coder=coders.StrUtf8Coder()).with_output_types(str)
            for line in lines:
                test_stream.add_elements([line])
            test_stream.advance_watermark_to_infinity()

            output = (
                p
                | test_stream
                | "ExtractWords" >> beam.FlatMap(tokenize)
                | "RequestRPC" >> beam.ParDo(RpcDoFn())
            )

            EXPECTED_OUTPUT = compute_expected_output(lines)

            assert_that(output, equal_to(EXPECTED_OUTPUT))


if __name__ == "__main__":
    unittest.main()
