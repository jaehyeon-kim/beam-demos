import sys
import unittest
from concurrent import futures

import grpc
import service_pb2
import service_pb2_grpc
import server


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


class TestService(unittest.TestCase):
    server_class = server.RcpServiceServicer
    port = 550051

    def setUp(self):
        self.server = grpc.server(futures.ThreadPoolExecutor())
        service_pb2_grpc.add_RcpServiceServicer_to_server(
            self.server_class(), self.server
        )
        self.server.add_insecure_port(f"[::]:{self.port}")
        self.server.start()

    def tearDown(self):
        self.server.stop(None)

    def test_resolve(self):
        with grpc.insecure_channel(f"[::]:{self.port}") as channel:
            stub = service_pb2_grpc.RcpServiceStub(channel)
            request = service_pb2.Request(input="Hello")
            response = stub.resolve(request)
        self.assertEqual(5, response.output)

    def test_resolve_batch(self):
        with grpc.insecure_channel(f"[::]:{self.port}") as channel:
            stub = service_pb2_grpc.RcpServiceStub(channel)
            request_list = service_pb2.RequestList()
            request_list.request.extend(
                [
                    service_pb2.Request(input=item)
                    for item in "Beautiful is better than ugly".split()
                ]
            )
            response = stub.resolveBatch(request_list)
        self.assertEqual([9, 2, 6, 4, 4], [r.output for r in response.response])


if __name__ == "__main__":
    main(out=None)
