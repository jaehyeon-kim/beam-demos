import sys
import unittest

from grpc import StatusCode
from grpc_testing import server_from_dictionary, strict_real_time
import service_pb2
from server import RpcServiceServicer


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


class TestService(unittest.TestCase):
    def __init__(self, methodName):
        super().__init__(methodName)

        rpc_servicer = RpcServiceServicer()
        servicers = {
            service_pb2.DESCRIPTOR.services_by_name["RpcService"]: rpc_servicer
        }
        self.test_server = server_from_dictionary(servicers, strict_real_time())

    def test_resolve(self):
        request = service_pb2.Request(input="Hello")
        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                service_pb2.DESCRIPTOR.services_by_name["RpcService"].methods_by_name[
                    "resolve"
                ]
            ),
            invocation_metadata={},
            request=request,
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        self.assertEqual(code, StatusCode.OK)
        self.assertEqual(5, response.output)

    def test_resolve_batch(self):
        request_list = service_pb2.RequestList()
        request_list.request.extend(
            [
                service_pb2.Request(input=item)
                for item in "Beautiful is better than ugly".split()
            ]
        )
        method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                service_pb2.DESCRIPTOR.services_by_name["RpcService"].methods_by_name[
                    "resolveBatch"
                ]
            ),
            invocation_metadata={},
            request=request_list,
            timeout=1,
        )

        response, metadata, code, details = method.termination()
        self.assertEqual(code, StatusCode.OK)
        self.assertEqual([9, 2, 6, 4, 4], [r.output for r in response.response])


if __name__ == "__main__":
    main(out=None)
