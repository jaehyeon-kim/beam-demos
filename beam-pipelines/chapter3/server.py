import os
from concurrent import futures

import grpc
import service_pb2
import service_pb2_grpc


class RcpServiceServicer(service_pb2_grpc.RcpServiceServicer):
    def resolve(self, request, context):
        print(f"resolve Request Made: input - {request.input}")
        response = service_pb2.Response(output=len(request.input))
        return response

    def resolveBatch(self, request, context):
        print("resolveBatch Request Made:")
        print(f"\tInputs - {', '.join([r.input for r in request.request])}")
        response = service_pb2.ResponseList()
        response.response.extend(
            [service_pb2.Response(output=len(r.input)) for r in request.request]
        )
        return response


def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    service_pb2_grpc.add_RcpServiceServicer_to_server(RcpServiceServicer(), server)
    server.add_insecure_port(os.getenv("INSECURE_PORT", "0.0.0.0:50051"))
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
