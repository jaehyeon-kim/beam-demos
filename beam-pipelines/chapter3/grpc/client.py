import os

import grpc
import service_pb2
import service_pb2_grpc


def run():
    with grpc.insecure_channel(
        os.getenv("INSECURE_PORT", "localhost:50051")
    ) as channel:
        stub = service_pb2_grpc.RcpServiceStub(channel)
        print("Select 1 for resolve")
        print("Select 2 for resolveBatch")
        rpc_call = input("Which rpc would you like to make: ")
        if rpc_call == "1":
            request = service_pb2.Request(input="Hello")
            response = stub.resolve(request)
            print("resolve Response Received:")
            print(response)
        if rpc_call == "2":
            request_list = service_pb2.RequestList()
            request_list.request.extend(
                [
                    service_pb2.Request(input=item)
                    for item in "Beautiful is better than ugly".split()
                ]
            )
            response = stub.resolveBatch(request_list)
            print("resolveBatch Reponse Received:")
            print(response)


if __name__ == "__main__":
    run()
