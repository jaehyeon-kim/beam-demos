import time

import grpc
import service_pb2
import service_pb2_grpc


def get_client_stream_requests():
    while True:
        name = input("Please enter a name (or nothing to stop chatting):")
        if name == "":
            break
        hello_request = service_pb2.HelloRequest(greeting="Hello", name=name)
        yield hello_request
        time.sleep(1)


def run():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = service_pb2_grpc.RpcServiceStub(channel)
        print("1. Resolve - Unary")
        print("2. ResolveBatch - Unary")
        rpc_call = input("Which rpc would you like to make: ")
        if rpc_call == "1":
            element = input("Please enter a word: ")
            if not element:
                element = "Hello"
            request = service_pb2.Request(input=element)
            resolved = stub.resolve(request)
            print("Resolve response received: ")
            print(f"({element}, {resolved.output})")
        if rpc_call == "2":
            element = input("Please enter a text: ")
            if not element:
                element = "Beautiful is better than ugly"
            words = element.split(" ")
            request_list = service_pb2.RequestList()
            request_list.request.extend([service_pb2.Request(input=e) for e in words])
            response = stub.resolveBatch(request_list)
            resolved = [r.output for r in response.response]
            print("ResolveBatch response received: ")
            print(", ".join([f"({t[0]}, {t[1]})" for t in zip(words, resolved)]))


if __name__ == "__main__":
    run()
