import time
import grpc
import anyserve.grpc_service_pb2 as pb2
import anyserve.grpc_service_pb2_grpc as pb2_grpc


def send_request(stub: pb2_grpc.GRPCInferenceServiceStub):
    infer_core = pb2.InferCore(content=b"1 2 3", metadata={"model_name": "test"})

    request = pb2.InferRequest(infer=infer_core)

    for response in stub.Infer(request):
        print(f"Request ID: {response.request_id}")
        print(f"Response: {response}")


def consume_request(stub: pb2_grpc.GRPCInferenceServiceStub):
    request = pb2.FetchInferRequest(metadata={"model_name": "test"})

    content = b"1 2 3"
    request_id = None

    for response in stub.FetchInfer(request):
        request_id = response.request_id
        print(f"Request ID: {response.request_id}")
        print(f"Response: {response}")
        if response.HasField("infer"):
            content = response.infer.content
            content = content[::-1]

    if request_id is not None:

        def response_generator():
            time.sleep(1)
            yield pb2.SendResponseRequest(
                request_id=request_id,
                response=pb2.InferCore(
                    content=content, metadata={"@type": "response.processing"}
                ),
            )
            time.sleep(1)
            yield pb2.SendResponseRequest(
                request_id=request_id,
                response=pb2.InferCore(
                    content=content, metadata={"@type": "response.processing"}
                ),
            )
            time.sleep(1)
            yield pb2.SendResponseRequest(
                request_id=request_id,
                response=pb2.InferCore(metadata={"@type": "response.finished"}),
            )

        stub.SendResponse(response_generator())
    else:
        print("No responses received from FetchInfer")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="test client for anyserve")
    parser.add_argument(
        "--mode",
        type=str,
        default="consume",
        help="Mode of operation (produce, consume)",
    )
    args = parser.parse_args()

    mode = args.mode
    print(f"Running in {mode} mode")
    channel = grpc.insecure_channel("localhost:50052")
    stub = pb2_grpc.GRPCInferenceServiceStub(channel)
    if mode == "produce":
        send_request(stub)
    elif mode == "consume":
        consume_request(stub)
    else:
        raise ValueError(f"Invalid mode: {mode}")
