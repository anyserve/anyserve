buf:
	buf generate
	# gRPC python generated code has a bug, use sed to fix it, ugly but works
	# https://github.com/protocolbuffers/protobuf/issues/1491
	sed -i 's/import grpc_service_pb2 as/from . import grpc_service_pb2 as/' ${PWD}/clients/python/src/anyserve/grpc_service_pb2_grpc.py