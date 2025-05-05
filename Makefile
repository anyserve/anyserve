buf:
	buf generate
	# gRPC python generated code has a bug, use sed to fix it, ugly but works
	# https://github.com/protocolbuffers/protobuf/issues/1491
	# macOS requires an empty string for -i parameter
	if [ "$(shell uname)" = "Darwin" ]; then \
		sed -i '' 's/import grpc_service_pb2 as/from . import grpc_service_pb2 as/' ${PWD}/clients/python/src/anyserve/grpc_service_pb2_grpc.py; \
	else \
		sed -i 's/import grpc_service_pb2 as/from . import grpc_service_pb2 as/' ${PWD}/clients/python/src/anyserve/grpc_service_pb2_grpc.py; \
	fi