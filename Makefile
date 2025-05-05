.PHONY: buf build clean

buf:
	buf generate
	# gRPC python generated code has a bug, use sed to fix it, ugly but works
	# https://github.com/protocolbuffers/protobuf/issues/1491
	sed -i '' 's/import grpc_service_pb2 as/from . import grpc_service_pb2 as/' clients/python/src/anyserve/grpc_service_pb2_grpc.py

build:
	$(MAKE) -C anyserve build
	$(MAKE) -C clients/python build

clean:
	$(MAKE) -C anyserve clean
	$(MAKE) -C clients/python clean

doc:
	cd docs && pnpm install && pnpm run build