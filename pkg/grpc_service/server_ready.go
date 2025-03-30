package grpc_service

import (
	"context"

	"github.com/anyserve/anyserve/pkg/proto"
)

// ServerReady implements the ServerReady RPC method
func (s *InferenceService) ServerReady(ctx context.Context, req *proto.ServerReadyRequest) (*proto.ServerReadyResponse, error) {
	return &proto.ServerReadyResponse{
		Ready: true,
	}, nil
}
