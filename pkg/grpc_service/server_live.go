package grpc_service

import (
	"context"

	"github.com/anyserve/anyserve/pkg/proto"
)

// ServerLive implements the ServerLive RPC method
func (s *InferenceService) ServerLive(ctx context.Context, req *proto.ServerLiveRequest) (*proto.ServerLiveResponse, error) {
	s.logger.Info("ServerLive request received")
	return &proto.ServerLiveResponse{
		Live: true,
	}, nil
}
