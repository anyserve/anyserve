package grpc_service

import (
	"context"
	"os"

	"github.com/anyserve/anyserve/internal/version"
	"github.com/anyserve/anyserve/pkg/proto"
)

// ServerLive implements the ServerLive RPC method
func (s *InferenceService) ServerLive(ctx context.Context, req *proto.ServerLiveRequest) (*proto.ServerLiveResponse, error) {
	return &proto.ServerLiveResponse{
		Live:     true,
		Version:  getVersion(),
		Hostname: getHostname(),
	}, nil
}

func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

func getVersion() string {
	return version.Version
}
