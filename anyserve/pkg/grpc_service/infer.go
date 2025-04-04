package grpc_service

import (
	"context"

	"github.com/anyserve/anyserve/pkg/proto"
	"github.com/google/uuid"
)

// Infer implements the Infer RPC method
func (s *InferenceService) Infer(ctx context.Context, req *proto.InferRequest) (*proto.InferResponse, error) {
	requestID := uuid.New().String()
	return &proto.InferResponse{
		RequestId: requestID,
		Output:    []byte("processed output"),
	}, nil
}
