package grpc_service

import (
	"github.com/anyserve/anyserve/pkg/proto"
	"github.com/google/uuid"
)

// FetchInfer implements the FetchInfer RPC method
func (s *InferenceService) FetchInfer(req *proto.FetchInferRequest, stream proto.GRPCInferenceService_FetchInferServer) error {
	// Simple implementation to satisfy the interface
	// Replace with actual implementation as needed
	requestID := uuid.New().String()

	response := &proto.FetchInferResponse{
		RequestId: requestID,
		Input:     []byte("sample input"),
	}

	return stream.Send(response)
}
