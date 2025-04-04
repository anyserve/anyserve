package grpc_service

import (
	"io"

	"github.com/anyserve/anyserve/pkg/proto"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// InferStream implements the InferStream RPC method
func (s *InferenceService) InferStream(stream proto.GRPCInferenceService_InferStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		requestID := uuid.New().String()
		s.logger.Debug("Received InferStream request", zap.String("model_id", req.ModelId))

		if err := stream.Send(&proto.InferResponse{
			RequestId: requestID,
			Output:    []byte("processed stream output"),
		}); err != nil {
			return err
		}
	}
}
