package grpc_service

import (
	"io"

	"github.com/anyserve/anyserve/pkg/proto"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// InferStream implements the InferStream RPC method
func (s *InferenceService) InferStream(stream proto.GRPCInferenceService_InferStreamServer) error {
	requestID := uuid.New().String()

	_logger := logger.With(zap.String("request_id", requestID))
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		_logger.Info("Received InferStream request", zap.Any("request", req))

		if err := stream.Send(&proto.InferResponse{
			RequestId: requestID,
			Response:  &proto.ResponseCore{Output: []byte("processed stream output")},
		}); err != nil {
			return err
		}
	}
}
