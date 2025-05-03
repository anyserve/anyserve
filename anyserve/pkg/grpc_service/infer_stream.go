package grpc_service

import (
	"fmt"
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
			_logger.Debug("Received infer request: EOF")
			return nil
		}
		if err != nil {
			_logger.Error("Received infer request error", zap.Error(err))
			return err
		}

		_logger.Debug(fmt.Sprintf("Received infer request: %v", req))

		if err := stream.Send(&proto.InferResponse{
			RequestId: requestID,
			Response:  &proto.InferCore{Content: []byte("processed stream output")},
		}); err != nil {
			_logger.Error("Failed to send infer response", zap.Error(err))
			return err
		}
	}
}
