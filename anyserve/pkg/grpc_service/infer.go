package grpc_service

import (
	"context"
	"strconv"
	"time"

	"github.com/anyserve/anyserve/pkg/proto"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func (s *InferenceService) Infer(req *proto.InferRequest, stream proto.GRPCInferenceService_InferServer) error {

	requestID := uuid.New().String()
	ctx := context.Background()

	_logger := logger.With(zap.String("request_id", requestID))

	if req.Metadata == nil {
		req.Metadata = make(map[string]string)
	}

	// inject timestamp to request metadata
	if _, ok := req.Metadata["_timestamp"]; !ok {
		req.Metadata["_timestamp"] = strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	err := s.meta.QueueInferRequest(ctx, req, requestID)

	if err != nil {
		_logger.Error("Failed to queue inference request", zap.Error(err))
		stream.Send(&proto.InferResponse{
			RequestId: requestID,
			Status:    proto.InferResponse_ERROR.Enum(),
		})
		return err
	}

	stream.Send(&proto.InferResponse{
		RequestId: requestID,
		Status:    proto.InferResponse_ACK.Enum(),
	})

	inferResponseChan, err := s.meta.PopInferResponse(ctx, requestID)
	if err != nil {
		_logger.Error("Failed to pop inference response", zap.Error(err))
		return err
	}

	for resp := range inferResponseChan {
		if err := stream.Send(resp); err != nil {
			_logger.Error("Failed to send inference response", zap.Error(err))
			return err
		}
	}

	_logger.Info("Inference request completed")

	return nil
}
