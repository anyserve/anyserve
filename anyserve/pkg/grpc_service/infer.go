package grpc_service

import (
	"context"
	"reflect"
	"strconv"
	"time"

	"github.com/anyserve/anyserve/pkg/proto"
	"github.com/anyserve/anyserve/pkg/utils"
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
		_ = stream.Send(&proto.InferResponse{
			RequestId: requestID,
			Status:    proto.InferResponse_ERROR.Enum(),
		})
		return err
	}

	_ = stream.Send(&proto.InferResponse{
		RequestId: requestID,
		Status:    proto.InferResponse_ACK.Enum(),
	})

	responseChan, err := s.meta.PopInferResponse(ctx, requestID)
	if err != nil {
		_logger.Error("Failed to pop inference response", zap.Error(err))
		return err
	}

	for response := range responseChan {
		logger.Info("Received inference type", zap.Any("type", reflect.TypeOf(*response)))
		outputBytes, err := utils.ToBytes(*response)
		if err != nil {
			_logger.Error("Failed to convert inference response to []byte", zap.Error(err))
			return err
		}
		if err := stream.Send(&proto.InferResponse{
			RequestId: requestID,
			Status:    proto.InferResponse_PROCESSING.Enum(),
			Output:    outputBytes,
		}); err != nil {
			_logger.Error("Failed to send inference response", zap.Error(err))
			return err
		}
	}

	_ = stream.Send(&proto.InferResponse{
		RequestId: requestID,
		Status:    proto.InferResponse_FINISH.Enum(),
	})

	_logger.Info("Inference request completed")

	return nil
}
