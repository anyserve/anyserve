package grpc_service

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/anyserve/anyserve/pkg/config"
	"github.com/anyserve/anyserve/pkg/proto"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func (s *InferenceService) Infer(req *proto.InferRequest, stream proto.GRPCInferenceService_InferServer) error {

	requestID := uuid.New().String()
	ctx := context.Background()

	_logger := logger.With(zap.String("request_id", requestID))

	if req.Infer.Metadata == nil {
		req.Infer.Metadata = make(map[string]string)
	}

	req.Infer.Metadata[config.METADATA_TIMESTAMP] = strconv.FormatInt(time.Now().UnixNano(), 10)
	req.Infer.Metadata[config.INFER_METADATA_STATUS] = config.INFER_METADATA_STATUS_VALUE_QUEUED

	_logger.Debug(fmt.Sprintf("Infer %s queued", requestID))
	err := s.meta.QueueInferRequest(ctx, req, requestID)

	if err != nil {
		_logger.Error("Failed to queue infer request", zap.Error(err))
		_ = stream.Send(&proto.InferResponse{
			RequestId: requestID,
		})
		return err
	}

	err = stream.Send(&proto.InferResponse{
		RequestId: requestID,
		Response: &proto.InferCore{
			Metadata: map[string]string{
				config.RESPONSE_METADATA_TYPE: config.RESPONSE_METADATA_TYPE_VALUE_ACK,
			},
		},
	})

	if err != nil {
		_logger.Error("Failed to send ACK", zap.Error(err))
		return err
	}

	responseChan, err := s.meta.PopInferResponse(ctx, requestID)
	if err != nil {
		_logger.Error("Failed to pop infer response", zap.Error(err))
		return err
	}

	for response := range responseChan {
		switch response.Metadata[config.RESPONSE_METADATA_TYPE] {
		case config.RESPONSE_METADATA_TYPE_VALUE_FINISH:
			_logger.Debug(fmt.Sprintf("Infer % response finished: %v", response))
			if err := stream.Send(&proto.InferResponse{
				RequestId: requestID,
				Response:  response,
			}); err != nil {
				_logger.Error("Failed to send infer response", zap.Error(err))
				return err
			}
			return nil
		case config.RESPONSE_METADATA_TYPE_VALUE_FAILED:
			_logger.Error("Infer response error", zap.String("error", string(response.Content)))
			return errors.New("inference response error")
		case config.RESPONSE_METADATA_TYPE_VALUE_PROCESSING:
			if err := stream.Send(&proto.InferResponse{
				RequestId: requestID,
				Response:  response,
			}); err != nil {
				_logger.Error("Failed to send infer response", zap.Error(err))
				return err
			}
		default:
			_logger.Error("Unknown infer response type", zap.String("type", response.Metadata[config.RESPONSE_METADATA_TYPE]))
			return errors.New("unknown infer response type")
		}
	}
	return nil
}
