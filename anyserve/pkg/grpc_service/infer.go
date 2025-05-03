package grpc_service

import (
	"context"
	"errors"
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
	// inject timestamp to request metadata for basic schedule
	if _, ok := req.Infer.Metadata[config.INFER_METADATA_TIMESTAMP]; !ok {
		req.Infer.Metadata[config.INFER_METADATA_TIMESTAMP] = strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	err := s.meta.QueueInferRequest(ctx, req, requestID)

	if err != nil {
		_logger.Error("Failed to queue inference request", zap.Error(err))
		_ = stream.Send(&proto.InferResponse{
			RequestId: requestID,
		})
		return err
	}

	// send ACK to client
	ack := &proto.InferCore{
		Metadata: map[string]string{
			config.RESPONSE_METADATA_TYPE: config.RESPONSE_METADATA_TYPE_VALUE_ACK,
		},
	}
	err = stream.Send(&proto.InferResponse{
		RequestId: requestID,
		Response:  ack,
	})
	if err != nil {
		_logger.Error("Failed to send ACK", zap.Error(err))
		return err
	}

	responseChan, err := s.meta.PopInferResponse(ctx, requestID)
	if err != nil {
		_logger.Error("Failed to pop inference response", zap.Error(err))
		return err
	}

	for response := range responseChan {
		switch response.Metadata[config.RESPONSE_METADATA_TYPE] {
		case config.RESPONSE_METADATA_TYPE_VALUE_FINISH:
			return nil
		case config.RESPONSE_METADATA_TYPE_VALUE_FAILED:
			_logger.Error("Inference response error", zap.String("error", string(response.Content)))
			return errors.New("inference response error")
		case config.RESPONSE_METADATA_TYPE_VALUE_PROCESSING:
			if err := stream.Send(&proto.InferResponse{
				RequestId: requestID,
				Response:  response,
			}); err != nil {
				_logger.Error("Failed to send inference response", zap.Error(err))
				return err
			}
		default:
			_logger.Error("Unknown inference response type", zap.String("type", response.Metadata[config.RESPONSE_METADATA_TYPE]))
			return errors.New("unknown inference response type")
		}
	}
	return nil
}
