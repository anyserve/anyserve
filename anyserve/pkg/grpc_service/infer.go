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
	ctx := context.Background()
	var requestID string
	requestExists := false
	var err error

	if req.RequestId != nil {
		requestID = *req.RequestId
		requestExists, err = s.meta.ExistsInferRequest(ctx, requestID)
		if err != nil {
			logger.With(zap.String("request_id", requestID)).Error("Failed to check if infer request exists", zap.Error(err))
			return err
		}
	} else {
		requestID = uuid.New().String()
		if req.Infer.Metadata == nil {
			req.Infer.Metadata = make(map[string]string)
		}
		req.Infer.Metadata[config.METADATA_TIMESTAMP] = strconv.FormatInt(time.Now().UnixNano(), 10)
		req.Infer.Metadata[config.INFER_METADATA_STATUS] = config.INFER_METADATA_STATUS_VALUE_QUEUED

		logger.With(zap.String("request_id", requestID)).Debug(fmt.Sprintf("Infer %s queued", requestID))
		err := s.meta.QueueInferRequest(ctx, req, requestID)

		if err != nil {
			logger.With(zap.String("request_id", requestID)).Error("Failed to queue infer request", zap.Error(err))
			return err
		}
		requestExists = true
	}

	if !requestExists {
		logger.With(zap.String("request_id", requestID)).Error("Infer request not found")
		return errors.New("infer request not found")
	}

	// Pop infer response from queue
	responseChan, err := s.meta.PopInferResponse(ctx, requestID)
	if err != nil {
		logger.With(zap.String("request_id", requestID)).Error("Failed to pop infer response", zap.Error(err))
		return err
	}

	for response := range responseChan {
		if err := stream.Send(&proto.InferResponse{
			RequestId: requestID,
			Response:  response,
		}); err != nil {
			logger.With(zap.String("request_id", requestID)).Error("Failed to send infer response", zap.Error(err))
			return err
		}
		if response.Metadata[config.RESPONSE_METADATA_TYPE] == config.RESPONSE_METADATA_TYPE_VALUE_FINISH {
			return nil
		}
	}
	return nil
}
