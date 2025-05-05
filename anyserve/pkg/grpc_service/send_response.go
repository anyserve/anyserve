package grpc_service

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/anyserve/anyserve/pkg/config"
	"github.com/anyserve/anyserve/pkg/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// SendResponse implements the SendResponse RPC method
func (s *InferenceService) SendResponse(stream proto.GRPCInferenceService_SendResponseServer) error {
	ctx := stream.Context()
	for {
		sendResponseRequest, err := stream.Recv()
		requestId := sendResponseRequest.RequestId
		response := sendResponseRequest.Response

		if err == io.EOF {
			logger.Debug(fmt.Sprintf("Received send response request: EOF %s", sendResponseRequest.RequestId))
			_ = s.setInferRequestStatus(ctx, requestId, config.INFER_METADATA_STATUS_VALUE_COMPLETED)
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			logger.Error(fmt.Sprintf("Received send response request error: %s", err))
			return err
		}

		switch response.Metadata[config.RESPONSE_METADATA_TYPE] {

		case config.RESPONSE_METADATA_TYPE_VALUE_ACK:
			logger.Debug(fmt.Sprintf("%s response.ack", requestId))
			_ = s.setInferRequestStatus(ctx, requestId, config.INFER_METADATA_STATUS_VALUE_SCHEDULED)
			if err := s.meta.QueueSendResponseStream(stream.Context(), sendResponseRequest); err != nil {
				logger.Error(fmt.Sprintf("Failed to queue send response request: %s", err))
				return err
			}
		case config.RESPONSE_METADATA_TYPE_VALUE_FINISH:
			logger.Debug(fmt.Sprintf("%s response.finished", requestId))
			_ = s.setInferRequestStatus(ctx, requestId, config.INFER_METADATA_STATUS_VALUE_COMPLETED)
			if err := s.meta.QueueSendResponseStream(stream.Context(), sendResponseRequest); err != nil {
				logger.Error(fmt.Sprintf("Failed to queue send response request: %s", err))
				return err
			}
			return stream.SendAndClose(&emptypb.Empty{})

		case config.RESPONSE_METADATA_TYPE_VALUE_FAILED:
			logger.Error(fmt.Sprintf("%s response.failed", requestId))
			_ = s.setInferRequestStatus(ctx, requestId, config.INFER_METADATA_STATUS_VALUE_FAILED)
			if err := s.meta.QueueSendResponseStream(stream.Context(), sendResponseRequest); err != nil {
				logger.Error(fmt.Sprintf("Failed to queue send response request: %s", err))
				return err
			}
			return errors.New("inference response error")

		case config.RESPONSE_METADATA_TYPE_VALUE_PROCESSING:
			logger.Debug(fmt.Sprintf("%s response.processing", requestId))
			_ = s.setInferRequestStatus(ctx, requestId, config.INFER_METADATA_STATUS_VALUE_PROCESSING)
			if err := s.meta.QueueSendResponseStream(stream.Context(), sendResponseRequest); err != nil {
				logger.Error(fmt.Sprintf("Failed to queue send response request: %s", err))
				return err
			}
		default:
			logger.Error(fmt.Sprintf("Unknown response type: %s", response.Metadata[config.RESPONSE_METADATA_TYPE]))
			return errors.New("unknown response type")
		}
	}
}

func (s *InferenceService) setInferRequestStatus(ctx context.Context, requestId string, status string) error {
	if err := s.meta.SetInferRequest(ctx, requestId, map[string]string{
		config.INFER_METADATA_STATUS: status,
	}); err != nil {
		logger.Error(fmt.Sprintf("Failed to set infer request to %s: %s", status, err))
		return err
	}
	return nil
}
