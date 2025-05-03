package grpc_service

import (
	"fmt"

	"github.com/anyserve/anyserve/pkg/proto"
	"go.uber.org/zap"
)

func (s *InferenceService) FetchInfer(req *proto.FetchInferRequest, stream proto.GRPCInferenceService_FetchInferServer) error {
	ctx := stream.Context()

	responseChan, err := s.meta.PopInferRequest(ctx, req.Metadata)
	if err != nil {
		logger.Error("Failed to Pop Infer Request", zap.Error(err))
		return err
	}

	for response := range responseChan {
		logger.Debug(fmt.Sprintf("Fetch Infer Request: %v", response))
		if err := stream.Send(response); err != nil {
			logger.Error("Failed to Send Fetch Infer Request", zap.Error(err))
			return err
		}
	}

	return nil
}
