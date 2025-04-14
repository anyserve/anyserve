package grpc_service

import (
	"github.com/anyserve/anyserve/pkg/proto"
)

// FetchInfer implements the FetchInfer RPC method
func (s *InferenceService) FetchInfer(req *proto.FetchInferRequest, stream proto.GRPCInferenceService_FetchInferServer) error {
	ctx := stream.Context()

	responseChan, err := s.meta.PopInferRequest(ctx, req.Metadata)
	if err != nil {
		return err
	}

	for response := range responseChan {
		logger.Sugar().Debugf("FetchInfer response: %v", response)
		if err := stream.Send(response); err != nil {
			return err
		}
	}

	return nil
}
