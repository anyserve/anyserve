package grpc_service

import (
	"io"

	"github.com/anyserve/anyserve/pkg/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"
)

// SendResponse implements the SendResponse RPC method
func (s *InferenceService) SendResponse(stream proto.GRPCInferenceService_SendResponseServer) error {

	for {
		sendResponseRequest, err := stream.Recv()
		if err == io.EOF {
			logger.Debug("Received send response request: EOF")
			return stream.SendAndClose(&emptypb.Empty{})
		}

		if err != nil {
			logger.Error("Received send response request error", zap.Error(err))
			return err
		}
		err = s.meta.QueueSendResponseStream(stream.Context(), sendResponseRequest)
		if err != nil {
			logger.Error("Failed to queue send response request", zap.Error(err))
			return err
		}
	}

}
