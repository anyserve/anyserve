package grpc_service

import (
	"io"

	"github.com/anyserve/anyserve/pkg/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// SendResponse implements the SendResponse RPC method
func (s *InferenceService) SendResponse(stream proto.GRPCInferenceService_SendResponseServer) error {
	for {
		sendResponseRequest, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}
		s.meta.QueueSendResponseStream(stream.Context(), sendResponseRequest)
	}
}
