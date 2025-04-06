package meta

import (
	"context"

	"github.com/anyserve/anyserve/pkg/proto"
)

type Format struct {
	Name string
	UUID string
}

type Meta interface {
	Init(format Format, force bool) error
	GetFormat() Format

	QueueInferRequest(ctx context.Context, proto *proto.InferRequest) error
	QueueInferRequestStream(ctx context.Context, inferRequestChan <-chan *proto.InferRequest) <-chan error
	QueueSendResponseStream(ctx context.Context, sendResponseRequestChan <-chan *proto.SendResponseRequest) <-chan error

	PopInferRequest(ctx context.Context, modelId string) (<-chan *proto.FetchInferRequest, error)
	PopInferResponse(ctx context.Context, requestId string) (<-chan *proto.InferResponse, error)
}

func NewMeta(metaUrl string) (Meta, error) {
	return nil, nil
}
