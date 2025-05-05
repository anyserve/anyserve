package meta

import (
	"context"

	"github.com/anyserve/anyserve/pkg/proto"
)

// engine define the operations of the meta engine
type engine interface {
	doInit(format *Format, force bool) error
	doLoad() ([]byte, error)

	doSetRequest(ctx context.Context, requestId string, input []byte) error
	doPushRequestQueue(ctx context.Context, requestId string, metadata map[string]string) error
	doDeleteRequest(ctx context.Context, requestId string) error
	doSetInferRequest(ctx context.Context, requestId string, metadata map[string]string) error
	doExistsInferRequest(ctx context.Context, requestId string) (bool, error)
	doPopRequestQueue(ctx context.Context, metadata map[string]string) ([]string, error)
	doGetRequest(ctx context.Context, requestId string) ([]byte, error)

	doPushResponseQueue(ctx context.Context, requestId string, response *proto.InferCore) error
	doPopResponseQueue(ctx context.Context, requestId string) (*proto.InferCore, error)

	doResponseQueueExists(ctx context.Context, requestId string) (bool, error)

	doQueueExists(ctx context.Context, queueName string) (bool, error)
	doListQueues(ctx context.Context) ([]Queue, error)
	doCreateQueue(ctx context.Context, queue Queue) error
	doCreateQueueIndex(ctx context.Context, queue Queue) error
	doDeleteQueue(ctx context.Context, queueName string) error
	doDeleteQueueIndex(ctx context.Context, queueName string) error
	doDeleteQueueData(ctx context.Context, queueName string) error
}
