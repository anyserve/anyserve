package meta

import "context"

// engine define the operations of the meta engine
type engine interface {
	doInit(format *Format, force bool) error
	doLoad() ([]byte, error)

	doSetRequest(ctx context.Context, requestId string, input []byte) error
	doPushRequestQueue(ctx context.Context, requestId string, metadata map[string]string) error

	doPopRequestQueue(ctx context.Context, metadata map[string]string) ([]string, error)
	doGetRequest(ctx context.Context, requestId string) ([]byte, error)

	doPushResponseQueue(ctx context.Context, requestId string, response any) error
	doPopResponseQueue(ctx context.Context, requestId string) (any, error)

	doExists(ctx context.Context, key string) (bool, error)
}
