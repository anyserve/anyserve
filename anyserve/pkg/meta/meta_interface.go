package meta

import (
	"context"
	"fmt"
	"strings"

	"github.com/anyserve/anyserve/pkg/proto"
	"github.com/anyserve/anyserve/pkg/utils"
	"go.uber.org/zap"
)

var logger = utils.GetLogger("meta")

type Format struct {
	Name string
	UUID string
}

type Backend func(driver, addr string) (Meta, error)

var metaBackends = make(map[string]Backend)

func Register(name string, register Backend) {
	metaBackends[name] = register
}

type Meta interface {

	// Init the meta data to backend
	Init(format *Format, force bool) error

	// GetFormat returns current format
	GetFormat() Format

	// Load the meta data from the backend
	Load() (*Format, error)

	QueueInferRequest(ctx context.Context, proto *proto.InferRequest, requestId string) error
	PopInferRequest(ctx context.Context, metadata map[string]string) (<-chan *proto.FetchInferResponse, error)
	// TODO: Implement
	// QueueInferRequestStream(ctx context.Context, inferRequestChan <-chan *proto.InferRequest, requestId string) <-chan error

	QueueSendResponseStream(ctx context.Context, sendResponseRequest *proto.SendResponseRequest) error
	PopInferResponse(ctx context.Context, requestId string) (<-chan *proto.InferCore, error)
}

func NewMeta(metaURI string) (Meta, error) {
	var err error

	p := strings.Index(metaURI, "://")
	if p < 0 {
		logger.Error("invalid uri", zap.String("uri", metaURI))
		return nil, fmt.Errorf("invalid uri: %s", metaURI)
	}

	driver := metaURI[:p]
	addr := metaURI[p+3:]

	initFunc, ok := metaBackends[driver]
	if !ok {
		return nil, fmt.Errorf("invalid driver: %s", driver)
	}

	meta, err := initFunc(driver, addr)
	if err != nil {
		return nil, err
	}

	return meta, nil
}
