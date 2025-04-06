package meta

import (
	"fmt"
	"strings"

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
	// Implement in baseMeta
	Init(format *Format, force bool) error

	// GetFormat returns current format
	// Implement in baseMeta
	GetFormat() Format

	// Load the meta data from the backend
	// Implement in baseMeta
	Load() (*Format, error)

	// QueueInferRequest(ctx context.Context, proto *proto.InferRequest) error
	// QueueInferRequestStream(ctx context.Context, inferRequestChan <-chan *proto.InferRequest) <-chan error
	// QueueSendResponseStream(ctx context.Context, sendResponseRequestChan <-chan *proto.SendResponseRequest) <-chan error

	// PopInferRequest(ctx context.Context, modelId string) (<-chan *proto.FetchInferRequest, error)
	// PopInferResponse(ctx context.Context, requestId string) (<-chan *proto.InferResponse, error)
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
