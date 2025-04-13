package meta

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/anyserve/anyserve/pkg/proto"
	"go.uber.org/zap"
)

// baseMeta is the base implementation of the Meta interface
type baseMeta struct {
	sync.Mutex
	addr   string
	format *Format

	e engine
}

func newBaseMeta(addr string) *baseMeta {
	return &baseMeta{
		addr: addr,
	}
}

func (m *baseMeta) Init(format *Format, force bool) error {
	return m.e.doInit(format, force)
}

func (m *baseMeta) GetFormat() Format {
	m.Lock()
	defer m.Unlock()
	return *m.format
}

func (m *baseMeta) Load() (*Format, error) {
	body, err := m.e.doLoad()
	if err == nil && len(body) == 0 {
		err = fmt.Errorf("backend is not formatted, please run `anyserve init ...` first")
	}
	if err != nil {
		return nil, err
	}
	var format = new(Format)
	if err = json.Unmarshal(body, format); err != nil {
		return nil, fmt.Errorf("json: %s", err)
	}
	m.Lock()
	m.format = format
	m.Unlock()
	return format, nil
}

func (m *baseMeta) QueueInferRequest(ctx context.Context, proto *proto.InferRequest, requestId string) error {
	var err error
	err = m.e.doSetRequest(ctx, requestId, proto.Input)
	if err != nil {
		return err
	}
	err = m.e.doPushRequestQueue(ctx, requestId, proto.Metadata)
	if err != nil {
		return err
	}
	return nil
}
func (m *baseMeta) PopInferRequest(ctx context.Context, metadata map[string]string) (<-chan *proto.FetchInferRequest, error) {
	inferRequestChan := make(chan *proto.FetchInferRequest)
	go func() {
		defer close(inferRequestChan)
	}()
	return inferRequestChan, nil
}

func (m *baseMeta) PopInferResponse(ctx context.Context, requestId string) (<-chan *proto.InferResponse, error) {
	inferResponseChan := make(chan *proto.InferResponse)
	go func() {
		defer close(inferResponseChan)
		for {
			exists, err := m.e.doExists(ctx, fmt.Sprintf("r:%s", requestId))
			if err != nil {
				logger.Error("QueueExists", zap.Error(err))
				return
			}
			if !exists {
				continue
			}
			response, err := m.e.doPopResponseQueue(ctx, fmt.Sprintf("r:%s", requestId))
			if err != nil {
				logger.Error("PopInferResponse", zap.Error(err))
				return
			}
			inferResponseChan <- response.(*proto.InferResponse)
		}
	}()
	return inferResponseChan, nil
}
