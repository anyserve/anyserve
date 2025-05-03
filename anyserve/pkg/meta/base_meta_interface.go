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
	logger := zap.L().With(zap.String("request_id", requestId))
	var err error
	err = m.e.doSetRequest(ctx, requestId, proto.Infer.Content)
	if err != nil {
		logger.Error("SetRequest", zap.Error(err))
		return err
	}
	err = m.e.doPushRequestQueue(ctx, requestId, proto.Infer.Metadata)
	if err != nil {
		logger.Error("PushRequestQueue", zap.Error(err))
		return err
	}
	return nil
}
func (m *baseMeta) PopInferRequest(ctx context.Context, metadata map[string]string) (<-chan *proto.FetchInferResponse, error) {
	inferRequestChan := make(chan *proto.FetchInferResponse)

	go func() {
		defer close(inferRequestChan)
		requestIds, err := m.e.doPopRequestQueue(ctx, metadata)
		if err != nil {
			logger.Error("PopRequestQueue", zap.Error(err))
			return
		}

		for _, requestId := range requestIds {
			request, err := m.e.doGetRequest(ctx, requestId)
			if err != nil {
				logger.Error("GetRequest", zap.Error(err))
				return
			}
			logger.Sugar().Debugf("PopInferRequest request: %v", request)
			inferRequestChan <- &proto.FetchInferResponse{
				RequestId: requestId,
				Infer:     &proto.InferCore{Content: request, Metadata: metadata},
			}
		}
	}()

	return inferRequestChan, nil
}

func (m *baseMeta) QueueSendResponseStream(ctx context.Context, sendResponseRequest *proto.SendResponseRequest) error {
	err := m.e.doPushResponseQueue(ctx, sendResponseRequest.RequestId, sendResponseRequest.Response)
	if err != nil {
		return err
	}
	return nil
}

func (m *baseMeta) PopInferResponse(ctx context.Context, requestId string) (<-chan *proto.InferCore, error) {
	inferResponseChan := make(chan *proto.InferCore)
	go func() {
		defer close(inferResponseChan)
		for {
			exists, err := m.e.doResponseQueueExists(ctx, requestId)
			if err != nil {
				logger.Error("QueueExists", zap.Error(err))
				return
			}
			if !exists {
				continue
			}
			response, err := m.e.doPopResponseQueue(ctx, requestId)
			if err != nil {
				logger.Error("PopInferResponse", zap.Error(err))
				return
			}
			inferResponseChan <- response
		}
	}()
	return inferResponseChan, nil
}
