package grpc_service

import (
	"github.com/anyserve/anyserve/pkg/meta"
	"github.com/anyserve/anyserve/pkg/utils"
)

var logger = utils.GetLogger("grpc_service")

type InferenceService struct {
	meta meta.Meta
}

func NewInferenceService(meta *meta.Meta) *InferenceService {
	return &InferenceService{
		meta: *meta,
	}
}
