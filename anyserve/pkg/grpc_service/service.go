package grpc_service

import (
	"github.com/anyserve/anyserve/pkg/meta"
	"github.com/anyserve/anyserve/pkg/utils"
)

var logger = utils.GetLogger("grpc_service")

// InferenceService implements the GRPCInferenceService defined in proto
type InferenceService struct {
	meta meta.Meta
}

// NewInferenceService creates a new inference service instance
func NewInferenceService(meta *meta.Meta) *InferenceService {
	return &InferenceService{
		meta: *meta,
	}
}
