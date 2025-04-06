package grpc_service

import (
	"github.com/anyserve/anyserve/pkg/utils"
)

var logger = utils.GetLogger("grpc_service")

// InferenceService implements the GRPCInferenceService defined in proto
type InferenceService struct {
}

// NewInferenceService creates a new inference service instance
func NewInferenceService() *InferenceService {
	return &InferenceService{}
}
