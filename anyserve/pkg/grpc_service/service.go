package grpc_service

import (
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// InferenceService implements the GRPCInferenceService defined in proto
type InferenceService struct {
	logger *zap.Logger
}

// NewInferenceService creates a new inference service instance
func NewInferenceService(logger *zap.Logger) *InferenceService {
	return &InferenceService{
		logger: logger,
	}
}

// Module provides the fx module for the gRPC service
var Module = fx.Provide(NewInferenceService)
