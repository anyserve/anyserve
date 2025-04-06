package grpc_server

import (
	"context"
	"runtime/debug"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
)

// loggerInterceptor creates a gRPC interceptor for logging
func loggerInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		requestLogger := logger.With(zap.String("method", info.FullMethod))
		start := time.Now()

		// Add request logging
		requestLogger.Debug("gRPC request received")

		// Execute the handler
		resp, err := handler(ctx, req)

		// Log the request
		duration := time.Since(start)
		statusCode := codes.OK
		if err != nil {
			statusCode = status.Code(err)
		}

		logFunc := requestLogger.Info
		if statusCode != codes.OK {
			logFunc = requestLogger.Error
			logFunc("gRPC request failed",
				zap.Duration("duration", duration),
				zap.String("status", statusCode.String()),
				zap.Error(err),
			)
		} else {
			logFunc("gRPC request processed",
				zap.Duration("duration", duration),
				zap.String("status", statusCode.String()),
			)
		}

		return resp, err
	}
}

func recoveryInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	recoveryOpts := []grpc_recovery.Option{
		grpc_recovery.WithRecoveryHandler(customRecoveryHandler(logger)),
	}
	return grpc_recovery.UnaryServerInterceptor(
		recoveryOpts...,
	)
}

func recoveryInterceptorStream(logger *zap.Logger) grpc.StreamServerInterceptor {
	recoveryOpts := []grpc_recovery.Option{
		grpc_recovery.WithRecoveryHandler(customRecoveryHandler(logger)),
	}
	return grpc_recovery.StreamServerInterceptor(
		recoveryOpts...,
	)
}

func customRecoveryHandler(logger *zap.Logger) grpc_recovery.RecoveryHandlerFunc {
	return func(p interface{}) error {
		logger.Error("gRPC handler panic recovered",
			zap.Any("panic", p),
			zap.String("stack", string(debug.Stack())))
		return status.Errorf(codes.Internal, "Internal server error")
	}
}
