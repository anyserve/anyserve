package grpc_server

import (
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"time"

	"github.com/anyserve/anyserve/pkg/config"
	"github.com/anyserve/anyserve/pkg/grpc_service"
	"github.com/anyserve/anyserve/pkg/proto"
	"github.com/anyserve/anyserve/pkg/utils"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

var logger = utils.GetLogger("grpc_server")

type Server struct {
	config           *config.GRPCConfig
	inferenceService *grpc_service.InferenceService

	server *grpc.Server
}

func customRecoveryHandler(logger *zap.Logger) grpc_recovery.RecoveryHandlerFunc {
	return func(p interface{}) error {
		logger.Error("gRPC handler panic recovered",
			zap.Any("panic", p),
			zap.String("stack", string(debug.Stack())))
		return status.Errorf(codes.Internal, "Internal server error")
	}
}

func NewServer(cfg *config.GRPCConfig, inferenceService *grpc_service.InferenceService) *Server {
	recoveryOpts := []grpc_recovery.Option{
		grpc_recovery.WithRecoveryHandler(customRecoveryHandler(logger.Logger)),
	}

	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     2 * time.Minute,  // Increased from 1 minute
			MaxConnectionAge:      10 * time.Minute, // Increased from 5 minutes for longer-lived connections
			MaxConnectionAgeGrace: 30 * time.Second, // Increased grace period
			Time:                  15 * time.Second, // Slightly reduced probe frequency
			Timeout:               5 * time.Second,  // Reduced timeout for faster failure detection
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second, // Reduced minimum time between pings
			PermitWithoutStream: true,
		}),

		// Resource limits - optimized for common use cases
		grpc.MaxRecvMsgSize(8 * 1024 * 1024), // 8MB, increased from 4MB
		grpc.MaxSendMsgSize(8 * 1024 * 1024), // 8MB, increased from 4MB
		grpc.MaxConcurrentStreams(1000),      // Control concurrent streams

		// Chain multiple interceptors
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			loggerInterceptor(logger.Logger),
			grpc_recovery.UnaryServerInterceptor(recoveryOpts...),
			grpc_prometheus.UnaryServerInterceptor,
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_recovery.StreamServerInterceptor(recoveryOpts...),
			grpc_prometheus.StreamServerInterceptor,
		)),
	}

	// Add TLS if configured
	if cfg.TLSEnabled && cfg.CertFile != "" && cfg.KeyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			logger.Error("Failed to load TLS credentials", zap.Error(err))
		} else {
			opts = append(opts, grpc.Creds(creds))
			logger.Info("TLS enabled for gRPC server")
		}
	}

	grpcServer := grpc.NewServer(opts...)

	// Initialize Prometheus metrics
	grpc_prometheus.Register(grpcServer)

	return &Server{
		config:           cfg,
		server:           grpcServer,
		inferenceService: inferenceService,
	}
}

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

func (s *Server) Start(lifecycle fx.Lifecycle) {

	proto.RegisterGRPCInferenceServiceServer(s.server, s.inferenceService)
	logger.Sugar().Debugf("Registered gRPC services")

	port := s.config.Port
	addr := fmt.Sprintf("%s:%d", s.config.Host, port)

	logger.Sugar().Debugf("Starting gRPC server on %s:%d", s.config.Host, s.config.Port)

	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			lis, err := net.Listen("tcp", addr)
			if err != nil {
				logger.Error("Failed to listen", zap.Error(err))
				return err
			}

			go func() {
				if err := s.server.Serve(lis); err != nil && err != grpc.ErrServerStopped {
					logger.Error("Failed to start gRPC server", zap.Error(err))
				}
			}()

			logger.Sugar().Infof("gRPC server started on %s:%d", s.config.Host, s.config.Port)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Sugar().Info("Stopping gRPC server")
			done := make(chan struct{})
			go func() {
				s.server.GracefulStop()
				close(done)
			}()

			shutdownTimeout := 15 * time.Second
			select {
			case <-done:
				logger.Sugar().Info("gRPC server stopped gracefully")
			case <-time.After(shutdownTimeout):
				logger.Sugar().Warn("gRPC server shutdown timed out, forcing stop")
				s.server.Stop()
			}
			return nil
		},
	})
}
