package grpc_server

import (
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"time"

	"github.com/anyserve/anyserve/pkg/config"
	"github.com/anyserve/anyserve/pkg/proto"
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

// Server represents the gRPC server
type Server struct {
	logger *zap.Logger
	config *config.Config
	server *grpc.Server
}

// customRecoveryHandler handles panics in gRPC handlers
func customRecoveryHandler(logger *zap.Logger) grpc_recovery.RecoveryHandlerFunc {
	return func(p interface{}) error {
		logger.Error("gRPC handler panic recovered",
			zap.Any("panic", p),
			zap.String("stack", string(debug.Stack())))
		return status.Errorf(codes.Internal, "Internal server error")
	}
}

// NewServer creates a new gRPC server instance
func NewServer(logger *zap.Logger, cfg *config.Config) *Server {
	// Setup recovery options
	recoveryOpts := []grpc_recovery.Option{
		grpc_recovery.WithRecoveryHandler(customRecoveryHandler(logger)),
	}

	// Define server options for better performance and monitoring
	opts := []grpc.ServerOption{
		// Connection management - optimized keepalive settings
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
			loggerInterceptor(logger),
			grpc_recovery.UnaryServerInterceptor(recoveryOpts...),
			grpc_prometheus.UnaryServerInterceptor,
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_recovery.StreamServerInterceptor(recoveryOpts...),
			grpc_prometheus.StreamServerInterceptor,
		)),
	}

	// Add TLS if configured
	if cfg.Server.GRPC.TLSEnabled && cfg.Server.GRPC.CertFile != "" && cfg.Server.GRPC.KeyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(cfg.Server.GRPC.CertFile, cfg.Server.GRPC.KeyFile)
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
		logger: logger,
		config: cfg,
		server: grpcServer,
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

// RegisterServices registers all the services with the gRPC server
func (s *Server) RegisterServices(inferenceService proto.GRPCInferenceServiceServer) {
	s.logger.Info("Registering gRPC services")
	proto.RegisterGRPCInferenceServiceServer(s.server, inferenceService)
}

// Start starts the gRPC server
func (s *Server) Start(lifecycle fx.Lifecycle) {
	port := s.config.Server.GRPC.Port
	addr := fmt.Sprintf("%s:%d", s.config.Server.Host, port)
	s.logger.Info("Starting gRPC server", zap.String("addr", addr))

	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			lis, err := net.Listen("tcp", addr)
			if err != nil {
				s.logger.Error("Failed to listen", zap.Error(err))
				return err
			}

			go func() {
				if err := s.server.Serve(lis); err != nil && err != grpc.ErrServerStopped {
					s.logger.Error("Failed to start gRPC server", zap.Error(err))
				}
			}()

			s.logger.Info("gRPC server started successfully")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			s.logger.Info("Stopping gRPC server")
			// Add a timeout for graceful shutdown
			done := make(chan struct{})
			go func() {
				s.server.GracefulStop()
				close(done)
			}()

			// Wait for graceful shutdown or timeout
			shutdownTimeout := 15 * time.Second // Increased from 10 seconds
			select {
			case <-done:
				s.logger.Info("gRPC server stopped gracefully")
			case <-time.After(shutdownTimeout):
				s.logger.Warn("gRPC server shutdown timed out, forcing stop")
				s.server.Stop()
			}
			return nil
		},
	})
}

// Module provides the fx module for the gRPC server
var Module = fx.Provide(NewServer)
