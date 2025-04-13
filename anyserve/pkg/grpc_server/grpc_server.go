package grpc_server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/anyserve/anyserve/pkg/config"
	"github.com/anyserve/anyserve/pkg/grpc_service"
	"github.com/anyserve/anyserve/pkg/proto"
	"github.com/anyserve/anyserve/pkg/utils"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

var logger = utils.GetLogger("grpc_server")

type Server struct {
	config           *config.GRPCConfig
	inferenceService *grpc_service.InferenceService

	grpcServer *grpc.Server
}

func NewServer(lc fx.Lifecycle, cfg *config.GRPCConfig, inferenceService *grpc_service.InferenceService) *Server {
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
			recoveryInterceptor(logger.Logger),
			grpc_prometheus.UnaryServerInterceptor,
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			recoveryInterceptorStream(logger.Logger),
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

	server := &Server{
		config:           cfg,
		grpcServer:       grpcServer,
		inferenceService: inferenceService,
	}

	proto.RegisterGRPCInferenceServiceServer(server.grpcServer, server.inferenceService)
	logger.Sugar().Debugf("Registered gRPC services")

	port := server.config.Port
	addr := fmt.Sprintf("%s:%d", server.config.Host, port)

	logger.Sugar().Debugf("Starting gRPC server on %s:%d", server.config.Host, server.config.Port)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			lis, err := net.Listen("tcp", addr)
			if err != nil {
				logger.Error("Failed to listen", zap.Error(err))
				return err
			}

			go func() {
				if err := server.grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
					logger.Error("Failed to start gRPC server", zap.Error(err))
				}
			}()

			logger.Sugar().Infof("gRPC server started on %s:%d", server.config.Host, server.config.Port)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Sugar().Info("Stopping gRPC server")
			done := make(chan struct{})
			go func() {
				server.grpcServer.GracefulStop()
				close(done)
			}()

			shutdownTimeout := 5 * time.Second
			select {
			case <-done:
				logger.Sugar().Info("gRPC server stopped gracefully")
			case <-time.After(shutdownTimeout):
				logger.Sugar().Warn("gRPC server shutdown timed out, forcing stop")
				server.grpcServer.Stop()
			}
			return nil
		},
	})
	return server
}
