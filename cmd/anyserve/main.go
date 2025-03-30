package main

import (
	"flag"
	"fmt"

	"github.com/anyserve/anyserve/pkg/config"
	"github.com/anyserve/anyserve/pkg/grpc_server"
	"github.com/anyserve/anyserve/pkg/grpc_service"
	"github.com/anyserve/anyserve/pkg/logger"
	"github.com/anyserve/anyserve/pkg/proto"
	"github.com/anyserve/anyserve/pkg/server"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var configFile string

func init() {
	flag.StringVar(&configFile, "c", "config.yaml", "config file path")
	flag.Parse()
}

func main() {
	fmt.Println("config file:", configFile)

	app := fx.New(
		fx.Supply(configFile),
		config.Module,
		logger.Module,
		server.Module,
		grpc_server.Module,
		grpc_service.Module,

		fx.Invoke(func(
			cfg *config.Config,
			logger *zap.Logger,
			httpServer *server.Server,
			grpcServer *grpc_server.Server,
			inferenceService proto.GRPCInferenceServiceServer,
			lc fx.Lifecycle,
		) {
			logger.Info("Initializing anyserve...")

			// Initialize HTTP server
			httpServer.RegisterHandlers()
			httpServer.Start(lc)

			// Initialize gRPC server
			grpcServer.RegisterServices(inferenceService)
			grpcServer.Start(lc)
		}),
	)

	app.Run()
}
