package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/anyserve/anyserve/internal/version"
	"github.com/anyserve/anyserve/pkg/config"
	"github.com/anyserve/anyserve/pkg/grpc_server"
	"github.com/anyserve/anyserve/pkg/grpc_service"
	"github.com/anyserve/anyserve/pkg/logger"
	"github.com/anyserve/anyserve/pkg/proto"
	"github.com/anyserve/anyserve/pkg/server"
	"github.com/anyserve/anyserve/pkg/storage"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var (
	configFile  string
	showVersion bool
)

func init() {
	flag.StringVar(&configFile, "c", "config.yaml", "config file path")
	flag.BoolVar(&showVersion, "version", false, "show version information and exit")
}

func main() {
	flag.Parse()

	if showVersion {
		fmt.Printf("Anyserve Version: %s\n", version.Version)
		fmt.Printf("Git Commit: %s\n", version.GitCommit)
		fmt.Printf("Build Date: %s\n", version.BuildDate)
		fmt.Printf("Go Version: %s\n", version.GoVersion)
		os.Exit(0)
	}

	fmt.Println("config file:", configFile)

	app := fx.New(
		fx.Supply(configFile),
		config.Module,
		logger.Module,
		server.Module,
		grpc_server.Module,
		grpc_service.Module,
		storage.Module,
		fx.Invoke(func(
			cfg *config.Config,
			logger *zap.Logger,
			httpServer *server.Server,
			grpcServer *grpc_server.Server,
			inferenceService proto.GRPCInferenceServiceServer,
			embedNATS *storage.EmbedNATS,
			lc fx.Lifecycle,
		) {
			logger.Info("Initializing anyserve...")

			// Initialize HTTP server
			httpServer.RegisterHandlers()
			httpServer.Start(lc)

			// Initialize gRPC server
			grpcServer.RegisterServices(inferenceService)
			grpcServer.Start(lc)

			// Initialize embedded NATS server
			embedNATS.Start(lc)
		}),
	)

	app.Run()
}
