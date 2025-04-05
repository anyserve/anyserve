package cmd

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/anyserve/anyserve/internal/version"
	"github.com/anyserve/anyserve/pkg/utils"

	"github.com/urfave/cli/v3"
)

func init() {
	cli.VersionFlag = &cli.BoolFlag{
		Name:    "version",
		Aliases: []string{"V"},
		Usage:   "print version information and exit",
	}
}

func Main(args []string) error {

	cmd := &cli.Command{
		Name:  "anyserve",
		Usage: "",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return nil
		},
		Copyright:       "Apache License 2.0",
		HideHelpCommand: true,
		Version:         version.VersionString(),
		Flags:           globalFlags(),
		Commands: []*cli.Command{
			cmdServe(),
		},
	}

	if err := cmd.Run(context.Background(), args); err != nil {
		log.Fatal(err)
	}
	return nil
}

// func main() {

// 	fmt.Println("config file:", configFile)

// 	app := fx.New(
// 		fx.Supply(configFile),
// 		config.Module,
// 		logger.Module,
// 		server.Module,
// 		grpc_server.Module,
// 		grpc_service.Module,
// 		embedded_nats.Module,
// 		fx.Invoke(func(
// 			cfg *config.Config,
// 			logger *zap.Logger,
// 			httpServer *server.Server,
// 			grpcServer *grpc_server.Server,
// 			inferenceService *grpc_service.InferenceService,
// 			embeddedNATS *embedded_nats.EmbeddedNATS,
// 			lc fx.Lifecycle,
// 		) {
// 			logger.Info("Initializing anyserve...")

// 			// Initialize HTTP server
// 			httpServer.RegisterHandlers()
// 			httpServer.Start(lc)

// 			// Initialize gRPC server
// 			grpcServer.RegisterServices(inferenceService)
// 			grpcServer.Start(lc)

// 			// Initialize embedded NATS server
// 			embeddedNATS.Start(lc)
// 		}),
// 	)

// 	app.Run()
// }

func setup(cmd *cli.Command, n int) {
	if cmd.NArg() < n {
		fmt.Printf("ERROR: This command requires at least %d arguments\n", n)
		fmt.Printf("USAGE:\n   anyserve %s [command options] %s\n", cmd.Name, cmd.ArgsUsage)
		os.Exit(1)
	}

	if cmd.Bool("verbose") {
		utils.SetLevel("debug")
	} else {
		utils.SetLevel(cmd.String("log-level"))
	}
}
