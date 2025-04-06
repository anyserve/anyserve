package cmd

import (
	"context"

	"github.com/anyserve/anyserve/pkg/config"
	"github.com/anyserve/anyserve/pkg/grpc_server"
	"github.com/anyserve/anyserve/pkg/grpc_service"
	"github.com/anyserve/anyserve/pkg/http_server"
	"github.com/anyserve/anyserve/pkg/utils"
	"github.com/urfave/cli/v3"
	"go.uber.org/fx"
)

var logger = utils.GetLogger("cmd")

func cmdServe() *cli.Command {
	return &cli.Command{
		Name:      "serve",
		Action:    serveFunc,
		Usage:     "Start anyserve server",
		ArgsUsage: "META-URL",
		Flags:     expandFlags(serveFlags(), httpServerFlags(), grpcServerFlags()),
	}
}

func serveFunc(ctx context.Context, cmd *cli.Command) error {
	defer logger.Sync()

	setup(cmd)

	grpcConfig := &config.GRPCConfig{
		Host:       cmd.String("grpc.host"),
		Port:       cmd.Int("grpc.port"),
		TLSEnabled: cmd.Bool("grpc.tls_enabled"),
		CertFile:   cmd.String("grpc.cert_file"),
		KeyFile:    cmd.String("grpc.key_file"),
	}

	httpConfig := &config.HTTPConfig{
		Host: cmd.String("http.host"),
		Port: cmd.Int("http.port"),
	}

	app := fx.New(
		fx.Supply(grpcConfig, httpConfig),

		fx.Provide(grpc_service.NewInferenceService),
		fx.Provide(http_server.NewServer),
		fx.Provide(grpc_server.NewServer),

		fx.Invoke(func(httpServer *http_server.Server) {}),
		fx.Invoke(func(grpcServer *grpc_server.Server) {}),
	)
	app.Run()

	return nil
}

func serveFlags() []cli.Flag {
	return []cli.Flag{}
}

func httpServerFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "http.host",
			Usage: "HTTP server listening address",
			Value: "0.0.0.0",
		},
		&cli.IntFlag{
			Name:  "http.port",
			Usage: "HTTP server listening port",
			Value: 8848,
		},
	}
}

func grpcServerFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "grpc.host",
			Usage: "gRPC server listening address",
			Value: "0.0.0.0",
		},
		&cli.IntFlag{
			Name:  "grpc.port",
			Usage: "gRPC server listening port",
			Value: 50052,
		},
		&cli.BoolFlag{
			Name:  "grpc.tls_enabled",
			Usage: "Enable TLS for gRPC server",
			Value: false,
		},
		&cli.StringFlag{
			Name:  "grpc.cert_file",
			Usage: "TLS certificate file path for gRPC server",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "grpc.key_file",
			Usage: "TLS private key file path for gRPC server",
			Value: "",
		},
	}
}
