package main

import (
	"flag"
	"fmt"

	"github.com/anyserve/anyserve/pkg/config"
	"github.com/anyserve/anyserve/pkg/logger"
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

		fx.Invoke(func(
			cfg *config.Config,
			logger *zap.Logger,
			server *server.Server,
			lc fx.Lifecycle,
		) {
			logger.Info("Initializing anyserve...")

			server.RegisterHandlers()
			server.Start(lc)
		}),
	)

	app.Run()
}
