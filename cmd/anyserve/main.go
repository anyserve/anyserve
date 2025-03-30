package main

import (
	"github.com/anyserve/anyserve/pkg/logger"
	"github.com/anyserve/anyserve/pkg/server"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

func main() {
	fx.New(
		logger.Module,
		server.Module,

		fx.Invoke(func(
			logger *zap.Logger,
			server *server.Server,
		) {
			logger.Info("Initializing anyserve...")
			server.RegisterHandlers()
		}),
	).Run()
}
