package storage

import (
	"context"

	"github.com/nats-io/nats-server/v2/server"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type EmbedNATS struct {
	logger *zap.Logger
	server *server.Server
}

func NewEmbedNATS(logger *zap.Logger) *EmbedNATS {
	return &EmbedNATS{
		logger: logger,
	}
}

func (s *EmbedNATS) Start(lifecycle fx.Lifecycle) {
	s.logger.Info("Initializing embedded NATS server")

	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			opts := server.Options{
				NoSigs: true,
			}
			ns, err := server.NewServer(&opts)
			if err != nil {
				s.logger.Error("Failed to create NATS server", zap.Error(err))
				return err
			}
			s.server = ns

			go func() {
				s.logger.Info("Starting embedded NATS server")
				ns.Start()
				ns.WaitForShutdown()
			}()

			s.logger.Info("Embedded NATS server started successfully")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			s.logger.Info("Stopping embedded NATS server")
			if s.server != nil {
				s.server.Shutdown()
			}
			s.logger.Info("Embedded NATS server stopped")
			return nil
		},
	})
}

var Module = fx.Provide(NewEmbedNATS)
