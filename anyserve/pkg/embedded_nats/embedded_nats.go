package embedded_nats

import (
	"context"
	"time"

	"github.com/anyserve/anyserve/pkg/config"
	"github.com/nats-io/nats-server/v2/server"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type EmbeddedNATS struct {
	logger *zap.Logger
	cfg    *config.Config

	server *server.Server
}

func NewEmbeddedNATS(logger *zap.Logger, cfg *config.Config) *EmbeddedNATS {
	return &EmbeddedNATS{
		logger: logger,
		cfg:    cfg,
	}
}

func (s *EmbeddedNATS) Start(lifecycle fx.Lifecycle) {
	if s.cfg.Queue.Engine != config.QueueEngineEmbeddedNATS {
		s.logger.Info("Embedded NATS server is not enabled")
		return
	}

	s.logger.Info("Initializing embedded NATS server")

	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			opts := server.Options{
				NoSigs:     true,
				ServerName: "anyserve_embedded_server",
				Port:       s.cfg.EmbeddedNATS.Port,
				JetStream:  true,
				StoreDir:   s.cfg.EmbeddedNATS.StoreDir,
			}
			ns, err := server.NewServer(&opts)
			if err != nil {
				s.logger.Error("Failed to create embedded NATS server", zap.Error(err))
				return err
			}
			s.server = ns

			go func() {
				s.logger.Info("Starting embedded NATS server")
				s.server.ConfigureLogger()
				s.server.Start()

				startTimeout := 5 * time.Second
				if !ns.ReadyForConnections(startTimeout) {
					s.logger.Fatal("Embedded NATS server failed to start within timeout",
						zap.Duration("timeout", startTimeout))
				}

				s.logger.Info("Embedded NATS server started successfully")
				s.server.WaitForShutdown()
			}()

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

var Module = fx.Provide(NewEmbeddedNATS)
