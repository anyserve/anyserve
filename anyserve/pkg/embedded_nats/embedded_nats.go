package embedded_nats

import (
	"context"
	"time"

	"github.com/anyserve/anyserve/pkg/utils"
	"github.com/nats-io/nats-server/v2/server"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var logger = utils.GetLogger("embedded_nats")

// nats://embedded
type EmbeddedNATS struct {
	server *server.Server
}

func NewEmbeddedNATS() *EmbeddedNATS {
	return &EmbeddedNATS{}
}

func (s *EmbeddedNATS) Start(lifecycle fx.Lifecycle) {

	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			opts := server.Options{
				NoSigs:     true,
				ServerName: "anyserve_embedded_server",
				Port:       4222,
				JetStream:  true,
				StoreDir:   "/tmp/nats",
			}
			ns, err := server.NewServer(&opts)
			if err != nil {
				return err
			}

			s.server = ns

			go func() {
				s.server.ConfigureLogger()
				s.server.Start()

				startTimeout := 5 * time.Second
				if !ns.ReadyForConnections(startTimeout) {
					logger.Fatal("Embedded NATS server failed to start within timeout",
						zap.Duration("timeout", startTimeout))
				}

				logger.Info("Embedded NATS server started successfully")
				s.server.WaitForShutdown()
			}()

			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Stopping embedded NATS server")
			if s.server != nil {
				s.server.Shutdown()
			}
			logger.Info("Embedded NATS server stopped")
			return nil
		},
	})
}

var Module = fx.Provide(NewEmbeddedNATS)
