package meta

import (
	"sync"

	"github.com/anyserve/anyserve/pkg/config"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type NATSStorage struct {
	logger *zap.Logger
	cfg    *config.Config

	nc *nats.Conn
	mu sync.Mutex
}

func NewNATSStorage(logger *zap.Logger, cfg *config.Config) *NATSStorage {
	return &NATSStorage{
		logger: logger,
		cfg:    cfg,
	}
}

func (c *NATSStorage) Connect() error {
	if c.cfg.Queue.Engine == config.QueueEngineEmbeddedNATS {
		nc, err := nats.Connect("nats://localhost:4222")
		if err != nil {
			return err
		}
		c.nc = nc
	}
	return nil
}

func (c *NATSStorage) Disconnect() {
	c.nc.Close()
}

func (c *NATSStorage) Publish(subject string, data []byte) error {
	return c.nc.Publish(subject, data)
}

func (c *NATSStorage) Subscribe(subject string, cb nats.MsgHandler) (*nats.Subscription, error) {
	return c.nc.Subscribe(subject, cb)
}

func (c *NATSStorage) Client() *nats.Conn {
	return c.nc
}
