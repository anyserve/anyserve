package meta

import (
	"github.com/nats-io/nats.go"
)

type NATSStorage struct {
	nc *nats.Conn
}

func NewNATSStorage(url string) *NATSStorage {
	// return &NATSStorage{
	// 	url: url,
	// }
	return nil
}

func (c *NATSStorage) Connect() error {

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
