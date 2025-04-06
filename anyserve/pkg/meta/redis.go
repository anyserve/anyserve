package meta

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

type redisMeta struct {
	*baseMeta

	rdb    redis.UniversalClient
	prefix string
}

// var _ Meta = &redisMeta{}
var _ engine = &redisMeta{}

func (m *redisMeta) Name() string {
	return "redis"
}

func (m *redisMeta) Shutdown() error {
	return m.rdb.Close()
}

func NewRedisMeta(url string) (*redisMeta, error) {
	var rdb redis.UniversalClient

	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL: %s: %w", url, err)
	}

	c := redis.NewClient(opts)
	rdb = c

	return &redisMeta{
		rdb:    rdb,
		prefix: "",
	}, nil
}

func (m *redisMeta) doInit(format *Format, force bool) error {
	// ctx := context.Background()
	// body, err := m.rdb.Get(ctx, m.setting()).Bytes()
	// if err != nil && err != redis.Nil {
	// 	return fmt.Errorf("failed to get setting: %w", err)
	// }
	return nil
}

func (m *redisMeta) setting() string {
	return m.prefix + "setting"
}
