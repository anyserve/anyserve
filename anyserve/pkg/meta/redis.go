package meta

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type redisMeta struct {
	*baseMeta

	rdb    redis.UniversalClient
	prefix string
}

func init() {
	Register("redis", NewRedisMeta)
	Register("unix", NewRedisMeta)
}

var _ Meta = &redisMeta{}
var _ engine = &redisMeta{}

func (m *redisMeta) Name() string {
	return "redis"
}

func (m *redisMeta) Shutdown() error {
	return m.rdb.Close()
}

func NewRedisMeta(driver, addr string) (Meta, error) {
	uri := driver + "://" + addr

	var rdb redis.UniversalClient

	opts, err := redis.ParseURL(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL: %s: %w", addr, err)
	}

	c := redis.NewClient(opts)
	rdb = c

	m := &redisMeta{
		rdb:      rdb,
		prefix:   "",
		baseMeta: newBaseMeta(addr),
	}

	m.e = m

	return m, nil
}

func (m *redisMeta) doInit(format *Format, force bool) error {
	logger.Debug("doInit", zap.Any("format", format))
	ctx := context.Background()

	data, err := json.MarshalIndent(format, "", "")
	if err != nil {
		return fmt.Errorf("json: %s", err)
	}
	if err = m.rdb.Set(ctx, m.setting(), data, 0).Err(); err != nil {
		return err
	}
	return nil
}

func (m *redisMeta) doLoad() ([]byte, error) {
	body, err := m.rdb.Get(context.Background(), m.setting()).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	return body, err
}

func (m *redisMeta) setting() string {
	return m.prefix + "setting"
}
