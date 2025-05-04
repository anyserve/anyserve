package meta

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/anyserve/anyserve/pkg/config"
	"github.com/anyserve/anyserve/pkg/proto"
	"github.com/redis/go-redis/v9"
	protobufproto "google.golang.org/protobuf/proto"
)

// FT.CREATE tsIndex ON HASH PREFIX 1 "meta:" SCHEMA @timestamp NUMERIC SORTABLE @status TAG SORTABLE
// FT.SEARCH tsIndex '@\@status:{queued}' SORTBY @timestamp
// FT.DROPINDEX tsIndex

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
	opts.UnstableResp3 = true

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

func (m *redisMeta) key(key string) string {
	if m.prefix == "" {
		return key
	}
	return fmt.Sprintf("%s:%s", m.prefix, key)
}

func (m *redisMeta) unkey(key string) string {
	if m.prefix == "" {
		return key
	}
	return key[len(m.prefix)+1:]
}

func (m *redisMeta) metaKey(requestId string) string {
	return m.key("meta:" + requestId)
}

// Return requestId from metaKey
// meta:4d480a48-5ac1-4e61-9275-2dea44697ce3 -> 4d480a48-5ac1-4e61-9275-2dea44697ce3
func (m *redisMeta) unmetaKey(key string) string {
	metaKey := m.unkey(key)
	requestId := metaKey[len("meta:"):]
	return requestId
}

func (m *redisMeta) responseKey(requestId string) string {
	return m.key("response:" + requestId)
}

func (m *redisMeta) doInit(format *Format, force bool) error {
	ctx := context.Background()

	data, err := json.MarshalIndent(format, "", "")
	if err != nil {
		return fmt.Errorf("json: %s", err)
	}

	if err = m.rdb.Set(ctx, m.key("setting"), data, 0).Err(); err != nil {
		return err
	}

	infoCmd := redis.NewCmd(ctx, "FT.INFO", "tsIndex")
	err = m.rdb.Process(ctx, infoCmd)
	if err == nil {
		logger.Info("Redis search index 'tsIndex' already exists")
		return nil
	}

	if !strings.Contains(err.Error(), "Unknown index name") {
		return fmt.Errorf("failed to check search index: %w", err)
	}

	indexCmd := redis.NewCmd(ctx, "FT.CREATE", "tsIndex", "ON", "HASH", "PREFIX", "1", "meta:", "SCHEMA",
		config.METADATA_TIMESTAMP, "NUMERIC", "SORTABLE",
		config.INFER_METADATA_STATUS, "TAG", "SORTABLE")

	err = m.rdb.Process(ctx, indexCmd)
	if err != nil {
		return fmt.Errorf("failed to create search index: %w", err)
	}

	logger.Info("Created Redis search index 'tsIndex'")
	return nil
}

func (m *redisMeta) doLoad() ([]byte, error) {
	body, err := m.rdb.Get(context.Background(), m.key("setting")).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	return body, err
}

func (m *redisMeta) doSetRequest(ctx context.Context, requestId string, input []byte) error {
	return m.rdb.Set(ctx, m.key(requestId), input, 0).Err()
}

func (m *redisMeta) doPushRequestQueue(ctx context.Context, requestId string, metadata map[string]string) error {
	return m.rdb.HSet(ctx, m.metaKey(requestId), metadata).Err()
}

func (m *redisMeta) doSetInferRequest(ctx context.Context, requestId string, metadata map[string]string) error {
	var values []interface{}
	for field, value := range metadata {
		values = append(values, field, value)
	}
	return m.rdb.HMSet(ctx, m.metaKey(requestId), values...).Err()
}

func (m *redisMeta) doDeleteRequest(ctx context.Context, requestId string) error {
	pipe := m.rdb.Pipeline()
	pipe.Del(ctx, m.metaKey(requestId))
	pipe.Del(ctx, m.key(requestId))
	pipe.Del(ctx, m.responseKey(requestId))
	_, err := pipe.Exec(ctx)
	return err
}

func (m *redisMeta) doPopRequestQueue(ctx context.Context, metadata map[string]string) ([]string, error) {

	searchResult, err := m.rdb.FTSearchWithArgs(ctx, "tsIndex", "@\\@status:{queued}", &redis.FTSearchOptions{
		SortBy: []redis.FTSearchSortBy{
			{
				FieldName: config.METADATA_TIMESTAMP,
				Asc:       true,
			},
		},
		DialectVersion: 3,
		LimitOffset:    0,
		Limit:          1,
	}).RawResult()

	if err != nil {
		logger.Sugar().Errorf("Redis search error: %v", err)
		return []string{}, err
	}
	if searchResult == nil {
		return []string{}, nil
	}
	_searchResult := searchResult.(map[any]any)
	if _searchResult == nil {
		return []string{}, nil
	}
	_allDocs := _searchResult["results"].([]any)
	if len(_allDocs) == 0 {
		return []string{}, nil
	}

	requestId := _allDocs[0].(map[any]any)["id"].(string)
	requestId = m.unmetaKey(requestId)

	logger.Debug(fmt.Sprintf("Pop %s from request queue", requestId))
	return []string{requestId}, nil
}

func (m *redisMeta) doGetRequest(ctx context.Context, requestId string) ([]byte, error) {
	logger.Debug(fmt.Sprintf("Get %s from request queue", requestId))
	return m.rdb.Get(ctx, m.key(requestId)).Bytes()
}

func (m *redisMeta) doPushResponseQueue(ctx context.Context, requestId string, response *proto.InferCore) error {
	data, err := protobufproto.Marshal(response)
	if err != nil {
		return err
	}
	return m.rdb.RPush(ctx, m.responseKey(requestId), data).Err()
}

func (m *redisMeta) doPopResponseQueue(ctx context.Context, requestId string) (*proto.InferCore, error) {
	result, err := m.rdb.LPop(ctx, m.responseKey(requestId)).Result()
	if err != nil {
		return nil, err
	}
	var response proto.InferCore
	err = protobufproto.Unmarshal([]byte(result), &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (m *redisMeta) doResponseQueueExists(ctx context.Context, requestId string) (bool, error) {
	exists, err := m.rdb.Exists(ctx, m.responseKey(requestId)).Result()
	return exists > 0, err
}

func (m *redisMeta) doExistsInferRequest(ctx context.Context, requestId string) (bool, error) {
	metaExists, err := m.rdb.Exists(ctx, m.metaKey(requestId)).Result()
	if err != nil {
		return false, err
	}
	requestExists, err := m.rdb.Exists(ctx, m.key(requestId)).Result()
	if err != nil {
		return false, err
	}
	return metaExists > 0 && requestExists > 0, nil
}
