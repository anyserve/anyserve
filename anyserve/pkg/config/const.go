package config

const (
	QueueEngineEmbeddedNATS = "embedded_nats"
	QueueEngineNATS         = "nats"
	QueueEngineRedis        = "redis"
)

const (
	INFER_METADATA_TIMESTAMP = "@timestamp"

	RESPONSE_METADATA_TIMESTAMP = "@timestamp"
	RESPONSE_METADATA_TYPE      = "@type"
)

const (
	RESPONSE_METADATA_TYPE_VALUE_ACK        = "response.created"
	RESPONSE_METADATA_TYPE_VALUE_PROCESSING = "response.processing"
	RESPONSE_METADATA_TYPE_VALUE_FINISH     = "response.finished"
	RESPONSE_METADATA_TYPE_VALUE_FAILED     = "response.failed"
)
