package config

const (
	QueueEngineEmbeddedNATS = "embedded_nats"
	QueueEngineNATS         = "nats"
	QueueEngineRedis        = "redis"
)

const METADATA_TIMESTAMP = "@timestamp"

const (
	INFER_METADATA_STATUS = "@status"

	INFER_METADATA_STATUS_VALUE_QUEUED                = "queued"
	INFER_METADATA_STATUS_VALUE_SCHEDULED             = "scheduled"
	INFER_METADATA_STATUS_VALUE_PROCESSING            = "processing"
	INFER_METADATA_STATUS_VALUE_COMPLETED             = "completed"
	INFER_METADATA_STATUS_VALUE_FAILED                = "failed"
	INFER_METADATA_STATUS_VALUE_CANCELLED             = "cancelled"
	INFER_METADATA_STATUS_VALUE_TIMEDOUT              = "timedout"
	INFER_METADATA_STATUS_VALUE_WAITING_FOR_RESOURCES = "waiting_for_resources"
)

const (
	RESPONSE_METADATA_TYPE = "@type"

	RESPONSE_METADATA_TYPE_VALUE_ACK        = "response.created"
	RESPONSE_METADATA_TYPE_VALUE_PROCESSING = "response.processing"
	RESPONSE_METADATA_TYPE_VALUE_FINISH     = "response.finished"
	RESPONSE_METADATA_TYPE_VALUE_FAILED     = "response.failed"
)
