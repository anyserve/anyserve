pub const DEFAULT_QUEUE: &str = "default";

pub const METADATA_TIMESTAMP: &str = "@timestamp";
pub const INFER_METADATA_STATUS: &str = "@status";
pub const INFER_METADATA_STATUS_VALUE_QUEUED: &str = "queued";
pub const INFER_METADATA_STATUS_VALUE_SCHEDULED: &str = "scheduled";
pub const INFER_METADATA_STATUS_VALUE_PROCESSING: &str = "processing";
pub const INFER_METADATA_STATUS_VALUE_COMPLETED: &str = "completed";
pub const INFER_METADATA_STATUS_VALUE_FAILED: &str = "failed";
pub const INFER_METADATA_STATUS_VALUE_CANCELLED: &str = "cancelled";
pub const INFER_METADATA_STATUS_VALUE_TIMEDOUT: &str = "timedout";
pub const INFER_METADATA_STATUS_VALUE_WAITING_FOR_RESOURCES: &str = "waiting_for_resources";

pub const RESPONSE_METADATA_TYPE: &str = "@type";
pub const RESPONSE_METADATA_TYPE_VALUE_ACK: &str = "response.created";
pub const RESPONSE_METADATA_TYPE_VALUE_PROCESSING: &str = "response.processing";
pub const RESPONSE_METADATA_TYPE_VALUE_FINISH: &str = "response.finished";
pub const RESPONSE_METADATA_TYPE_VALUE_FAILED: &str = "response.failed";
