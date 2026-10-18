//! Domain failures, independent of any HTTP or gRPC representation.

use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    InvalidArgument,
    NotFound,
    Conflict,
    ResourceExhausted,
    /// The journal cannot accept durable work, or the runtime is shutting down.
    PersistenceUnavailable,
    /// A lease or generation is no longer current.
    StaleLease,
    /// The requested events were dropped; `oldest` is the first still readable.
    CursorExpired {
        oldest: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeError {
    pub kind: ErrorKind,
    message: String,
}

impl RuntimeError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn not_found(entity: &str, id: &str) -> Self {
        Self::new(
            ErrorKind::NotFound,
            format!("{entity} '{id}' does not exist"),
        )
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidArgument, message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, message)
    }

    pub fn exhausted(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::ResourceExhausted, message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::PersistenceUnavailable, message)
    }

    pub fn stale(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::StaleLease, message)
    }

    pub fn cursor_expired(oldest: u64) -> Self {
        Self::new(
            ErrorKind::CursorExpired { oldest },
            format!("cursor expired; oldest available sequence is {oldest}"),
        )
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn to_status(&self) -> tonic::Status {
        let code = match self.kind {
            ErrorKind::InvalidArgument => tonic::Code::InvalidArgument,
            ErrorKind::NotFound => tonic::Code::NotFound,
            ErrorKind::Conflict => tonic::Code::AlreadyExists,
            ErrorKind::ResourceExhausted => tonic::Code::ResourceExhausted,
            ErrorKind::PersistenceUnavailable => tonic::Code::Unavailable,
            ErrorKind::StaleLease => tonic::Code::FailedPrecondition,
            ErrorKind::CursorExpired { .. } => tonic::Code::OutOfRange,
        };
        tonic::Status::new(code, self.message.clone())
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for RuntimeError {}

impl From<RuntimeError> for tonic::Status {
    fn from(error: RuntimeError) -> Self {
        error.to_status()
    }
}
