use amqp_client_rust::errors::{AppError as RuAppError, AppErrorType, RequeueError as RuRequeueError};
use pyo3::exceptions::PyException;
use pyo3::{PyErr, Python};
use std::fmt::{self, Display};

pyo3::create_exception!(amqp_rs, RequeueError, PyException);

/// Converts a Python exception raised inside a subscriber/RPC handler into a Rust
/// boxed error, preserving `RequeueError` so `amqp-client-rust` sends `Basic.Nack(requeue=true)`.
pub fn py_err_to_handler_error(err: PyErr) -> Box<dyn std::error::Error + Send + Sync> {
    let is_requeue = Python::try_attach(|py| err.is_instance_of::<RequeueError>(py)).unwrap_or(false);
    let msg = err.to_string();
    if is_requeue {
        Box::new(RuRequeueError::new(msg))
    } else {
        Box::new(std::io::Error::other(msg))
    }
}

impl From<RuAppError> for AppError {
    fn from(error: RuAppError) -> Self {
        Self {
            message: error.message,
            description: error.description,
            error_type: error.error_type,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppError {
    pub message: Option<String>,
    pub description: Option<String>,
    pub error_type: AppErrorType,
}

impl Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.message, &self.description) {
            (Some(msg), Some(desc)) => write!(f, "{}: {}", msg, desc),
            (Some(msg), None) => write!(f, "{}", msg),
            (None, Some(desc)) => write!(f, "{}", desc),
            (None, None) => write!(f, "{:?}", self.error_type),
        }
    }
}

impl From<AppError> for PyErr {
    fn from(error: AppError) -> Self {
        PyException::new_err(format!("{error}"))
    }
}
