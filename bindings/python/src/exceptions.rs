use amqp_client_rust::errors::{AppError as RuAppError, AppErrorType};
use pyo3::PyErr;
use pyo3::exceptions::PyException;
use std::fmt::{self, Display};

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
