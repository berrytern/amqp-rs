use std::fmt::{self, Display};
use napi::{Error, Status};
use amqp_client_rust::errors::{AppError as RuAppError, AppErrorType};

#[derive(Debug, Clone)]
pub struct AppError {
    pub message: Option<String>,
    pub description: Option<String>,
    pub error_type: AppErrorType,
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

impl Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, r"{{ {:?}, {:?} }}", self.message, self.description)
    }
}

impl From<AppError> for Error {
    fn from(error: AppError) -> Self {
        Error::new(Status::GenericFailure, format!("{error}"))
    }
}