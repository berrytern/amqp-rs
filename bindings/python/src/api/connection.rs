use std::{pin::Pin, sync::Arc};
use amqp_client_rust::{
    amqprs::tls::{TlsAdaptor as RuTlsAdaptor}, 
    api::{
        eventbus::AsyncEventbusRabbitMQ as RuAsyncEventbusRabbitMQ,
        connection::AsyncConnection as RuAsyncConnection,
        utils::{
            ContentEncoding as RuContentEncoding,
            DeliveryMode as RuDeliveryMode,
            Message as RuMessage,
            Confirmations as RuPublishConfirmation,
            QueueOptions as RuQueueOptions
        },
    }, domain::config::{
        Config as RuConfig, ConfigOptions as RuConfigOptions, QoSConfig as RuQoSConfig,
    }
};
use crate::{exceptions::AppError, utils::{Config, ContentEncoding, DeliveryMode, Message, Payload, PublishConfirmations, QueueOptions}};
use pyo3::{
    exceptions::PyValueError, prelude::*, types::PyBytes
};

#[pyclass(skip_from_py_object)]
pub struct AsyncConnection {
    pub(crate) inner: Arc<RuAsyncConnection>,
}

#[pymethods]
impl AsyncConnection {
    #[new]
    pub fn new(config: Config, publish_confirmations: PublishConfirmations, auto_ack: bool, prefetch_count: Option<u16>) -> PyResult<Self> {
        let rt = pyo3_async_runtimes::tokio::get_runtime();

        let _guard = rt.enter();
        let connection = RuAsyncConnection::new(Arc::new(config.into()), publish_confirmations.into(), auto_ack, prefetch_count);
        Ok(Self {
            inner: Arc::new(connection),
        })
    }

    pub fn publish<'py>(
        slf: PyRef<'py, Self>,
        exchange_name: &str,
        routing_key: &str,
        body: Payload,
        content_type: &str,
        content_encoding: ContentEncoding,
        command_timeout: Option<u64>,
        delivery_mode: DeliveryMode,
        expiration: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let conn = Arc::clone(&slf.inner);
        let exchange_name = exchange_name.to_owned();
        let routing_key = routing_key.to_owned();
        let payload_bytes = match body {
            Payload::Bytes(b) => b.as_bytes().to_vec(),
            Payload::Str(s) => s.to_str()?.as_bytes().to_vec(),
        };
        let content_type = content_type.to_owned();
        let content_encoding = content_encoding.to_owned();
        let delivery_mode = delivery_mode.to_owned();
        let expiration = expiration.to_owned();
        let command_timeout = command_timeout.map(std::time::Duration::from_secs);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            conn.publish(&exchange_name, &routing_key, payload_bytes, &content_type, content_encoding.into(), command_timeout, delivery_mode.into(), expiration).await.map_err(|e| PyValueError::new_err(format!("Failed to update secret: {}", e)))
        })
        
    }

    pub fn subscribe<'py>(
        slf: PyRef<'py, Self>,
        handler: Py<PyAny>,
        routing_key: &str,
        exchange_name: &str,
        exchange_type: &str,
        queue_name: &str,
        process_timeout: Option<u64>,
        command_timeout: Option<u64>,
        queue_options: QueueOptions,
    ) -> PyResult<Bound<'py, PyAny>> {

        let locals = pyo3_async_runtimes::TaskLocals::with_running_loop(slf.py())?;
        let py = slf.py();
        let conn = Arc::clone(&slf.inner);
        let handler = Arc::new(handler);
        let routing_key = routing_key.to_owned();
        let exchange_name = exchange_name.to_owned();
        let exchange_type = exchange_type.to_owned();
        let queue_name = queue_name.to_owned();
        let process_timeout = process_timeout.map(std::time::Duration::from_secs);
        let command_timeout = command_timeout.map(std::time::Duration::from_secs);
        let queue_options: RuQueueOptions = QueueOptions::to_ru_options(queue_options)?;
        let handler = move |body| {
            let handler_clone = handler.clone();
            let locals_clone = locals.clone();
            async move {
                pyo3_async_runtimes::tokio::scope(locals_clone, async move {
                    let future_result = Python::attach(|py| -> PyResult<_> {
                        let bound_handler = handler_clone.bind(py);
                        let coro = bound_handler.call1((Message::from(body),))?;

                        pyo3_async_runtimes::tokio::into_future(coro)
                    });
                    match future_result {
                        Ok(py_future) => match py_future.await {
                            Ok(_) => Ok(()),
                            Err(e) => Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e.to_string(),
                            ))
                                as Box<dyn std::error::Error + Send + Sync>),
                        },
                        Err(e) => Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Failed to execute Python callback: {}", e),
                        ))
                            as Box<dyn std::error::Error + Send + Sync>),
                    }
                })
                .await
            }
        };
        let handler = Arc::new(move |data| {
            Box::pin(handler(data)) as Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>>
        });

        pyo3_async_runtimes::tokio::future_into_py(py, async move {

            match conn
                .subscribe(
                    handler,
                    &routing_key,
                    &exchange_name,
                    &exchange_type,
                    &queue_name,
                    process_timeout,
                    command_timeout,
                    queue_options,
                )
                .await
            {
                Ok(res) => Ok(res),
                Err(e) => Err(AppError::from(e).into()),
            }
        })
        
    }

    pub fn rpc_server<'py>(
        slf: PyRef<'py, Self>,
        handler: Py<PyAny>,
        routing_key: &str,
        exchange_name: &str,
        exchange_type: &str,
        queue_name: &str,
        process_timeout: Option<u64>,
        command_timeout: Option<u64>,
        queue_options: QueueOptions,
    ) -> PyResult<Bound<'py, PyAny>> {
        let locals = pyo3_async_runtimes::TaskLocals::with_running_loop(slf.py())?;
        let py = slf.py();
        let conn = Arc::clone(&slf.inner);
        let handler = Arc::new(handler);
        let routing_key = routing_key.to_owned();
        let exchange_name = exchange_name.to_owned();
        let exchange_type = exchange_type.to_owned();
        let queue_name = queue_name.to_owned();
        let process_timeout = process_timeout.map(std::time::Duration::from_secs);
        let command_timeout = command_timeout.map(std::time::Duration::from_secs);
        let queue_options: RuQueueOptions = QueueOptions::to_ru_options(queue_options)?;
        let handler = move |body| {
            let handler_clone = handler.clone();
            let locals_clone = locals.clone();
            async move {
                pyo3_async_runtimes::tokio::scope(locals_clone, async move {
                    let py_future_result = Python::attach(|py| -> PyResult<_> {
                        let bound_handler = handler_clone.bind(py);
                        let coro = bound_handler.call1((Message::from(body),))?;

                        // Now into_future will successfully find the asyncio loop!
                        pyo3_async_runtimes::tokio::into_future(coro)
                    });
                    match py_future_result {
                        Ok(py_future) => match py_future.await {
                            Ok(result) => {
                                Python::attach(|py| {
                                    if let Ok(message) = result.extract::<Message>(py) {
                                        return Ok(RuMessage::from(message));
                                    }
                                match result.cast_bound::<PyBytes>(py) {
                                    Ok(bytes) => Ok( RuMessage { body: bytes.as_bytes().into(), content_type: None }),
                                    Err(_) => Err(Box::new(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        "RPC handler must return bytes",
                                    ))
                                        as Box<dyn std::error::Error + Send + Sync>),
                                }
                            })},
                            Err(e) => Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e.to_string(),
                            ))
                                as Box<dyn std::error::Error + Send + Sync>),
                        },
                        Err(e) => Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Failed to execute Python callback: {}", e),
                        ))
                            as Box<dyn std::error::Error + Send + Sync>),
                    }
                })
                .await
            }
        };
        let handler = Arc::new(move |data| {
            Box::pin(handler(data)) as Pin<Box<dyn Future<Output = Result<RuMessage, Box<dyn std::error::Error + Send + Sync>>> + Send>>
        });

        pyo3_async_runtimes::tokio::future_into_py(py, async move {

            match conn
                .rpc_server(
                    handler,
                    &routing_key,
                    &exchange_name,
                    &exchange_type,
                    &queue_name,
                    process_timeout,
                    command_timeout,
                    queue_options,
                )
                .await
            {
                Ok(res) => Ok(res),
                Err(e) => Err(AppError::from(e).into()),
            }
        })
    }

    pub fn rpc_client<'py>(
        slf: PyRef<'py, Self>,
        exchange_name: &str,
        routing_key: &str,
        body: Payload,
        content_type: &str,
        content_encoding: ContentEncoding,
        response_timeout_millis: u32,
        command_timeout: Option<u64>,
        delivery_mode: DeliveryMode,
        expiration: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let conn = Arc::clone(&slf.inner);
        let exchange_name = exchange_name.to_owned();
        let routing_key = routing_key.to_owned();
        let payload_bytes = match body {
            Payload::Bytes(b) => b.as_bytes().to_vec(),
            Payload::Str(s) => s.to_str()?.as_bytes().to_vec(),
        };
        let content_type = content_type.to_owned();
        let content_encoding = content_encoding.to_owned();
        let delivery_mode = delivery_mode.to_owned();
        let expiration = expiration.to_owned();
        let command_timeout = command_timeout.map(std::time::Duration::from_secs);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            conn.rpc_client(&exchange_name, &routing_key, payload_bytes, &content_type, content_encoding.into(), response_timeout_millis, command_timeout, delivery_mode.into(), expiration).await.map_err(|e| PyValueError::new_err(format!("Failed to update secret: {}", e)))
        })
    }

    pub fn update_secret<'py>(
        slf: PyRef<'py, Self>,
        new_secret: &str,
        reason: &str,
        command_timeout: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let conn = Arc::clone(&slf.inner);
        let new_secret = new_secret.to_owned();
        let reason = reason.to_owned();
        let command_timeout = command_timeout.map(std::time::Duration::from_secs);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            conn.update_secret(&new_secret, &reason, command_timeout).await.map_err(|e| PyValueError::new_err(format!("Failed to update secret: {}", e)))
        })
    }
    
    pub fn close<'py>(slf: PyRef<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let conn = Arc::clone(&slf.inner);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            conn.close().await.map_err(|e| PyValueError::new_err(format!("Failed to close connection: {}", e)))
        })
    }
}