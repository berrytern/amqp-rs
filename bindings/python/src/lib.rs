use std::{sync::Arc};
use amqp_client_rust::{
    amqprs::tls::{TlsAdaptor as RuTlsAdaptor}, api::{
        eventbus::AsyncEventbusRabbitMQ as RuAsyncEventbusRabbitMQ,
        utils::{ContentEncoding as RuContentEncoding, DeliveryMode as RuDeliveryMode, Message as RuMessage},
    }, domain::config::{
        Config as RuConfig, ConfigOptions as RuConfigOptions, QoSConfig as RuQoSConfig,
    }
};
use pyo3::{
    exceptions::PyValueError, prelude::*, types::{PyBytes, PyString}
};
pub mod exceptions;
pub mod api;
pub mod utils;
use exceptions::AppError;
use std::path::PathBuf;

use crate::{
    api::connection::AsyncConnection,
    utils::{Config, ConfigOptions, ContentEncoding, DeliveryMode, Message, Payload, QoSConfig, TlsAdaptor}
};

/*static TOKIO_RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});*/
#[pyclass(skip_from_py_object)]
#[derive(Clone)]
struct AsyncEventbus {
    eventbus: Arc<RuAsyncEventbusRabbitMQ>,
}

#[pymethods]
impl AsyncEventbus {
    #[new]
    fn new(config: Config, qos_config: QoSConfig) -> Self {
        let rt = pyo3_async_runtimes::tokio::get_runtime();

        let _guard = rt.enter();
        Self {
            eventbus: Arc::new(RuAsyncEventbusRabbitMQ::new(
                config.into(),
                qos_config.into(),
            )),
        }
    }

    #[pyo3(signature = (exchange_name, routing_key, body, content_type=Some("application/json"), content_encoding=ContentEncoding::Null, command_timeout=16, delivery_mode=DeliveryMode::Transient, expiration=None))]
    fn publish<'py>(
        slf: PyRef<'py, Self>,
        exchange_name: &'py str,
        routing_key: &'py str,
        body: Payload,
        content_type: Option<&'py str>,
        content_encoding: ContentEncoding,
        command_timeout: Option<u64>,
        delivery_mode: DeliveryMode,
        expiration: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let eventbus = Arc::clone(&slf.eventbus);
        let py = slf.py();

        let exchange_name = exchange_name.to_owned();
        let routing_key = routing_key.to_owned();
        let payload_bytes = match body {
            Payload::Bytes(b) => b.as_bytes().to_vec(),
            Payload::Str(s) => s.to_str()?.as_bytes().to_vec(),
        };

        let content_type = content_type.map(|s| s.to_owned());
        let content_encoding = content_encoding.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let command_timeout = command_timeout.map(std::time::Duration::from_secs);
            match eventbus
                .publish(
                    &exchange_name,
                    &routing_key,
                    payload_bytes,
                    content_type.as_deref(),
                    content_encoding.into(),
                    command_timeout,
                    Some(delivery_mode.into()),
                    expiration,
                )
                .await
            {
                Ok(res) => Ok(res),
                Err(e) => return Err(AppError::from(e).into()),
            }
        })
    }

    #[pyo3(signature = (exchange_name, routing_key, body, content_type="application/json", content_encoding=ContentEncoding::Null, response_timeout=20_000, connection_timeout=Some(32), delivery_mode=DeliveryMode::Transient, expiration=None))]
    fn rpc_client<'py>(
        slf: PyRef<'py, Self>,
        exchange_name: &str,
        routing_key: &str,
        body: Payload<'py>,
        content_type: &str,
        content_encoding: ContentEncoding,
        response_timeout: u32,
        connection_timeout: Option<u64>,
        delivery_mode: DeliveryMode,
        expiration: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let eventbus = Arc::clone(&slf.eventbus);

        let exchange_name = exchange_name.to_owned();
        let routing_key = routing_key.to_owned();
        let payload_bytes = match body {
            Payload::Bytes(b) => b.as_bytes().to_vec(),
            Payload::Str(s) => s.to_str()?.as_bytes().to_vec(),
        };
        let content_type = content_type.to_owned();
        let content_encoding = content_encoding.clone();

        pyo3_async_runtimes::tokio::future_into_py(slf.py(), async move {
            let conn_timeout = connection_timeout.map(std::time::Duration::from_secs);
            let response = match eventbus
                .rpc_client(
                    &exchange_name,
                    &routing_key,
                    payload_bytes,
                    &content_type,
                    content_encoding.into(),
                    response_timeout,
                    conn_timeout,
                    Some(delivery_mode.into()),
                    expiration,
                )
                .await
            {
                Ok(res) => Ok(res),
                Err(e) => Err(AppError::from(e).into()),
            };
            response
        })
    }

    #[pyo3(signature = (exchange_name, routing_key, handler, process_timeout=None, command_timeout=Some(16)))]
    fn subscribe<'py>(
        slf: PyRef<'py, Self>,
        exchange_name: &str,
        routing_key: &str,
        handler: Py<PyAny>,
        process_timeout: Option<u64>,
        command_timeout: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let eventbus = Arc::clone(&slf.eventbus);
        let locals = pyo3_async_runtimes::TaskLocals::with_running_loop(slf.py())?;
        let py = slf.py();
        let handler = Arc::new(handler);
        let exchange_name = exchange_name.to_owned();
        let routing_key = routing_key.to_owned();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let process_timeout = process_timeout.map(std::time::Duration::from_secs);
            let command_timeout = command_timeout.map(std::time::Duration::from_secs);

            match eventbus
                .subscribe(
                    &exchange_name,
                    &routing_key,
                    move |body| {
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
                    },
                    process_timeout,
                    command_timeout,
                )
                .await
            {
                Ok(res) => Ok(res),
                Err(e) => Err(AppError::from(e).into()),
            }
        })
    }

    #[pyo3(signature = (routing_key, handler, process_timeout=None, command_timeout=None))]
    fn provide_resource<'py>(
        slf: PyRef<'py, Self>,
        routing_key: &str,
        handler: Py<PyAny>,
        process_timeout: Option<u64>,
        command_timeout: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let eventbus = Arc::clone(&slf.eventbus);
        let locals = pyo3_async_runtimes::TaskLocals::with_running_loop(slf.py())?;
        let py = slf.py();
        let handler = Arc::new(handler);
        let routing_key = routing_key.to_owned();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let process_timeout = process_timeout.map(std::time::Duration::from_secs);
            let command_timeout = command_timeout.map(std::time::Duration::from_secs);

            match eventbus
                .provide_resource(
                    &routing_key,
                    move |body| {
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
                    },
                    process_timeout,
                    command_timeout,
                )
                .await
            {
                Ok(res) => Ok(res),
                Err(e) => Err(AppError::from(e).into()),
            }
        })
    }
    fn dispose(slf: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let eventbus = Arc::clone(&slf.eventbus); // Clone the Arc for the async move
        let py = slf.py();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            eventbus.dispose().await.map_err(|_| {
                AppError {
                    description: None,
                    message: None,
                    error_type: amqp_client_rust::errors::AppErrorType::UnexpectedResultError,
                }
                .into()
            })
        })
    }
}

#[pymodule]
fn amqp_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<AsyncEventbus>()?;
    m.add_class::<AsyncConnection>()?;
    m.add_class::<Config>()?;
    m.add_class::<ConfigOptions>()?;
    m.add_class::<QoSConfig>()?;
    m.add_class::<TlsAdaptor>()?;
    m.add_class::<ContentEncoding>()?;
    m.add_class::<Message>()?;
    Ok(())
}
