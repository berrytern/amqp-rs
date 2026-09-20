use amqp_client_rust::api::{
    eventbus::AsyncEventbusRabbitMQ as RuAsyncEventbusRabbitMQ, utils::Message as RuMessage,
};
use pyo3::{prelude::*, types::PyBytes};
use std::sync::Arc;
pub mod api;
pub mod exceptions;
pub mod utils;
use exceptions::AppError;

use crate::{
    api::connection::AsyncConnection,
    utils::{
        Config, ConfigOptions, ContentEncoding, DeliveryMode, Message, Payload,
        PublishConfirmations, QoSConfig, QueueOptions, TlsAdaptor,
    },
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
    string_cache: Arc<std::sync::RwLock<std::collections::HashSet<Arc<str>>>>,
}

impl AsyncEventbus {
    fn intern_string(&self, s: &str) -> Arc<str> {
        if let Some(existing) = self.string_cache.read().ok().and_then(|c| c.get(s).cloned()) {
            return existing;
        }
        if let Ok(mut cache) = self.string_cache.write() {
            if let Some(existing) = cache.get(s) {
                return Arc::clone(existing);
            }
            let arc_s: Arc<str> = Arc::from(s);
            cache.insert(Arc::clone(&arc_s));
            arc_s
        } else {
            Arc::from(s)
        }
    }
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
            string_cache: Arc::new(std::sync::RwLock::new(std::collections::HashSet::new())),
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

        let ex = slf.intern_string(exchange_name);
        let rk = slf.intern_string(routing_key);
        let ct = content_type.map(|s| slf.intern_string(s));
        let payload_bytes = match body {
            Payload::Bytes(b) => b.as_bytes().to_vec(),
            Payload::Str(s) => s.to_str()?.as_bytes().to_vec(),
        };

        let content_encoding = content_encoding.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let command_timeout = command_timeout.map(std::time::Duration::from_secs);
            match eventbus
                .publish(
                    &ex,
                    &rk,
                    payload_bytes,
                    ct.as_deref(),
                    content_encoding.into(),
                    command_timeout,
                    Some(delivery_mode.into()),
                    expiration,
                )
                .await
            {
                Ok(res) => Ok(res),
                Err(e) => Err(AppError::from(e).into()),
            }
        })
    }

    #[pyo3(signature = (exchange_name, routing_key, messages, content_type=Some("application/json"), content_encoding=ContentEncoding::Null, command_timeout=16, delivery_mode=DeliveryMode::Transient))]
    fn publish_batch<'py>(
        slf: PyRef<'py, Self>,
        exchange_name: &'py str,
        routing_key: &'py str,
        messages: Vec<Payload<'py>>,
        content_type: Option<&'py str>,
        content_encoding: ContentEncoding,
        command_timeout: Option<u64>,
        delivery_mode: DeliveryMode,
    ) -> PyResult<Bound<'py, PyAny>> {
        let eventbus = Arc::clone(&slf.eventbus);
        let py = slf.py();

        let ex = slf.intern_string(exchange_name);
        let rk = slf.intern_string(routing_key);
        let ct = content_type.map(|s| slf.intern_string(s));
        let content_encoding = content_encoding.clone();

        let mut payloads = Vec::with_capacity(messages.len());
        for body in messages {
            let b = match body {
                Payload::Bytes(b) => b.as_bytes().to_vec(),
                Payload::Str(s) => s.to_str()?.as_bytes().to_vec(),
            };
            payloads.push(b);
        }

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let command_timeout = command_timeout.map(std::time::Duration::from_secs);
            let mut tasks = Vec::with_capacity(payloads.len());
            for payload in payloads {
                let bus = Arc::clone(&eventbus);
                let ex = Arc::clone(&ex);
                let rk = Arc::clone(&rk);
                let ct = ct.clone();
                let ce = content_encoding.clone();
                let dm = delivery_mode.clone();
                tasks.push(tokio::spawn(async move {
                    bus.publish(
                        &ex,
                        &rk,
                        payload,
                        ct.as_deref(),
                        ce.into(),
                        command_timeout,
                        Some(dm.into()),
                        None,
                    )
                    .await
                }));
            }
            for task in tasks {
                match task.await {
                    Ok(Ok(())) => {},
                    Ok(Err(e)) => return Err(AppError::from(e).into()),
                    Err(e) => return Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
                }
            }
            Ok(())
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

        let ex = slf.intern_string(exchange_name);
        let rk = slf.intern_string(routing_key);
        let ct = slf.intern_string(content_type);
        let payload_bytes = match body {
            Payload::Bytes(b) => b.as_bytes().to_vec(),
            Payload::Str(s) => s.to_str()?.as_bytes().to_vec(),
        };
        let content_encoding = content_encoding.clone();

        pyo3_async_runtimes::tokio::future_into_py(slf.py(), async move {
            let conn_timeout = connection_timeout.map(std::time::Duration::from_secs);
            match eventbus
                .rpc_client(
                    &ex,
                    &rk,
                    payload_bytes,
                    &ct,
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
            }
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
                                let future_result = match Python::try_attach(|py| -> PyResult<_> {
                                    let bound_handler = handler_clone.bind(py);
                                    let coro = bound_handler.call1((Message::from(body),))?;

                                    match pyo3_async_runtimes::tokio::into_future(coro.clone()) {
                                        Ok(fut) => Ok(fut),
                                        Err(e) => {
                                            if let Ok(close_fn) =
                                                coro.getattr(pyo3::intern!(py, "close"))
                                            {
                                                let _ = close_fn.call0();
                                            }
                                            Err(e)
                                        }
                                    }
                                }) {
                                    Some(res) => res,
                                    None => return Ok(()),
                                };
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
                                let py_future_result =
                                    match Python::try_attach(|py| -> PyResult<_> {
                                        let bound_handler = handler_clone.bind(py);
                                        let coro = bound_handler.call1((Message::from(body),))?;

                                        // Now into_future will successfully find the asyncio loop!
                                        match pyo3_async_runtimes::tokio::into_future(coro.clone())
                                        {
                                            Ok(fut) => Ok(fut),
                                            Err(e) => {
                                                if let Ok(close_fn) =
                                                    coro.getattr(pyo3::intern!(py, "close"))
                                                {
                                                    let _ = close_fn.call0();
                                                }
                                                Err(e)
                                            }
                                        }
                                    }) {
                                        Some(res) => res,
                                        None => {
                                            return Err(Box::new(std::io::Error::new(
                                                std::io::ErrorKind::Other,
                                                "Python runtime is finalizing or unavailable",
                                            ))
                                                as Box<dyn std::error::Error + Send + Sync>);
                                        }
                                    };
                                match py_future_result {
                                    Ok(py_future) => match py_future.await {
                                        Ok(result) => {
                                            match Python::try_attach(|py| {
                                                if let Ok(message) = result.extract::<Message>(py) {
                                                    return Ok(RuMessage::from(message));
                                                }
                                                match result.cast_bound::<PyBytes>(py) {
                                                    Ok(bytes) => Ok(RuMessage {
                                                        body: bytes.as_bytes().into(),
                                                        content_type: None,
                                                    }),
                                                    Err(_) => Err(Box::new(std::io::Error::new(
                                                        std::io::ErrorKind::InvalidData,
                                                        "RPC handler must return bytes",
                                                    ))
                                                        as Box<
                                                            dyn std::error::Error + Send + Sync,
                                                        >),
                                                }
                                            }) {
                                                Some(res) => res,
                                                None => Err(Box::new(std::io::Error::new(
                                                    std::io::ErrorKind::Other,
                                                    "Python runtime is finalizing or unavailable",
                                                ))
                                                    as Box<dyn std::error::Error + Send + Sync>),
                                            }
                                        }
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
            tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current()
                    .block_on(async move { eventbus.dispose().await.map_err(|e| e.to_string()) })
            })
            .await
            .map_err(|e| AppError {
                description: Some(e.to_string()),
                message: None,
                error_type: amqp_client_rust::errors::AppErrorType::UnexpectedResultError,
            })?
            .map_err(|_| AppError {
                description: None,
                message: None,
                error_type: amqp_client_rust::errors::AppErrorType::UnexpectedResultError,
            })?;

            Ok(())
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
    m.add_class::<DeliveryMode>()?;
    m.add_class::<QueueOptions>()?;
    m.add_class::<PublishConfirmations>()?;
    Ok(())
}
