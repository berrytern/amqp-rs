use amqp_client_rust::api::{
    eventbus::AsyncEventbusRabbitMQ as RuAsyncEventbusRabbitMQ,
    utils::{
        Message as RuMessage, PublishOptions as RuPublishOptions,
        RpcClientOptions as RuRpcClientOptions,
    },
};
use pyo3::{prelude::*, types::PyBytes};
use std::sync::Arc;
pub use amqp_client_rust;
pub mod api;
pub mod exceptions;
pub mod utils;
use exceptions::AppError;

use crate::{
    api::connection::AsyncConnection,
    utils::{
        BatchConfig, Config, ConfigOptions, ContentEncoding, DeliveryAck, DeliveryMode, Message,
        Payload, PublishConfirmations, QoSConfig, QueueOptions, TlsAdaptor,
    },
};

struct BatchItem {
    exchange: Arc<str>,
    routing_key: Arc<str>,
    payload: Vec<u8>,
    content_type: Arc<str>,
    content_encoding: ContentEncoding,
    command_timeout: Option<std::time::Duration>,
    delivery_mode: DeliveryMode,
    expiration: Option<u32>,
    ack_sender: tokio::sync::oneshot::Sender<Result<(), AppError>>,
}

struct SubItem {
    message: RuMessage,
    ack_tx: tokio::sync::oneshot::Sender<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
}

static TOKIO_INITIALIZED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub(crate) fn enter_active_runtime() -> Option<tokio::runtime::EnterGuard<'static>> {
    if tokio::runtime::Handle::try_current().is_ok() {
        None
    } else {
        TOKIO_INITIALIZED.store(true, std::sync::atomic::Ordering::Release);
        Some(pyo3_async_runtimes::tokio::get_runtime().enter())
    }
}

#[pyfunction]
#[pyo3(signature = (worker_threads=None))]
pub fn init_tokio(worker_threads: Option<usize>) -> PyResult<()> {
    if TOKIO_INITIALIZED.load(std::sync::atomic::Ordering::Acquire) {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Tokio runtime has already been initialized",
        ));
    }

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(threads) = worker_threads {
        if threads == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "worker_threads must be greater than 0",
            ));
        }
        builder.worker_threads(threads);
    }
    pyo3_async_runtimes::tokio::init(builder);
    let _ = pyo3_async_runtimes::tokio::get_runtime();
    TOKIO_INITIALIZED.store(true, std::sync::atomic::Ordering::Release);
    Ok(())
}

#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct AsyncEventbus {
    pub eventbus: Arc<RuAsyncEventbusRabbitMQ>,
    batch_sender: Option<tokio::sync::mpsc::UnboundedSender<BatchItem>>,
    batch_config: BatchConfig,
}

impl AsyncEventbus {
    /// Creates an `AsyncEventbus` from an existing native Rust `RuAsyncEventbusRabbitMQ`.
    pub fn from_inner(eventbus: Arc<RuAsyncEventbusRabbitMQ>, batch_config: Option<BatchConfig>) -> Self {
        let resolved_batch_config = batch_config.unwrap_or(BatchConfig {
            enabled: false,
            max_batch_size: 100,
            max_delay_ms: 0,
            max_payload_bytes: 1024,
        });

        let batch_sender = if resolved_batch_config.enabled {
            let _guard = enter_active_runtime();
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<BatchItem>();
            let bus = Arc::clone(&eventbus);
            let max_batch_size = resolved_batch_config.max_batch_size;
            let max_delay = std::time::Duration::from_millis(resolved_batch_config.max_delay_ms);

            tokio::spawn(async move {
                let mut batch = Vec::with_capacity(max_batch_size);
                while let Some(first) = rx.recv().await {
                    batch.push(first);

                    if max_delay.as_nanos() > 0 {
                        let deadline = tokio::time::Instant::now() + max_delay;
                        while batch.len() < max_batch_size {
                            tokio::select! {
                                biased;
                                item = rx.recv() => {
                                    match item {
                                        Some(item) => batch.push(item),
                                        None => break,
                                    }
                                }
                                _ = tokio::time::sleep_until(deadline) => {
                                    break;
                                }
                            }
                        }
                    } else {
                        while batch.len() < max_batch_size {
                            match rx.try_recv() {
                                Ok(item) => batch.push(item),
                                Err(_) => break,
                            }
                        }
                    }

                    for item in batch.drain(..) {
                        let b = Arc::clone(&bus);
                        tokio::spawn(async move {
                            let pub_opts = RuPublishOptions {
                                content_type: &item.content_type,
                                content_encoding: item.content_encoding.into(),
                                command_timeout: item.command_timeout,
                                delivery_mode: item.delivery_mode.into(),
                                expiration: item.expiration,
                            };
                            let res = b.publish(
                                &item.exchange,
                                &item.routing_key,
                                item.payload,
                                &pub_opts,
                            ).await;
                            let _ = item.ack_sender.send(res.map_err(AppError::from));
                        });
                    }
                }
            });
            Some(tx)
        } else {
            None
        };

        Self {
            eventbus,
            batch_sender,
            batch_config: resolved_batch_config,
        }
    }

    /// Returns a cloned `Arc` to the underlying native Rust `RuAsyncEventbusRabbitMQ`.
    ///
    /// This allows Rust code to call AMQP publish/subscribe directly with zero FFI overhead.
    #[inline]
    pub fn inner(&self) -> Arc<RuAsyncEventbusRabbitMQ> {
        Arc::clone(&self.eventbus)
    }
}

#[pymethods]
impl AsyncEventbus {
    #[new]
    #[pyo3(signature = (config, qos_config, batch_config=None))]
    fn new(
        config: Config,
        qos_config: QoSConfig,
        batch_config: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let resolved_batch_config = if let Some(bc) = batch_config {
            if let Ok(b) = bc.extract::<bool>() {
                BatchConfig {
                    enabled: b,
                    max_batch_size: 100,
                    max_delay_ms: 0,
                    max_payload_bytes: 1024,
                }
            } else if let Ok(cfg) = bc.extract::<BatchConfig>() {
                cfg
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "batch_config must be a bool or an instance of BatchConfig",
                ));
            }
        } else if let Some(cfg) = &config.options.batch_config {
            cfg.clone()
        } else {
            BatchConfig {
                enabled: false,
                max_batch_size: 100,
                max_delay_ms: 0,
                max_payload_bytes: 1024,
            }
        };

        let _guard = enter_active_runtime();

        let eventbus = Arc::new(RuAsyncEventbusRabbitMQ::new(
            config.into(),
            qos_config.into(),
        ));

        Ok(Self::from_inner(eventbus, Some(resolved_batch_config)))
    }

    #[getter]
    fn batch_config(&self) -> BatchConfig {
        self.batch_config.clone()
    }

    #[allow(clippy::too_many_arguments, clippy::collapsible_if)]
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

        let exchange_name: Arc<str> = Arc::from(exchange_name);
        let routing_key: Arc<str> = Arc::from(routing_key);
        let content_type: Arc<str> = Arc::from(content_type.unwrap_or("application/json"));
        let payload_bytes = match body {
            Payload::Bytes(b) => b.as_bytes().to_vec(),
            Payload::Str(s) => s.to_str()?.as_bytes().to_vec(),
        };
        let cmd_timeout = command_timeout.map(std::time::Duration::from_secs);

        if let Some(sender) = &slf.batch_sender {
            if payload_bytes.len() <= slf.batch_config.max_payload_bytes {
                let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
                let item = BatchItem {
                    exchange: exchange_name,
                    routing_key,
                    payload: payload_bytes,
                    content_type,
                    content_encoding,
                    command_timeout: cmd_timeout,
                    delivery_mode,
                    expiration,
                    ack_sender: ack_tx,
                };

                if let Err(e) = sender.send(item) {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Batch sender channel closed: {}", e
                    )));
                }

                return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    match ack_rx.await {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(e.into()),
                        Err(_) => Err(pyo3::exceptions::PyRuntimeError::new_err(
                            "Batch acknowledgment channel closed",
                        )),
                    }
                });
            }
        }

        let content_encoding = content_encoding.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let pub_opts = RuPublishOptions {
                content_type: &content_type,
                content_encoding: content_encoding.into(),
                command_timeout: cmd_timeout,
                delivery_mode: delivery_mode.into(),
                expiration,
            };
            match eventbus
                .publish(
                    &exchange_name,
                    &routing_key,
                    payload_bytes,
                    &pub_opts,
                )
                .await
            {
                Ok(res) => Ok(res),
                Err(e) => Err(AppError::from(e).into()),
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
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

        let exchange_name: Arc<str> = Arc::from(exchange_name);
        let routing_key: Arc<str> = Arc::from(routing_key);
        let content_type: Arc<str> = Arc::from(content_type.unwrap_or("application/json"));
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
                let ex = Arc::clone(&exchange_name);
                let rk = Arc::clone(&routing_key);
                let ct = Arc::clone(&content_type);
                let ce = content_encoding.clone();
                let dm = delivery_mode.clone();
                tasks.push(tokio::spawn(async move {
                    let pub_opts = RuPublishOptions {
                        content_type: &ct,
                        content_encoding: ce.into(),
                        command_timeout,
                        delivery_mode: dm.into(),
                        expiration: None,
                    };
                    bus.publish(
                        &ex,
                        &rk,
                        payload,
                        &pub_opts,
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

    #[allow(clippy::too_many_arguments)]
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

        let exchange_name: Arc<str> = Arc::from(exchange_name);
        let routing_key: Arc<str> = Arc::from(routing_key);
        let content_type: Arc<str> = Arc::from(content_type);
        let payload_bytes = match body {
            Payload::Bytes(b) => b.as_bytes().to_vec(),
            Payload::Str(s) => s.to_str()?.as_bytes().to_vec(),
        };
        let content_encoding = content_encoding.clone();

        pyo3_async_runtimes::tokio::future_into_py(slf.py(), async move {
            let conn_timeout = connection_timeout.map(std::time::Duration::from_secs);
            let rpc_opts = RuRpcClientOptions {
                content_type: &content_type,
                content_encoding: content_encoding.into(),
                response_timeout_millis: response_timeout,
                command_timeout: conn_timeout,
                delivery_mode: delivery_mode.into(),
                expiration,
            };
            match eventbus
                .rpc_client(
                    &exchange_name,
                    &routing_key,
                    payload_bytes,
                    &rpc_opts,
                )
                .await
            {
                Ok(res) => Ok(res),
                Err(e) => Err(AppError::from(e).into()),
            }
        })
    }

    #[pyo3(signature = (exchange_name, routing_key, handler, process_timeout=None, command_timeout=Some(16), batch_dispatch=None))]
    fn subscribe<'py>(
        slf: PyRef<'py, Self>,
        exchange_name: &str,
        routing_key: &str,
        handler: Py<PyAny>,
        process_timeout: Option<u64>,
        command_timeout: Option<u64>,
        batch_dispatch: Option<bool>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let eventbus = Arc::clone(&slf.eventbus);
        let locals = pyo3_async_runtimes::TaskLocals::with_running_loop(slf.py())?;
        let py = slf.py();
        let handler = Arc::new(handler);
        let exchange_name = exchange_name.to_owned();
        let routing_key = routing_key.to_owned();
        let use_batch = batch_dispatch.unwrap_or(slf.batch_config.enabled);
        let max_batch_size = slf.batch_config.max_batch_size;
        let max_delay_ms = slf.batch_config.max_delay_ms;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let process_timeout = process_timeout.map(std::time::Duration::from_secs);
            let command_timeout = command_timeout.map(std::time::Duration::from_secs);

            if use_batch {
                let (sub_tx, mut sub_rx) = tokio::sync::mpsc::channel::<SubItem>(1024);
                let max_delay = std::time::Duration::from_millis(max_delay_ms);
                let locals_clone = locals.clone();
                let handler_clone = handler.clone();

                tokio::spawn(async move {
                    let mut batch = Vec::with_capacity(max_batch_size);

                    while let Some(first) = sub_rx.recv().await {
                        batch.push(first);

                        if max_delay.as_nanos() > 0 {
                            let deadline = tokio::time::Instant::now() + max_delay;
                            while batch.len() < max_batch_size {
                                tokio::select! {
                                    biased;
                                    item = sub_rx.recv() => {
                                        match item {
                                            Some(item) => batch.push(item),
                                            None => break,
                                        }
                                    }
                                    _ = tokio::time::sleep_until(deadline) => break,
                                }
                            }
                        } else {
                            while batch.len() < max_batch_size {
                                match sub_rx.try_recv() {
                                    Ok(item) => batch.push(item),
                                    Err(_) => break,
                                }
                            }
                        }

                        let current_batch = std::mem::replace(&mut batch, Vec::with_capacity(max_batch_size));
                        for item in current_batch {
                            let item_handler = handler_clone.clone();
                            let item_locals = locals_clone.clone();
                            tokio::spawn(pyo3_async_runtimes::tokio::scope(item_locals, async move {
                                let msg = Message::from(item.message);
                                let py_future_res = Python::try_attach(|py| -> PyResult<_> {
                                    let bound_handler = item_handler.bind(py);
                                    let res = bound_handler.call1((msg,))?;
                                    match pyo3_async_runtimes::tokio::into_future(res.clone()) {
                                        Ok(fut) => Ok(Some(fut)),
                                        Err(e) => {
                                            if let Ok(close_fn) = res.getattr(pyo3::intern!(py, "close")) {
                                                let _ = close_fn.call0();
                                                Err(e)
                                            } else {
                                                Ok(None)
                                            }
                                        }
                                    }
                                });

                                match py_future_res {
                                    Some(Ok(Some(fut))) => match fut.await {
                                        Ok(_) => {
                                            let _ = item.ack_tx.send(Ok(()));
                                        }
                                        Err(e) => {
                                            let _ = item.ack_tx.send(Err(Box::new(std::io::Error::other(e.to_string()))));
                                        }
                                    },
                                    Some(Ok(None)) => {
                                        let _ = item.ack_tx.send(Ok(()));
                                    }
                                    Some(Err(e)) => {
                                        let _ = item.ack_tx.send(Err(Box::new(std::io::Error::other(format!("Failed to execute Python callback: {}", e)))));
                                    }
                                    None => {
                                        let _ = item.ack_tx.send(Err(Box::new(std::io::Error::other("Python runtime is finalizing or unavailable"))));
                                    }
                                }
                            }));
                        }
                    }
                });

                let sub_tx = sub_tx.clone();
                match eventbus
                    .subscribe(
                        &exchange_name,
                        &routing_key,
                        move |body| {
                            let tx = sub_tx.clone();
                            async move {
                                let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
                                if tx.send(SubItem { message: body, ack_tx }).await.is_err() {
                                    return Err(Box::new(std::io::Error::other("Subscription channel closed")) as _);
                                }
                                match ack_rx.await {
                                    Ok(res) => res,
                                    Err(_) => Err(Box::new(std::io::Error::other("Ack channel dropped")) as _),
                                }
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
            } else {
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
                                            Err(e) => Err(Box::new(std::io::Error::other(e.to_string())) as _),
                                        },
                                        Err(e) => Err(Box::new(std::io::Error::other(format!("Failed to execute Python callback: {}", e))) as _),
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
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (exchange_name, routing_key, handler, batch_size=100, max_delay_ms=0, process_timeout=None, command_timeout=Some(16)))]
    fn subscribe_batch<'py>(
        slf: PyRef<'py, Self>,
        exchange_name: &str,
        routing_key: &str,
        handler: Py<PyAny>,
        batch_size: usize,
        max_delay_ms: u64,
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

            let (sub_tx, mut sub_rx) = tokio::sync::mpsc::channel::<SubItem>(1024);
            let max_delay = std::time::Duration::from_millis(max_delay_ms);
            let locals_clone = locals.clone();
            let handler_clone = handler.clone();

            tokio::spawn(async move {
                let mut batch = Vec::with_capacity(batch_size);

                while let Some(first) = sub_rx.recv().await {
                    batch.push(first);

                    if max_delay.as_nanos() > 0 {
                        let deadline = tokio::time::Instant::now() + max_delay;
                        while batch.len() < batch_size {
                            tokio::select! {
                                biased;
                                item = sub_rx.recv() => {
                                    match item {
                                        Some(item) => batch.push(item),
                                        None => break,
                                    }
                                }
                                _ = tokio::time::sleep_until(deadline) => break,
                            }
                        }
                    } else {
                        while batch.len() < batch_size {
                            match sub_rx.try_recv() {
                                Ok(item) => batch.push(item),
                                Err(_) => break,
                            }
                        }
                    }

                    let current_batch = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                    let item_handler = handler_clone.clone();
                    let item_locals = locals_clone.clone();
                    let (messages, ack_txs): (Vec<Message>, Vec<_>) = current_batch
                        .into_iter()
                        .map(|item| (Message::from(item.message), item.ack_tx))
                        .unzip();

                    tokio::spawn(pyo3_async_runtimes::tokio::scope(item_locals, async move {
                        let py_future_res = Python::try_attach(|py| -> PyResult<_> {
                            let py_messages = pyo3::types::PyList::new(py, messages)?;
                            let bound_handler = item_handler.bind(py);
                            let res = bound_handler.call1((py_messages,))?;
                            match pyo3_async_runtimes::tokio::into_future(res.clone()) {
                                Ok(fut) => Ok(Some(fut)),
                                Err(e) => {
                                    if let Ok(close_fn) = res.getattr(pyo3::intern!(py, "close")) {
                                        let _ = close_fn.call0();
                                        Err(e)
                                    } else {
                                        Ok(None)
                                    }
                                }
                            }
                        });

                        match py_future_res {
                            Some(Ok(Some(fut))) => match fut.await {
                                Ok(_) => {
                                    for tx in ack_txs {
                                        let _ = tx.send(Ok(()));
                                    }
                                }
                                Err(e) => {
                                    let err_msg = e.to_string();
                                    for tx in ack_txs {
                                        let _ = tx.send(Err(Box::new(std::io::Error::other(err_msg.clone()))));
                                    }
                                }
                            },
                            Some(Ok(None)) => {
                                for tx in ack_txs {
                                    let _ = tx.send(Ok(()));
                                }
                            }
                            Some(Err(e)) => {
                                let err_msg = format!("Failed to execute Python callback: {}", e);
                                for tx in ack_txs {
                                    let _ = tx.send(Err(Box::new(std::io::Error::other(err_msg.clone()))));
                                }
                            }
                            None => {
                                for tx in ack_txs {
                                    let _ = tx.send(Err(Box::new(std::io::Error::other("Python runtime is finalizing or unavailable"))));
                                }
                            }
                        }
                    }));
                }
            });

            let sub_tx = sub_tx.clone();
            match eventbus
                .subscribe(
                    &exchange_name,
                    &routing_key,
                    move |body| {
                        let tx = sub_tx.clone();
                        async move {
                            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
                            if tx.send(SubItem { message: body, ack_tx }).await.is_err() {
                                return Err(Box::new(std::io::Error::other("Subscription channel closed")) as _);
                            }
                            match ack_rx.await {
                                Ok(res) => res,
                                Err(_) => Err(Box::new(std::io::Error::other("Ack channel dropped")) as _),
                            }
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
                                            return Err(Box::new(std::io::Error::other(
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
                                                None => Err(Box::new(std::io::Error::other(
                                                    "Python runtime is finalizing or unavailable",
                                                ))
                                                    as Box<dyn std::error::Error + Send + Sync>),
                                            }
                                        }
                                        Err(e) => Err(Box::new(std::io::Error::other(
                                            e.to_string(),
                                        ))
                                            as Box<dyn std::error::Error + Send + Sync>),
                                    },
                                    Err(e) => Err(Box::new(std::io::Error::other(
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
        let eventbus = Arc::clone(&slf.eventbus);
        let py = slf.py();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            eventbus
                .dispose()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }
}

#[pymodule]
fn amqp_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    if let Ok(val) = std::env::var("TOKIO_WORKER_THREADS")
        && let Ok(threads) = val.parse::<usize>()
        && threads > 0
    {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all();
        builder.worker_threads(threads);
        pyo3_async_runtimes::tokio::init(builder);
    }

    m.add_function(wrap_pyfunction!(init_tokio, m)?)?;
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
    m.add_class::<BatchConfig>()?;
    m.add_class::<DeliveryAck>()?;
    Ok(())
}
