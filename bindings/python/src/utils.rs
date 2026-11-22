use crate::exceptions::AppError;
use amqp_client_rust::{
    amqprs::tls::TlsAdaptor as RuTlsAdaptor,
    api::utils::{
        Confirmations as RuPublishConfirmation, ContentEncoding as RuContentEncoding,
        DeliveryMode as RuDeliveryMode, ExchangeOptions as RuExchangeOptions,
        ExchangeType as RuExchangeType, Message as RuMessage,
        MessageMetadata as RuMessageMetadata, QueueOptions as RuQueueOptions,
        Topology as RuTopology,
    },
    domain::config::{
        Config as RuConfig, ConfigOptions as RuConfigOptions, QoSConfig as RuQoSConfig,
    },
};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyBytes, PyString},
};
use std::path::PathBuf;
use std::{collections::HashMap, sync::Arc};

#[pyclass(from_py_object, get_all, set_all)]
#[derive(Debug, Clone)]
pub enum DeliveryMode {
    Transient = 1,
    Persistent = 2,
}

impl From<DeliveryMode> for RuDeliveryMode {
    fn from(mode: DeliveryMode) -> Self {
        match mode {
            DeliveryMode::Transient => RuDeliveryMode::Transient,
            DeliveryMode::Persistent => RuDeliveryMode::Persistent,
        }
    }
}

#[pyclass(from_py_object, get_all, set_all)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeType {
    Direct = 0,
    Fanout = 1,
    Topic = 2,
    Headers = 3,
}

impl From<ExchangeType> for RuExchangeType {
    fn from(val: ExchangeType) -> Self {
        match val {
            ExchangeType::Direct => RuExchangeType::Direct,
            ExchangeType::Fanout => RuExchangeType::Fanout,
            ExchangeType::Topic => RuExchangeType::Topic,
            ExchangeType::Headers => RuExchangeType::Headers,
        }
    }
}

#[pyclass(from_py_object, get_all, set_all)]
#[derive(Debug, Clone, Copy)]
pub struct ExchangeOptions {
    pub exchange_type: ExchangeType,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub no_create: bool,
}

#[pymethods]
impl ExchangeOptions {
    #[new]
    #[pyo3(signature = (exchange_type=ExchangeType::Topic, durable=true, auto_delete=false, internal=false, no_create=false))]
    fn new(
        exchange_type: ExchangeType,
        durable: bool,
        auto_delete: bool,
        internal: bool,
        no_create: bool,
    ) -> Self {
        Self {
            exchange_type,
            durable,
            auto_delete,
            internal,
            no_create,
        }
    }

    #[staticmethod]
    pub fn topic() -> Self {
        Self::new(ExchangeType::Topic, true, false, false, false)
    }

    #[staticmethod]
    pub fn direct() -> Self {
        Self::new(ExchangeType::Direct, true, false, false, false)
    }

    #[staticmethod]
    pub fn fanout() -> Self {
        Self::new(ExchangeType::Fanout, true, false, false, false)
    }

    #[staticmethod]
    pub fn headers() -> Self {
        Self::new(ExchangeType::Headers, true, false, false, false)
    }
}

impl From<ExchangeOptions> for RuExchangeOptions {
    fn from(opts: ExchangeOptions) -> Self {
        RuExchangeOptions {
            exchange_type: opts.exchange_type.into(),
            durable: opts.durable,
            auto_delete: opts.auto_delete,
            internal: opts.internal,
            no_create: opts.no_create,
        }
    }
}

#[pyclass(from_py_object, get_all, set_all)]
#[derive(Debug, Clone)]
pub struct QueueOptions {
    pub auto_delete: bool,
    pub durable: bool,
    pub exclusive: bool,
    pub no_create: bool,
    pub dead_letter_exchange: Option<String>,
    pub dead_letter_routing_key: Option<String>,
    pub message_ttl: Option<u32>,
    pub expires: Option<u32>,
    pub max_length: Option<u32>,
    pub max_priority: Option<u8>,
    pub arguments: HashMap<String, String>,
}
#[pymethods]
impl QueueOptions {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (auto_delete=false, durable=true, exclusive=false, no_create=false, arguments=None, dead_letter_exchange=None, dead_letter_routing_key=None, message_ttl=None, expires=None, max_length=None, max_priority=None))]
    fn new(
        auto_delete: bool,
        durable: bool,
        exclusive: bool,
        no_create: bool,
        arguments: Option<HashMap<String, String>>,
        dead_letter_exchange: Option<String>,
        dead_letter_routing_key: Option<String>,
        message_ttl: Option<u32>,
        expires: Option<u32>,
        max_length: Option<u32>,
        max_priority: Option<u8>,
    ) -> Self {
        Self {
            auto_delete,
            durable,
            exclusive,
            no_create,
            dead_letter_exchange,
            dead_letter_routing_key,
            message_ttl,
            expires,
            max_length,
            max_priority,
            arguments: arguments.unwrap_or_default(),
        }
    }
}

impl QueueOptions {
    pub fn to_ru_options(options: QueueOptions) -> Result<RuQueueOptions, AppError> {
        let mut queue_options = RuQueueOptions::new()
            .auto_delete(options.auto_delete)
            .durable(options.durable)
            .exclusive(options.exclusive)
            .no_create(options.no_create)
            .arguments(&options.arguments)?;
        if let Some(dlx) = options.dead_letter_exchange {
            queue_options = queue_options.dead_letter_exchange(dlx);
        }
        if let Some(dlk) = options.dead_letter_routing_key {
            queue_options = queue_options.dead_letter_routing_key(dlk);
        }
        if let Some(ttl) = options.message_ttl {
            queue_options = queue_options.message_ttl(ttl);
        }
        if let Some(exp) = options.expires {
            queue_options = queue_options.expires(exp);
        }
        if let Some(max_len) = options.max_length {
            queue_options = queue_options.max_length(max_len);
        }
        if let Some(max_prio) = options.max_priority {
            queue_options = queue_options.max_priority(max_prio);
        }
        Ok(queue_options)
    }
}

#[pyclass(from_py_object)]
#[derive(Debug, Clone, Default)]
pub struct Topology {
    pub exchanges: Vec<(String, ExchangeOptions)>,
    pub queues: Vec<(String, QueueOptions)>,
    pub bindings: Vec<(String, String, String)>,
}

#[pymethods]
impl Topology {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    #[pyo3(signature = (name, options=None))]
    fn exchange(mut slf: PyRefMut<'_, Self>, name: String, options: Option<ExchangeOptions>) -> Self {
        slf.exchanges.push((name, options.unwrap_or_else(ExchangeOptions::topic)));
        slf.clone()
    }

    #[pyo3(signature = (name, options=None))]
    fn queue(mut slf: PyRefMut<'_, Self>, name: String, options: Option<QueueOptions>) -> Self {
        let q_opts = options.unwrap_or_else(|| QueueOptions {
            auto_delete: false,
            durable: true,
            exclusive: false,
            no_create: false,
            dead_letter_exchange: None,
            dead_letter_routing_key: None,
            message_ttl: None,
            expires: None,
            max_length: None,
            max_priority: None,
            arguments: HashMap::new(),
        });
        slf.queues.push((name, q_opts));
        slf.clone()
    }

    fn bind(mut slf: PyRefMut<'_, Self>, queue_name: String, exchange_name: String, routing_key: String) -> Self {
        slf.bindings.push((queue_name, exchange_name, routing_key));
        slf.clone()
    }
}

impl Topology {
    pub fn to_ru_topology(self) -> Result<RuTopology, AppError> {
        let mut topo = RuTopology::new();
        for (name, opts) in self.exchanges {
            topo = topo.exchange(name, opts.into());
        }
        for (name, opts) in self.queues {
            topo = topo.queue(name, QueueOptions::to_ru_options(opts)?);
        }
        for (queue_name, exchange_name, routing_key) in self.bindings {
            topo = topo.bind(queue_name, exchange_name, routing_key);
        }
        Ok(topo)
    }
}

#[pyclass(from_py_object, get_all, set_all)]
#[derive(Debug, Clone)]
pub enum PublishConfirmations {
    Disables = 0,
    PublisherConfirms = 1,
    RPCClientPublisherConfirms = 2,
    RPCServerPublisherConfirms = 3,
}

impl From<PublishConfirmations> for RuPublishConfirmation {
    fn from(confirm: PublishConfirmations) -> Self {
        match confirm {
            PublishConfirmations::Disables => RuPublishConfirmation::Disabled,
            PublishConfirmations::PublisherConfirms => RuPublishConfirmation::PublisherConfirms,
            PublishConfirmations::RPCClientPublisherConfirms => {
                RuPublishConfirmation::RPCClientPublisherConfirms
            }
            PublishConfirmations::RPCServerPublisherConfirms => {
                RuPublishConfirmation::RPCServerPublisherConfirms
            }
        }
    }
}

/// AMQP message container supporting zero-copy payload buffer protocol (PEP 3118).
#[pyclass(frozen, from_py_object)]
#[derive(Debug)]
pub struct Message {
    pub body: Arc<[u8]>,
    pub content_type: Option<String>,
    pub metadata: Option<Arc<RuMessageMetadata>>,
    pub cached_body: std::sync::OnceLock<Py<PyBytes>>,
}

impl Clone for Message {
    fn clone(&self) -> Self {
        Self {
            body: self.body.clone(),
            content_type: self.content_type.clone(),
            metadata: self.metadata.clone(),
            cached_body: std::sync::OnceLock::new(),
        }
    }
}

impl From<RuMessage> for Message {
    fn from(msg: RuMessage) -> Self {
        Self {
            body: msg.body,
            content_type: msg.content_type,
            metadata: msg.metadata,
            cached_body: std::sync::OnceLock::new(),
        }
    }
}
impl From<Message> for RuMessage {
    fn from(msg: Message) -> Self {
        Self {
            body: msg.body,
            content_type: msg.content_type,
            metadata: msg.metadata,
        }
    }
}
impl From<&Message> for RuMessage {
    fn from(msg: &Message) -> Self {
        Self {
            body: msg.body.clone(),
            content_type: msg.content_type.clone(),
            metadata: msg.metadata.clone(),
        }
    }
}

#[pymethods]
impl Message {
    /// Create a new Message instance with optional content type.
    #[new]
    #[pyo3(signature = (body, content_type=None))]
    fn py_new<'py>(body: Payload<'py>, content_type: Option<String>) -> PyResult<Self> {
        Self::new(body, content_type)
    }

    /// Create a new Message instance with optional content type.
    #[staticmethod]
    #[pyo3(signature = (body, content_type=None))]
    fn new<'py>(body: Payload<'py>, content_type: Option<String>) -> PyResult<Self> {
        let detected_ct = body.content_type();
        let body_bytes = body.to_arc_bytes();
        Ok(Self {
            body: body_bytes,
            content_type: content_type.or(detected_ct),
            metadata: None,
            cached_body: std::sync::OnceLock::new(),
        })
    }

    /// Return the message body as bytes (cached on first access).
    #[getter]
    fn body<'py>(slf: PyRef<'py, Self>) -> Bound<'py, PyBytes> {
        let py = slf.py();
        let cached = slf.cached_body.get_or_init(|| {
            PyBytes::new(py, &slf.body).unbind()
        });
        cached.bind(py).clone()
    }

    /// Return a zero-copy memoryview (PEP 3118) of the underlying payload buffer.
    fn memoryview<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, pyo3::types::PyMemoryView>> {
        let py = slf.py();
        unsafe {
            let ptr = pyo3::ffi::PyMemoryView_FromObject(slf.as_ptr());
            if ptr.is_null() {
                return Err(PyErr::take(py).unwrap_or_else(|| {
                    pyo3::exceptions::PyBufferError::new_err("Failed to create memoryview")
                }));
            }
            Bound::from_owned_ptr(py, ptr).cast_into::<pyo3::types::PyMemoryView>().map_err(|e| e.into())
        }
    }

    /// Return the optional message content type.
    #[getter]
    fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    #[getter]
    fn routing_key(&self) -> Option<&str> {
        self.metadata.as_deref().and_then(|m| m.routing_key())
    }

    #[getter]
    fn exchange(&self) -> Option<&str> {
        self.metadata.as_deref().and_then(|m| m.exchange())
    }

    #[getter]
    fn redelivered(&self) -> bool {
        self.metadata.as_deref().is_some_and(|m| m.redelivered())
    }

    #[getter]
    fn delivery_tag(&self) -> Option<u64> {
        self.metadata.as_deref().and_then(|m| m.delivery_tag())
    }

    #[getter]
    fn correlation_id(&self) -> Option<&str> {
        self.metadata.as_deref().and_then(|m| m.correlation_id())
    }

    #[getter]
    fn message_id(&self) -> Option<&str> {
        self.metadata.as_deref().and_then(|m| m.message_id())
    }

    #[getter]
    fn app_id(&self) -> Option<&str> {
        self.metadata.as_deref().and_then(|m| m.app_id())
    }

    #[getter]
    fn timestamp(&self) -> Option<u64> {
        self.metadata.as_deref().and_then(|m| m.timestamp())
    }

    #[getter]
    fn headers(&self) -> Option<HashMap<String, String>> {
        self.metadata.as_deref().and_then(|m| m.headers())
    }

    unsafe fn __getbuffer__(
        slf: PyRef<'_, Self>,
        view: *mut pyo3::ffi::Py_buffer,
        flags: std::os::raw::c_int,
    ) -> PyResult<()> {
        let bytes = &slf.body;
        let ret = unsafe {
            pyo3::ffi::PyBuffer_FillInfo(
                view,
                slf.as_ptr(),
                bytes.as_ptr() as *mut _,
                bytes.len() as pyo3::ffi::Py_ssize_t,
                1,
                flags,
            )
        };
        if ret == -1 {
            return Err(PyErr::take(slf.py()).unwrap_or_else(|| {
                pyo3::exceptions::PyBufferError::new_err("Buffer fill info failed")
            }));
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut pyo3::ffi::Py_buffer) {}
}

#[pyclass(from_py_object, get_all, set_all)]
#[derive(Debug, Clone)]
pub enum ContentEncoding {
    Zstd,
    Lz4,
    Zlib,
    Null,
}
impl From<ContentEncoding> for RuContentEncoding {
    fn from(encoding: ContentEncoding) -> Self {
        match encoding {
            ContentEncoding::Zstd => RuContentEncoding::Zstd,
            ContentEncoding::Lz4 => RuContentEncoding::Lz4,
            ContentEncoding::Zlib => RuContentEncoding::Zlib,
            ContentEncoding::Null => RuContentEncoding::None,
        }
    }
}

#[pyclass(from_py_object, get_all, set_all)]
#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub enabled: bool,
    pub max_batch_size: usize,
    pub max_delay_ms: u64,
    pub max_payload_bytes: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_batch_size: 100,
            max_delay_ms: 0,
            max_payload_bytes: 1024,
        }
    }
}

#[pymethods]
impl BatchConfig {
    #[new]
    #[pyo3(signature = (enabled=true, max_batch_size=100, max_delay_ms=0, max_payload_bytes=1024))]
    pub fn new(enabled: bool, max_batch_size: usize, max_delay_ms: u64, max_payload_bytes: usize) -> Self {
        Self {
            enabled,
            max_batch_size,
            max_delay_ms,
            max_payload_bytes,
        }
    }

    #[staticmethod]
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Self {
        Default::default()
    }
}

type AckSender = tokio::sync::oneshot::Sender<Result<(), Box<dyn std::error::Error + Send + Sync>>>;

pub struct AckInner {
    pub tx: std::sync::Mutex<Option<AckSender>>,
}

impl Drop for AckInner {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.lock().ok().and_then(|mut g| g.take()) {
            let _ = tx.send(Err(Box::new(std::io::Error::other("Delivery dropped without ACK/NACK"))));
        }
    }
}

#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct DeliveryAck {
    pub inner: Arc<AckInner>,
}

#[pymethods]
impl DeliveryAck {
    pub fn ack(&self) {
        if let Some(tx) = self.inner.tx.lock().ok().and_then(|mut g| g.take()) {
            let _ = tx.send(Ok(()));
        }
    }

    pub fn nack(&self, err: Option<String>) {
        if let Some(tx) = self.inner.tx.lock().ok().and_then(|mut g| g.take()) {
            let msg = err.unwrap_or_else(|| "Delivery nacked by subscriber".to_string());
            let _ = tx.send(Err(Box::new(std::io::Error::other(msg))));
        }
    }
}

#[pyclass(from_py_object, get_all, set_all)]
#[derive(Debug, Clone)]
pub struct ConfigOptions {
    pub queue_name: String,
    pub rpc_exchange_name: String,
    pub rpc_queue_name: String,
    pub dead_letter_exchange: Option<String>,
    pub dead_letter_routing_key: Option<String>,
    pub batch_config: Option<BatchConfig>,
    pub max_pending_commands: Option<usize>,
    pub max_pending_bytes: Option<usize>,
    pub fail_fast_on_disconnect: Option<bool>,
    pub sub_exchange: Option<ExchangeOptions>,
    pub sub_queue: Option<QueueOptions>,
    pub rpc_exchange: Option<ExchangeOptions>,
    pub rpc_queue: Option<QueueOptions>,
}
#[pymethods]
impl ConfigOptions {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (queue_name, rpc_exchange_name, rpc_queue_name, dead_letter_exchange=None, dead_letter_routing_key=None, batch_config=None, max_pending_commands=None, max_pending_bytes=None, fail_fast_on_disconnect=None, sub_exchange=None, sub_queue=None, rpc_exchange=None, rpc_queue=None))]
    fn new(
        queue_name: String,
        rpc_exchange_name: String,
        rpc_queue_name: String,
        dead_letter_exchange: Option<String>,
        dead_letter_routing_key: Option<String>,
        batch_config: Option<BatchConfig>,
        max_pending_commands: Option<usize>,
        max_pending_bytes: Option<usize>,
        fail_fast_on_disconnect: Option<bool>,
        sub_exchange: Option<ExchangeOptions>,
        sub_queue: Option<QueueOptions>,
        rpc_exchange: Option<ExchangeOptions>,
        rpc_queue: Option<QueueOptions>,
    ) -> Self {
        Self {
            queue_name,
            rpc_exchange_name,
            rpc_queue_name,
            dead_letter_exchange,
            dead_letter_routing_key,
            batch_config,
            max_pending_commands,
            max_pending_bytes,
            fail_fast_on_disconnect,
            sub_exchange,
            sub_queue,
            rpc_exchange,
            rpc_queue,
        }
    }
}
impl From<ConfigOptions> for RuConfigOptions {
    fn from(options: ConfigOptions) -> Self {
        let sub_queue = options
            .sub_queue
            .and_then(|q| QueueOptions::to_ru_options(q).ok())
            .unwrap_or_else(|| RuQueueOptions::new().durable(true));
        let rpc_queue = options
            .rpc_queue
            .and_then(|q| QueueOptions::to_ru_options(q).ok())
            .unwrap_or_else(|| RuQueueOptions::new().durable(true));
        Self {
            queue_name: options.queue_name,
            rpc_exchange_name: options.rpc_exchange_name,
            rpc_queue_name: options.rpc_queue_name,
            dead_letter_exchange: options.dead_letter_exchange,
            dead_letter_routing_key: options.dead_letter_routing_key,
            max_pending_commands: options.max_pending_commands.unwrap_or(10_000),
            max_pending_bytes: options.max_pending_bytes.unwrap_or(64 * 1024 * 1024),
            fail_fast_on_disconnect: options.fail_fast_on_disconnect.unwrap_or(false),
            default_command_timeout: std::time::Duration::from_secs(16),
            max_reconnect_delay: 30,
            sub_exchange: options.sub_exchange.map(Into::into).unwrap_or_else(RuExchangeOptions::topic),
            sub_queue,
            rpc_exchange: options.rpc_exchange.map(Into::into).unwrap_or_else(RuExchangeOptions::topic),
            rpc_queue,
        }
    }
}

#[pyclass(from_py_object, get_all, set_all)]
#[derive(Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub virtual_host: String,
    pub options: ConfigOptions,
    pub tls_adaptor: Option<TlsAdaptor>, // Placeholder for TLS adaptor
}
#[pymethods]
impl Config {
    #[new]
    #[pyo3(signature = (host, port, username, password, virtual_host, options, tls_adaptor=None))]
    fn new(
        host: String,
        port: u16,
        username: String,
        password: String,
        virtual_host: String,
        options: ConfigOptions,
        tls_adaptor: Option<TlsAdaptor>,
    ) -> Self {
        Self {
            host,
            port,
            username,
            password,
            virtual_host,
            options,
            tls_adaptor,
        }
    }
}

impl From<Config> for RuConfig {
    fn from(config: Config) -> Self {
        Self {
            host: config.host,
            port: config.port,
            username: config.username,
            password: config.password,
            virtual_host: config.virtual_host,
            options: config.options.into(),
            tls_adaptor: config.tls_adaptor.map(|t| t.into()),
        }
    }
}
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct TlsAdaptor {
    pub(crate) inner: Arc<RuTlsAdaptor>,
}

#[pymethods]
impl TlsAdaptor {
    #[staticmethod]
    pub fn with_client_auth(
        ca_path: Option<PathBuf>,
        cert_path: PathBuf,
        key_path: PathBuf,
        domain: String,
    ) -> PyResult<Self> {
        amqp_rs_core::install_crypto_provider()?;
        let (connection, domain) = amqp_rs_core::with_client_auth(
            ca_path.as_deref(),
            cert_path.as_path(),
            key_path.as_path(),
            domain,
        )?;
        let tls_adaptor = RuTlsAdaptor::new(connection, domain);
        let inner = Arc::new(tls_adaptor);
        Ok(Self { inner })
    }
    #[staticmethod]
    pub fn without_client_auth(root_ca_cert: Option<PathBuf>, domain: String) -> PyResult<Self> {
        amqp_rs_core::install_crypto_provider()?;
        let inner = Arc::new(
            RuTlsAdaptor::without_client_auth(root_ca_cert.as_deref(), domain)
                .map_err(|e| PyValueError::new_err(e.to_string()))?,
        );
        Ok(Self { inner })
    }
}
impl From<TlsAdaptor> for RuTlsAdaptor {
    fn from(adaptor: TlsAdaptor) -> Self {
        Arc::unwrap_or_clone(adaptor.inner)
    }
}

#[pyclass(from_py_object, get_all, set_all)]
#[derive(Debug, Clone)]
pub struct QoSConfig {
    pub pub_confirm: bool,
    pub rpc_client_confirm: bool,
    pub rpc_server_confirm: bool,
    pub sub_auto_ack: bool,
    pub rpc_server_auto_ack: bool,
    pub rpc_client_auto_ack: bool,
    pub sub_prefetch: Option<u16>,
    pub rpc_server_prefetch: Option<u16>,
    pub rpc_client_prefetch: Option<u16>,
}
impl Default for QoSConfig {
    fn default() -> Self {
        Self {
            pub_confirm: true,
            rpc_client_confirm: true,
            rpc_server_confirm: false,
            sub_auto_ack: false,
            rpc_server_auto_ack: false,
            rpc_client_auto_ack: true,
            sub_prefetch: None,
            rpc_server_prefetch: None,
            rpc_client_prefetch: None,
        }
    }
}

#[pymethods]
impl QoSConfig {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (pub_confirm=true, rpc_client_confirm=true, rpc_server_confirm=false, sub_auto_ack=false, rpc_server_auto_ack=false, rpc_client_auto_ack=true, sub_prefetch=None, rpc_server_prefetch=None, rpc_client_prefetch=None))]
    fn new(
        pub_confirm: bool,
        rpc_client_confirm: bool,
        rpc_server_confirm: bool,
        sub_auto_ack: bool,
        rpc_server_auto_ack: bool,
        rpc_client_auto_ack: bool,
        sub_prefetch: Option<u16>,
        rpc_server_prefetch: Option<u16>,
        rpc_client_prefetch: Option<u16>,
    ) -> Self {
        Self {
            pub_confirm,
            rpc_client_confirm,
            rpc_server_confirm,
            sub_auto_ack,
            rpc_server_auto_ack,
            rpc_client_auto_ack,
            sub_prefetch,
            rpc_server_prefetch,
            rpc_client_prefetch,
        }
    }

    #[staticmethod]
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Self {
        Default::default()
    }
}
impl From<QoSConfig> for RuQoSConfig {
    fn from(config: QoSConfig) -> Self {
        Self {
            pub_confirm: config.pub_confirm,
            rpc_client_confirm: config.rpc_client_confirm,
            rpc_server_confirm: config.rpc_server_confirm,
            sub_auto_ack: config.sub_auto_ack,
            rpc_server_auto_ack: config.rpc_server_auto_ack,
            rpc_client_auto_ack: config.rpc_client_auto_ack,
            sub_prefetch: config.sub_prefetch,
            rpc_server_prefetch: config.rpc_server_prefetch,
            rpc_client_prefetch: config.rpc_client_prefetch,
        }
    }
}

#[derive(FromPyObject)]
pub enum Payload<'py> {
    Bytes(Bound<'py, PyBytes>),
    Str(Bound<'py, PyString>),
    ByteArray(Bound<'py, pyo3::types::PyByteArray>),
    Message(Bound<'py, Message>),
}

impl<'py> Payload<'py> {
    #[inline]
    pub fn as_bytes(&self) -> PyResult<&[u8]> {
        match self {
            Payload::Bytes(b) => Ok(b.as_bytes()),
            Payload::Str(s) => Ok(s.to_str()?.as_bytes()),
            Payload::ByteArray(ba) => Ok(unsafe { ba.as_bytes() }),
            Payload::Message(m) => Ok(&m.get().body),
        }
    }

    #[inline]
    pub fn into_vec(self) -> PyResult<Vec<u8>> {
        match self {
            Payload::Bytes(b) => Ok(b.as_bytes().to_vec()),
            Payload::Str(s) => Ok(s.to_str()?.as_bytes().to_vec()),
            Payload::ByteArray(ba) => Ok(unsafe { ba.as_bytes().to_vec() }),
            Payload::Message(m) => Ok(m.get().body.to_vec()),
        }
    }

    #[inline]
    pub fn to_arc_bytes(&self) -> Arc<[u8]> {
        match self {
            Payload::Bytes(b) => b.as_bytes().into(),
            Payload::Str(s) => s.to_str().map(|s| s.as_bytes().into()).unwrap_or_else(|_| Arc::from([])),
            Payload::ByteArray(ba) => (unsafe { ba.as_bytes() }).into(),
            Payload::Message(m) => m.get().body.clone(),
        }
    }

    #[inline]
    pub fn content_type(&self) -> Option<String> {
        match self {
            Payload::Message(m) => m.get().content_type.clone(),
            _ => None,
        }
    }
}
