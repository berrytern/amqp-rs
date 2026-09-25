use crate::exceptions::AppError;
use amqp_client_rust::{
    amqprs::tls::TlsAdaptor as RuTlsAdaptor,
    api::utils::{
        Confirmations as RuPublishConfirmation, ContentEncoding as RuContentEncoding,
        DeliveryMode as RuDeliveryMode, Message as RuMessage, QueueOptions as RuQueueOptions,
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
#[derive(Debug, Clone)]
pub struct QueueOptions {
    pub auto_delete: bool,
    pub durable: bool,
    pub exclusive: bool,
    pub no_create: bool,
    pub arguments: HashMap<String, String>,
}
#[pymethods]
impl QueueOptions {
    #[new]
    fn new(
        auto_delete: bool,
        durable: bool,
        exclusive: bool,
        no_create: bool,
        arguments: HashMap<String, String>,
    ) -> Self {
        Self {
            auto_delete,
            durable,
            exclusive,
            no_create,
            arguments,
        }
    }
}

impl QueueOptions {
    pub fn to_ru_options(options: QueueOptions) -> Result<RuQueueOptions, AppError> {
        let queue_options = RuQueueOptions::new()
            .auto_delete(options.auto_delete)
            .durable(options.durable)
            .exclusive(options.exclusive)
            .no_create(options.no_create)
            .arguments(&options.arguments)?;
        Ok(queue_options)
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

#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct Message {
    body: Arc<[u8]>,
    content_type: Option<String>,
    cached_body: Arc<std::sync::OnceLock<Py<PyBytes>>>,
}
impl From<RuMessage> for Message {
    fn from(msg: RuMessage) -> Self {
        Self {
            body: msg.body,
            content_type: msg.content_type,
            cached_body: Arc::new(std::sync::OnceLock::new()),
        }
    }
}
impl From<Message> for RuMessage {
    fn from(msg: Message) -> Self {
        Self {
            body: msg.body,
            content_type: msg.content_type,
        }
    }
}

#[pymethods]
impl Message {
    #[staticmethod]
    fn new<'py>(body: Payload<'py>, content_type: Option<String>) -> PyResult<Self> {
        let body: Arc<[u8]> = match body {
            Payload::Bytes(b) => b.as_bytes().into(),
            Payload::Str(s) => s.to_str()?.as_bytes().into(),
        };
        Ok(Self {
            body,
            content_type,
            cached_body: Arc::new(std::sync::OnceLock::new()),
        })
    }

    #[getter]
    fn body<'py>(slf: PyRef<'py, Self>) -> Bound<'py, PyBytes> {
        let py = slf.py();
        let cached = slf.cached_body.get_or_init(|| {
            PyBytes::new(py, &slf.body).unbind()
        });
        cached.bind(py).clone()
    }

    #[getter]
    fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
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
            return Err(PyErr::fetch(slf.py()));
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

#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct DeliveryAck {
    pub ack_tx: Arc<std::sync::Mutex<Option<AckSender>>>,
}

#[pymethods]
impl DeliveryAck {
    pub fn ack(&self) {
        if let Some(tx) = self.ack_tx.lock().ok().and_then(|mut g| g.take()) {
            let _ = tx.send(Ok(()));
        }
    }

    pub fn nack(&self, err: Option<String>) {
        if let Some(tx) = self.ack_tx.lock().ok().and_then(|mut g| g.take()) {
            let msg = err.unwrap_or_else(|| "Delivery nacked by subscriber".to_string());
            let _ = tx.send(Err(Box::new(std::io::Error::other(msg))));
        }
    }
}

impl Drop for DeliveryAck {
    fn drop(&mut self) {
        if let Some(tx) = self.ack_tx.lock().ok().and_then(|mut g| g.take()) {
            let _ = tx.send(Err(Box::new(std::io::Error::other("Delivery dropped without ACK/NACK"))));
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
}
#[pymethods]
impl ConfigOptions {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (queue_name, rpc_exchange_name, rpc_queue_name, dead_letter_exchange=None, dead_letter_routing_key=None, batch_config=None, max_pending_commands=None, max_pending_bytes=None, fail_fast_on_disconnect=None))]
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
        }
    }
}
impl From<ConfigOptions> for RuConfigOptions {
    fn from(options: ConfigOptions) -> Self {
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
            rpc_client_auto_ack: false,
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
    #[pyo3(signature = (pub_confirm=true, rpc_client_confirm=true, rpc_server_confirm=false, sub_auto_ack=false, rpc_server_auto_ack=false, rpc_client_auto_ack=false, sub_prefetch=None, rpc_server_prefetch=None, rpc_client_prefetch=None))]
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
}
