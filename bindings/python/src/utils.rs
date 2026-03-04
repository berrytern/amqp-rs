use std::{collections::HashMap, sync::Arc};
use amqp_client_rust::{
    amqprs::tls::{TlsAdaptor as RuTlsAdaptor},
    api::{
        utils::{
            ContentEncoding as RuContentEncoding,
            DeliveryMode as RuDeliveryMode,
            Message as RuMessage,
            Confirmations as RuPublishConfirmation,
            QueueOptions as RuQueueOptions,
        },
    }, domain::config::{
        Config as RuConfig, ConfigOptions as RuConfigOptions, QoSConfig as RuQoSConfig,
    }
};
use pyo3::{
    exceptions::PyValueError, prelude::*, types::{PyBytes, PyString}
};
use crate::exceptions::AppError;
use std::path::PathBuf;


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
        let queue_options = RuQueueOptions::build()
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
            PublishConfirmations::Disables => RuPublishConfirmation::Disables,
            PublishConfirmations::PublisherConfirms => RuPublishConfirmation::PublisherConfirms,
            PublishConfirmations::RPCClientPublisherConfirms => RuPublishConfirmation::RPCClientPublisherConfirms,
            PublishConfirmations::RPCServerPublisherConfirms => RuPublishConfirmation::RPCServerPublisherConfirms,
        }
    }
}

#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct Message {
    body: Arc<[u8]>,
    content_type: Option<String>,
}
impl From<RuMessage> for Message {
    fn from(msg: RuMessage) -> Self {
        Self {
            body: msg.body,
            content_type: msg.content_type,
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
        })
    }

    #[getter]
    fn body<'py>(slf: PyRef<'py, Self>) -> Bound<'py, PyBytes> {
        PyBytes::new(slf.py(), &slf.body)
    }
    #[getter]
    fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }
}

#[pyclass(from_py_object, get_all, set_all)]
#[derive(Debug, Clone)]
pub enum ContentEncoding {
    Zstd,
    Lz4,
    Zlib,
    Null,
}
impl Into<RuContentEncoding> for ContentEncoding {
    fn into(self) -> RuContentEncoding {
        match self {
            ContentEncoding::Zstd => RuContentEncoding::Zstd,
            ContentEncoding::Lz4 => RuContentEncoding::Lz4,
            ContentEncoding::Zlib => RuContentEncoding::Zlib,
            ContentEncoding::Null => RuContentEncoding::None,
        }
    }
}

#[pyclass(from_py_object, get_all, set_all)]
#[derive(Debug, Clone)]
pub struct ConfigOptions {
    queue_name: String,
    rpc_exchange_name: String,
    rpc_queue_name: String,
}
#[pymethods]
impl ConfigOptions {
    #[new]
    fn new(queue_name: String, rpc_exchange_name: String, rpc_queue_name: String) -> Self {
        Self {
            queue_name,
            rpc_exchange_name,
            rpc_queue_name,
        }
    }
}
impl From<ConfigOptions> for RuConfigOptions {
    fn from(options: ConfigOptions) -> Self {
        Self {
            queue_name: options.queue_name,
            rpc_exchange_name: options.rpc_exchange_name,
            rpc_queue_name: options.rpc_queue_name,
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
        let (connection, domain) = amqp_rs_core::with_client_auth(ca_path.as_deref(), cert_path.as_path(), key_path.as_path(), domain)?;
        let tls_adaptor = RuTlsAdaptor::new(connection, domain);
        let inner = Arc::new(
            tls_adaptor
        );
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
#[pymethods]
impl QoSConfig {
    #[new]
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
    pub fn default() -> Self {
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