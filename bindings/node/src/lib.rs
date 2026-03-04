use std::{path::Path, sync::Arc};

use amqp_client_rust::{
    amqprs::tls::TlsAdaptor as RuTlsAdaptor,
    api::{
        eventbus::AsyncEventbusRabbitMQ as RuAsyncEventbusRabbitMQ,
        utils::{ContentEncoding as RuContentEncoding, DeliveryMode as RuDeliveryMode, Message as RuMessage},
    },
    domain::config::{
        Config as RuConfig, ConfigOptions as RuConfigOptions, QoSConfig as RuQoSConfig,
    },
};
use napi::{bindgen_prelude::*, threadsafe_function::{ThreadsafeFunction, UnknownReturnValue}};
use napi_derive::napi;
use amqp_rs_core::*;

pub mod exceptions;
use exceptions::AppError;

#[napi]
#[derive(Debug, Clone)]
pub enum DeliveryMode {
    Transient = 1,
    Persistent = 2
}

impl From<DeliveryMode> for RuDeliveryMode {
    fn from(mode: DeliveryMode) -> Self {
        match mode {
            DeliveryMode::Transient => RuDeliveryMode::Transient,
            DeliveryMode::Persistent => RuDeliveryMode::Persistent,
        }
    }
}

#[napi(string_enum)]
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

#[napi(object)]
pub struct Message {
    pub body: Buffer,
    pub content_type: Option<String>,
}

impl From<RuMessage> for Message {
    fn from(msg: RuMessage) -> Self {
        Self {
            body: msg.body.to_vec().into(),
            content_type: msg.content_type,
        }
    }
}

impl From<Message> for RuMessage {
    fn from(msg: Message) -> Self {
        Self {
            body: msg.body.to_vec().into(),
            content_type: msg.content_type,
        }
    }
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct ConfigOptions {
    pub queue_name: String,
    pub rpc_exchange_name: String,
    pub rpc_queue_name: String,
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

#[napi]
#[derive(Clone)]
pub struct TlsAdaptor {
    pub(crate) inner: Arc<RuTlsAdaptor>,
}
#[napi]
impl TlsAdaptor {
    // ... You can copy the exact build_root_store and build_client_certificates helper
    // methods from the original file here ...
    #[napi(factory)]
    pub fn with_client_auth(
        ca_path: Option<String>,
        cert_path: String,
        key_path: String,
        domain: String,
    ) -> Result<Self> {
        let ca_path_ref = ca_path.as_deref().map(Path::new);
        install_crypto_provider()?;
        let (connector, domain) = with_client_auth(ca_path_ref, Path::new(&cert_path), Path::new(&key_path), domain)?;
        let inner = Arc::new(
            RuTlsAdaptor::new(connector, domain)
        );

        Ok(Self { inner })
    }

    #[napi(factory)]
    pub fn without_client_auth(root_ca_cert: Option<String>, domain: String) -> Result<Self> {
        install_crypto_provider()?;
        let inner = Arc::new(
            RuTlsAdaptor::without_client_auth(root_ca_cert.as_deref().map(Path::new), domain)
                .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?,
        );
        Ok(Self { inner })
    }
}

impl From<TlsAdaptor> for RuTlsAdaptor {
    fn from(adaptor: TlsAdaptor) -> Self {
        Arc::unwrap_or_clone(adaptor.inner)
    }
}

#[napi]
pub struct Config {
  pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) virtual_host: String,
    pub(crate) options: ConfigOptions,
    pub(crate) tls_adaptor: Option<TlsAdaptor>,
}

#[napi]
impl Config {
    #[napi(constructor)]
    pub fn new(
        host: String,
        port: u16,
        username: String,
        password: String,
        virtual_host: String,
        options: ConfigOptions,
        tls_adaptor: Option<&TlsAdaptor>,
    ) -> Self {
        Self {
            host,
            port,
            username,
            password,
            virtual_host,
            options,
            tls_adaptor: tls_adaptor.cloned(),
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
            tls_adaptor: config.tls_adaptor.map(|t: TlsAdaptor| t.into()),
        }
    }
}


#[napi(object)]
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

#[napi]
#[derive(Clone)]
pub struct AsyncEventbus {
    eventbus: Arc<RuAsyncEventbusRabbitMQ>,
}
impl From<&Config> for RuConfig {
    fn from(config: &Config) -> Self {
        Self {
            host: config.host.clone(),
            port: config.port,
            username: config.username.clone(),
            password: config.password.clone(),
            virtual_host: config.virtual_host.clone(),
            options: config.options.clone().into(),
            tls_adaptor: config.tls_adaptor.as_ref().map(|t| t.clone().into()),
        }
    }
}
#[napi]
impl AsyncEventbus {
    #[napi(factory)] // This tells NAPI to bind this as a static method
    pub async fn connect(config: &Config, qos_config: QoSConfig) -> napi::Result<Self> {
        Ok(Self {
            eventbus: Arc::new(RuAsyncEventbusRabbitMQ::new(RuConfig::from(config), qos_config.into())),
        })
    }

    #[napi]
    pub async fn publish(
        &self,
        exchange_name: String,
        routing_key: String,
        body: Either<Buffer, String>,
        content_type: Option<String>,
        content_encoding: ContentEncoding,
        command_timeout: Option<u32>,
        delivery_mode: DeliveryMode,
        expiration: Option<u32>,
    ) -> Result<()> {
        let payload_bytes = match body {
            Either::A(b) => b.to_vec(),
            Either::B(s) => s.into_bytes(),
        };

        let command_timeout = command_timeout.map(|t| std::time::Duration::from_secs(t as u64));

        self.eventbus
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
            .map_err(|e| AppError::from(e))?;

        Ok(())
    }

    #[napi]
    pub async fn rpc_client(
        &self,
        exchange_name: String,
        routing_key: String,
        body: Either<Buffer, String>,
        content_type: String,
        content_encoding: ContentEncoding,
        response_timeout: u32,
        connection_timeout: Option<u32>,
        delivery_mode: DeliveryMode,
        expiration: Option<u32>,
    ) -> Result<Buffer> {
        let payload_bytes = match body {
            Either::A(b) => b.to_vec(),
            Either::B(s) => s.into_bytes(),
        };

        let conn_timeout = connection_timeout.map(|t| std::time::Duration::from_secs(t as u64));

        let res = self.eventbus
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
            .map_err(|e| AppError::from(e))?;

        Ok(res.into())
    }

    #[napi]
    pub async fn subscribe(
        &self,
        exchange_name: String,
        routing_key: String,
        #[napi(ts_arg_type = "(msg: Message) => Promise<void>")] handler: Arc<ThreadsafeFunction<Message, UnknownReturnValue>>,
        process_timeout: Option<u32>,
        command_timeout: Option<u32>,
    ) -> Result<()> {
        let process_timeout = process_timeout.map(|t| std::time::Duration::from_secs(t as u64));
        let command_timeout = command_timeout.map(|t| std::time::Duration::from_secs(t as u64));

        self.eventbus
            .subscribe(
                &exchange_name,
                &routing_key,
                move |body| {
                    let handler = handler.clone();
                    async move {
                        // Explicit type annotation added for `.await` resolution
                        let res: Result<UnknownReturnValue, napi::Status> = handler.call_async(Ok(Message::from(body))).await;
                        res.map(|_| ()).map_err(|e| {
                            e.into()
                        })
                    }
                },
                process_timeout,
                command_timeout,
            )
            .await
            .map_err(|e| AppError::from(e))?;

        Ok(())
    }

    #[napi]
    pub async fn provide_resource(
        &self,
        routing_key: String,
        #[napi(ts_arg_type = "(msg: Message) => Promise<Buffer | Message>")] handler: Arc<ThreadsafeFunction<Message, Message>>,
        process_timeout: Option<u32>,
        command_timeout: Option<u32>,
    ) -> Result<()> {
        let process_timeout = process_timeout.map(|t| std::time::Duration::from_secs(t as u64));
        let command_timeout = command_timeout.map(|t| std::time::Duration::from_secs(t as u64));

        self.eventbus
            .provide_resource(
                &routing_key,
                move |body| {
                    let handler = handler.clone();
                    async move {
                        // In JS, handlers should return a Buffer (or a JS object mirroring Message)
                        let res: Result<Message, napi::Status> = handler.call_async(Ok(Message::from(body))).await;
                        let buf = res?;
                        
                        Ok(buf.into())
                    }
                },
                process_timeout,
                command_timeout,
            )
            .await
            .map_err(|e| AppError::from(e))?;

        Ok(())
    }

    #[napi]
    pub async fn dispose(&self) -> Result<()> {
        self.eventbus.dispose().await.map_err(|_| {
            Error::new(Status::GenericFailure, "Unexpected Result Error during dispose")
        })
    }
}