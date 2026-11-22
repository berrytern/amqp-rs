use std::{collections::HashMap, path::Path, sync::Arc};

use amqp_client_rust::{
  amqprs::tls::TlsAdaptor as RuTlsAdaptor,
  api::{
    eventbus::AsyncEventbusRabbitMQ as RuAsyncEventbusRabbitMQ,
    utils::{
      ContentEncoding as RuContentEncoding, DeliveryMode as RuDeliveryMode,
      ExchangeOptions as RuExchangeOptions, ExchangeType as RuExchangeType,
      Message as RuMessage, PublishOptions as RuPublishOptions, QueueOptions as RuQueueOptions,
      RpcClientOptions as RuRpcClientOptions, Topology as RuTopology,
    },
  },
  domain::config::{
    Config as RuConfig, ConfigOptions as RuConfigOptions, QoSConfig as RuQoSConfig,
  },
};
use amqp_rs_core::*;
use napi::{
  bindgen_prelude::*,
  threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode, UnknownReturnValue},
};
use napi_derive::napi;

pub mod exceptions;
use exceptions::AppError;

#[napi]
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

#[napi(string_enum)]
#[derive(Debug, Clone)]
pub enum ContentEncoding {
  Zstd,
  Lz4,
  Zlib,
  Null,
}

impl From<ContentEncoding> for RuContentEncoding {
  fn from(val: ContentEncoding) -> Self {
    match val {
      ContentEncoding::Zstd => RuContentEncoding::Zstd,
      ContentEncoding::Lz4 => RuContentEncoding::Lz4,
      ContentEncoding::Zlib => RuContentEncoding::Zlib,
      ContentEncoding::Null => RuContentEncoding::None,
    }
  }
}

#[napi(string_enum)]
#[derive(Debug, Clone, Copy)]
pub enum ExchangeType {
  Direct,
  Fanout,
  Topic,
  Headers,
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

#[napi(object)]
#[derive(Debug, Clone)]
pub struct ExchangeOptions {
  pub exchange_type: Option<ExchangeType>,
  pub durable: Option<bool>,
  pub auto_delete: Option<bool>,
  pub internal: Option<bool>,
  pub no_create: Option<bool>,
}

impl From<ExchangeOptions> for RuExchangeOptions {
  fn from(opts: ExchangeOptions) -> Self {
    RuExchangeOptions {
      exchange_type: opts.exchange_type.map(Into::into).unwrap_or(RuExchangeType::Topic),
      durable: opts.durable.unwrap_or(true),
      auto_delete: opts.auto_delete.unwrap_or(false),
      internal: opts.internal.unwrap_or(false),
      no_create: opts.no_create.unwrap_or(false),
    }
  }
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct QueueOptions {
  pub auto_delete: Option<bool>,
  pub durable: Option<bool>,
  pub exclusive: Option<bool>,
  pub no_create: Option<bool>,
  pub dead_letter_exchange: Option<String>,
  pub dead_letter_routing_key: Option<String>,
  pub message_ttl: Option<u32>,
  pub expires: Option<u32>,
  pub max_length: Option<u32>,
  pub max_priority: Option<u8>,
  pub arguments: Option<HashMap<String, String>>,
}

impl TryFrom<QueueOptions> for RuQueueOptions {
  type Error = napi::Error;

  fn try_from(opts: QueueOptions) -> std::result::Result<Self, Self::Error> {
    let mut q = RuQueueOptions::new()
      .auto_delete(opts.auto_delete.unwrap_or(false))
      .durable(opts.durable.unwrap_or(true))
      .exclusive(opts.exclusive.unwrap_or(false))
      .no_create(opts.no_create.unwrap_or(false));
    if let Some(dlx) = opts.dead_letter_exchange {
      q = q.dead_letter_exchange(dlx);
    }
    if let Some(dlk) = opts.dead_letter_routing_key {
      q = q.dead_letter_routing_key(dlk);
    }
    if let Some(ttl) = opts.message_ttl {
      q = q.message_ttl(ttl);
    }
    if let Some(exp) = opts.expires {
      q = q.expires(exp);
    }
    if let Some(max_len) = opts.max_length {
      q = q.max_length(max_len);
    }
    if let Some(max_prio) = opts.max_priority {
      q = q.max_priority(max_prio);
    }
    if let Some(args) = opts.arguments {
      q = q.arguments(&args).map_err(|e| Error::new(Status::InvalidArg, e.to_string()))?;
    }
    Ok(q)
  }
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct TopologyExchange {
  pub name: String,
  pub options: Option<ExchangeOptions>,
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct TopologyQueue {
  pub name: String,
  pub options: Option<QueueOptions>,
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct TopologyBinding {
  pub queue_name: String,
  pub exchange_name: String,
  pub routing_key: String,
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct Topology {
  pub exchanges: Option<Vec<TopologyExchange>>,
  pub queues: Option<Vec<TopologyQueue>>,
  pub bindings: Option<Vec<TopologyBinding>>,
}

impl TryFrom<Topology> for RuTopology {
  type Error = napi::Error;

  fn try_from(topo: Topology) -> std::result::Result<Self, Self::Error> {
    let mut ru = RuTopology::new();
    if let Some(exchanges) = topo.exchanges {
      for ex in exchanges {
        ru = ru.exchange(ex.name, ex.options.map(Into::into).unwrap_or_default());
      }
    }
    if let Some(queues) = topo.queues {
      for q in queues {
        let ru_q = match q.options {
          Some(opts) => RuQueueOptions::try_from(opts)?,
          None => RuQueueOptions::new().durable(true),
        };
        ru = ru.queue(q.name, ru_q);
      }
    }
    if let Some(bindings) = topo.bindings {
      for b in bindings {
        ru = ru.bind(b.queue_name, b.exchange_name, b.routing_key);
      }
    }
    Ok(ru)
  }
}

#[napi(object)]
pub struct Message {
  pub body: Buffer,
  pub content_type: Option<String>,
  pub routing_key: Option<String>,
  pub exchange: Option<String>,
  pub redelivered: Option<bool>,
  pub delivery_tag: Option<i64>,
  pub correlation_id: Option<String>,
  pub message_id: Option<String>,
  pub app_id: Option<String>,
  pub timestamp: Option<i64>,
  pub headers: Option<HashMap<String, String>>,
}

impl From<RuMessage> for Message {
  fn from(msg: RuMessage) -> Self {
    let meta = msg.metadata.as_deref();
    Self {
      body: Buffer::from(msg.body.as_ref()),
      content_type: msg.content_type,
      routing_key: meta.and_then(|m| m.routing_key().map(str::to_owned)),
      exchange: meta.and_then(|m| m.exchange().map(str::to_owned)),
      redelivered: meta.map(|m| m.redelivered()),
      delivery_tag: meta.and_then(|m| m.delivery_tag().map(|t| t as i64)),
      correlation_id: meta.and_then(|m| m.correlation_id().map(str::to_owned)),
      message_id: meta.and_then(|m| m.message_id().map(str::to_owned)),
      app_id: meta.and_then(|m| m.app_id().map(str::to_owned)),
      timestamp: meta.and_then(|m| m.timestamp().map(|t| t as i64)),
      headers: meta.and_then(|m| m.headers()),
    }
  }
}

impl From<Message> for RuMessage {
  fn from(msg: Message) -> Self {
    Self {
      body: msg.body.to_vec().into(),
      content_type: msg.content_type,
      metadata: None,
    }
  }
}

#[napi(object)]
#[derive(Debug, Clone)]
pub struct ConfigOptions {
  pub queue_name: String,
  pub rpc_exchange_name: String,
  pub rpc_queue_name: String,
  pub dead_letter_exchange: Option<String>,
  pub dead_letter_routing_key: Option<String>,
  pub max_pending_commands: Option<u32>,
  pub max_pending_bytes: Option<u32>,
  pub fail_fast_on_disconnect: Option<bool>,
  pub sub_exchange: Option<ExchangeOptions>,
  pub sub_queue: Option<QueueOptions>,
  pub rpc_exchange: Option<ExchangeOptions>,
  pub rpc_queue: Option<QueueOptions>,
}

impl From<ConfigOptions> for RuConfigOptions {
  fn from(options: ConfigOptions) -> Self {
    let sub_queue = options
      .sub_queue
      .and_then(|q| RuQueueOptions::try_from(q).ok())
      .unwrap_or_else(|| RuQueueOptions::new().durable(true));
    let rpc_queue = options
      .rpc_queue
      .and_then(|q| RuQueueOptions::try_from(q).ok())
      .unwrap_or_else(|| RuQueueOptions::new().durable(true));
    Self {
      queue_name: options.queue_name,
      rpc_exchange_name: options.rpc_exchange_name,
      rpc_queue_name: options.rpc_queue_name,
      dead_letter_exchange: options.dead_letter_exchange,
      dead_letter_routing_key: options.dead_letter_routing_key,
      max_pending_commands: options.max_pending_commands.map(|v| v as usize).unwrap_or(10_000),
      max_pending_bytes: options.max_pending_bytes.map(|v| v as usize).unwrap_or(64 * 1024 * 1024),
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
    let (connector, domain) = with_client_auth(
      ca_path_ref,
      Path::new(&cert_path),
      Path::new(&key_path),
      domain,
    )?;
    let inner = Arc::new(RuTlsAdaptor::new(connector, domain));

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
      eventbus: Arc::new(RuAsyncEventbusRabbitMQ::new(
        RuConfig::from(config),
        qos_config.into(),
      )),
    })
  }

  #[napi]
  pub async fn declare_exchange(
    &self,
    exchange_name: String,
    options: Option<ExchangeOptions>,
    command_timeout: Option<u32>,
  ) -> Result<()> {
    let ru_opts = options.map(Into::into).unwrap_or_default();
    let cmd_timeout = command_timeout.map(|t| std::time::Duration::from_secs(t as u64));
    self
      .eventbus
      .declare_exchange(&exchange_name, ru_opts, cmd_timeout)
      .await
      .map_err(AppError::from)?;
    Ok(())
  }

  #[napi]
  pub async fn declare_topology(
    &self,
    topology: Topology,
    command_timeout: Option<u32>,
  ) -> Result<()> {
    let ru_topology = RuTopology::try_from(topology)?;
    let cmd_timeout = command_timeout.map(|t| std::time::Duration::from_secs(t as u64));
    self
      .eventbus
      .declare_topology(ru_topology, cmd_timeout)
      .await
      .map_err(AppError::from)?;
    Ok(())
  }

  #[napi]
  #[allow(clippy::too_many_arguments)]
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

    let cmd_timeout = command_timeout.map(|t| std::time::Duration::from_secs(t as u64));

    let pub_opts = RuPublishOptions {
      content_type: content_type.as_deref().unwrap_or("application/json"),
      content_encoding: content_encoding.into(),
      command_timeout: cmd_timeout,
      delivery_mode: delivery_mode.into(),
      expiration,
    };

    self
      .eventbus
      .publish(
        &exchange_name,
        &routing_key,
        payload_bytes,
        &pub_opts,
      )
      .await
      .map_err(AppError::from)?;

    Ok(())
  }

  #[napi]
  #[allow(clippy::too_many_arguments)]
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

    let rpc_opts = RuRpcClientOptions {
      content_type: &content_type,
      content_encoding: content_encoding.into(),
      response_timeout_millis: response_timeout,
      command_timeout: conn_timeout,
      delivery_mode: delivery_mode.into(),
      expiration,
    };

    let res = self
      .eventbus
      .rpc_client(
        &exchange_name,
        &routing_key,
        payload_bytes,
        &rpc_opts,
      )
      .await
      .map_err(AppError::from)?;

    Ok(res.into())
  }

  #[napi]
  pub async fn subscribe(
    &self,
    exchange_name: String,
    routing_key: String,
    #[napi(ts_arg_type = "(msg: Message) => Promise<void>")] handler: Arc<
      ThreadsafeFunction<Message, UnknownReturnValue>,
    >,
    process_timeout: Option<u32>,
    command_timeout: Option<u32>,
  ) -> Result<()> {
    let process_timeout = process_timeout.map(|t| std::time::Duration::from_secs(t as u64));
    let command_timeout = command_timeout.map(|t| std::time::Duration::from_secs(t as u64));

    self
      .eventbus
      .subscribe(
        &exchange_name,
        &routing_key,
        move |body| {
          let handler = handler.clone();
          async move {
            let status = handler.call(
              Ok(Message::from(body)),
              ThreadsafeFunctionCallMode::NonBlocking,
            );
            if status == napi::Status::Ok {
              Ok(())
            } else {
              Err(Box::new(std::io::Error::other(
                format!("ThreadsafeFunction call failed with status: {:?}", status),
              )) as Box<dyn std::error::Error + Send + Sync>)
            }
          }
        },
        process_timeout,
        command_timeout,
      )
      .await
      .map_err(AppError::from)?;

    Ok(())
  }

  #[napi]
  pub async fn provide_resource(
    &self,
    routing_key: String,
    #[napi(ts_arg_type = "(msg: Message) => Promise<Buffer | Message>")] handler: Arc<
      ThreadsafeFunction<Message, Message>,
    >,
    process_timeout: Option<u32>,
    command_timeout: Option<u32>,
  ) -> Result<()> {
    let process_timeout = process_timeout.map(|t| std::time::Duration::from_secs(t as u64));
    let command_timeout = command_timeout.map(|t| std::time::Duration::from_secs(t as u64));

    self
      .eventbus
      .provide_resource(
        &routing_key,
        move |body| {
          let handler = handler.clone();
          async move {
            // In JS, handlers should return a Buffer (or a JS object mirroring Message)
            let res: Result<Message, napi::Status> =
              handler.call_async(Ok(Message::from(body))).await;
            let buf = res?;

            Ok(buf.into())
          }
        },
        process_timeout,
        command_timeout,
      )
      .await
      .map_err(AppError::from)?;

    Ok(())
  }

  #[napi]
  pub async fn dispose(&self) -> Result<()> {
    self
      .eventbus
      .dispose()
      .await
      .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))
  }
}
