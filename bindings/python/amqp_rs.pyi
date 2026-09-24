from typing import Callable, Optional, Awaitable, Union
from concurrent.futures import Future
from enum import Enum


class Message:
    body: bytes
    content_type: Optional[str]

    @staticmethod
    def new(body: bytes, content_type: Optional[str] = None) -> "Message": ...

    def __buffer__(self, flags: int) -> memoryview: ...

class DeliveryMode(Enum):
    Transient = 1
    Persistent = 2

class ContentEncoding(Enum):
    Zstd = 'zstd',
    Lz4 = 'lz4',
    Zlib = 'zlib',
    Null = 'null'

class BatchConfig:
    enabled: bool
    max_batch_size: int
    max_delay_ms: int
    max_payload_bytes: int
    def __init__(self, enabled: bool = True, max_batch_size: int = 100, max_delay_ms: int = 0, max_payload_bytes: int = 1024) -> None: ...
    @staticmethod
    def default() -> "BatchConfig": ...

class DeliveryAck:
    def ack(self) -> None: ...
    def nack(self, err: Optional[str] = None) -> None: ...

class ConfigOptions:
    queue_name: str
    rpc_exchange_name: str
    rpc_queue_name: str
    dead_letter_exchange: Optional[str]
    dead_letter_routing_key: Optional[str]
    batch_config: Optional[BatchConfig]
    max_pending_commands: Optional[int]
    def __init__(
        self,
        queue_name: str,
        rpc_exchange_name: str,
        rpc_queue_name: str,
        dead_letter_exchange: Optional[str] = None,
        dead_letter_routing_key: Optional[str] = None,
        batch_config: Optional[BatchConfig] = None,
        max_pending_commands: Optional[int] = None,
    ) -> None: ...

class TlsAdaptor:
    @staticmethod
    def with_client_auth(ca_path: Optional[str], cert_path: str, key_path: str, domain: str) -> "TlsAdaptor": ...
    @staticmethod
    def without_client_auth(root_ca_cert: Optional[str], domain: str) -> "TlsAdaptor": ...

class Config:
    host: str
    port: int
    username: str
    password: str
    virtual_host: str
    options: ConfigOptions
    tls_adaptor: Optional[TlsAdaptor]

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        virtual_host: str,
        options: ConfigOptions,
        tls_adaptor: Optional[TlsAdaptor]
    ) -> None: ...


class QoSConfig:
    pub_confirm: bool
    rpc_client_confirm: bool
    rpc_server_confirm: bool
    sub_auto_ack: bool
    rpc_server_auto_ack: bool
    rpc_client_auto_ack: bool
    sub_prefetch: Optional[int]
    rpc_server_prefetch: Optional[int]
    rpc_client_prefetch: Optional[int]

    def __init__(self, pub_confirm: bool = True, rpc_client_confirm: bool = True, rpc_server_confirm: bool = False, sub_auto_ack: bool = False, rpc_server_auto_ack: bool = False, rpc_client_auto_ack: bool = False, sub_prefetch: Optional[int] = None, rpc_server_prefetch: Optional[int] = None, rpc_client_prefetch: Optional[int] = None) -> None:
        """
        Args:
            pub_confirm: set True to allow publisher confirmations on pub connectio
            rpc_client_confirm: set True to allow publisher confirmations on rpc client connection
            rpc_server_confirm: set True to allow publisher confirmations on rpc server connection
            sub_auto_ack: set to True to ack messages before processing on sub connection
            rpc_server_auto_ack: set to True to ack messages before processing on rpc server connection
            rpc_client_auto_ack: set to True to ack messages before processing on rpc client connection
            sub_prefetch_count: set how many messages to prefetch on sub connection
            rpc_server_prefetch_count: set how many messages to prefetch on rpc server connection
            rpc_client_prefetch_count: set how many messages to prefetch on rpc client connection
        
        Returns:
            QoSConfig object
        """
        ...
    
    def default() -> 'QoSConfig':
        ...
    

class AsyncEventbus:
    @property
    def batch_config(self) -> BatchConfig: ...

    def __init__(
        self,
        config: Config,
        qos_config: QoSConfig,
        batch_config: Optional[Union[bool, BatchConfig]] = None,
    ) -> None: ...
        """
        Create an AsyncEventbus object thats interacts with Bus
        thats provides some connection management abstractions.

        Args:
            config: the Config object
            qos_config: pass an event loop object

        Returns:
            AsyncEventbus object

        Raises:

        Examples:
            >>> async_eventbus = AsyncEventbus(
                config, qos_config)
            ### register subscribe
            >>> def handler(*body):
                    print(f"do something with: {body}")
            >>> subscribe_event = ExampleEvent("rpc_exchange")
            >>> await eventbus.subscribe(subscribe_event, handler, "user.find")
            ### provide resource
            >>> def handler2(*body):
                    print(f"do something with: {body}")
                    return "response"
            >>> await eventbus.provide_resource("user.find2", handle2)
        """
        ...

    def publish(
        self, 
        exchange_name: str,
        routing_key: str,
        body: Union[bytes, str],
        content_type: Optional[str] = "application/json",
        content_encoding: ContentEncoding = ContentEncoding.Null,
        publish_timeout: int = 16,
        connection_timeout: int = 16,
        delivery_mode: DeliveryMode = DeliveryMode.Transient,
        expiration: Optional[int] = None,
    ) -> Future[None]:
        """
        Sends a publish message to the bus following parameters passed

        Args:
            exchange: exchange name
            routing_key:  routing key name
            body: body that will be sent
            content_type: content type of message
            content_encoding: content encoding of message
            timeout: timeout in seconds for waiting for response
            connection_timeout: timeout for waiting for connection restabilishment
            delivery_mode: delivery mode
            expiration: maximum lifetime of message to stay on the queue

        Returns:
            None

        Raises:
            AutoReconnectException: when cannout reconnect on the gived timeout
            PublishTimeoutException: if publish confirmation is setted to True and \
            does not receive confirmation on the gived timeout
            NackException: if publish confirmation is setted to True and receives a nack


        Examples:
            >>> from json import dumps
            >>> exchange_name = "example.rpc"
            >>> routing_key = "user.find3"
            >>> await eventbus.publish(exchange_name, routing_key, dumps(["content_message"]), "application/json", ContentEncoding.Null, None)
        """
        ...

    def publish_batch(
        self,
        exchange_name: str,
        routing_key: str,
        messages: list[Union[bytes, str]],
        content_type: Optional[str] = "application/json",
        content_encoding: ContentEncoding = ContentEncoding.Null,
        publish_timeout: int = 16,
        delivery_mode: DeliveryMode = DeliveryMode.Transient,
    ) -> Future[None]:
        """
        Sends a batch of messages to the bus in a single FFI crossing.
        """
        ...

    def rpc_client(
        self, 
        exchange_name: str,
        routing_key: str,
        body: Union[bytes, str],
        content_type: str = "application/json",
        content_encoding: ContentEncoding = ContentEncoding.Null,
        response_timeout: int = 20_000,
        command_timeout: int = 32,
        delivery_mode: DeliveryMode = DeliveryMode.Transient,
        expiration: Optional[int] = None,
    ) -> Future[bytes]:
        """
        Sends a publish message to queue of the bus and waits for a response

        Args:
            exchange: exchange name
            routing_key:  routing key name
            body: body that will be sent
            content_type: content type of message
            content_encoding: content encoding of message
            response_timeout: timeout in seconds for waiting for response
            command_timeout: timeout for waiting for command execution
            delivery_mode: delivery mode
            expiration: maximum lifetime of message to stay on the queue

        Returns:
            bytes: response message

        Raises:
            AutoReconnectException: when cannout reconnect on the gived timeout
            PublishTimeoutException: if publish confirmation is setted to True and \
            does not receive confirmation on the gived timeout
            NackException: if publish confirmation is setted to True and receives a nack
            ResponseTimeoutException: if response timeout is reached
            RpcProviderException: if the rpc provider responded with an error

        Examples:
            >>> from json import dumps
            >>> await eventbus.rpc_client("example.rpc", "user.find", dumps([{"name": "example"}]), "application/json")
        """
        ...

    def subscribe(
        self,
        exchange_name: str,
        routing_key: str,
        handler: Callable[[Message], Any],
        process_timeout: Optional[int] = None,
        command_timeout: int = 16,
        batch_dispatch: Optional[bool] = None,
    ) -> Future[None]:
        """
        Register a provider to listen on queue of bus

        Args:
            exchange_name: exchange name
            routing_key: routing_key name
            handler: message handler, it will be called when a message is received
            process_timeout: timeout in seconds for waiting for process the received message
            command_timeout: timeout for waiting for command execution
            batch_dispatch: optional override to enable/disable FFI batch dispatching
        Returns:
            None: None
        """
        ...

    def subscribe_batch(
        self,
        exchange_name: str,
        routing_key: str,
        handler: Callable[[list[Message]], Any],
        batch_size: int = 100,
        max_delay_ms: int = 0,
        process_timeout: Optional[int] = None,
        command_timeout: int = 16,
    ) -> Future[None]:
        """
        Register a batch provider to receive batches of messages directly.

        Args:
            exchange_name: exchange name
            routing_key: routing_key name
            handler: batch handler receiving list[Message]
            batch_size: maximum number of messages per batch
            max_delay_ms: maximum delay in ms to wait for batch assembly
            process_timeout: timeout in seconds for waiting for process the received message
            command_timeout: timeout for waiting for command execution
        Returns:
            None: None
        """
        ...


    def provide_resource(
        self,
        routing_key: str,
        handler: Callable[[bytes], Awaitable[bytes]],
        process_timeout: Optional[int] = None,
        command_timeout: int = 16,
    ) -> Future[None]:
        """
        Register a provider to listen on queue of bus

        Args:
            routing_key: routing_key name
            handler: message handler, it will be called when a message is received
            process_timeout: timeout in seconds for waiting for process the received message
            command_timeout: timeout for waiting for command execution

        Returns:
            None: None


        Examples:
            >>> async def handle(body) -> Union[bytes, str]:
                    print(f"received message: {body}")
                    return b"[]"
            >>> await eventbus.provide_resource("user.find", handle)
        """
        ...
        
    def dispose(self) -> Future[None]:
        """Gracefully disposes the eventbus, closing connections and channels. Should be called when the eventbus is no longer needed to free up resources."""
        ...

class Payload:
    def __init__(self, data: bytes) -> None: ...

class PublishConfirmations(Enum):
    Disables = 0
    PublisherConfirms = 1
    RPCClientPublisherConfirms = 2
    RPCServerPublisherConfirms = 3

class QueueOptions:
    auto_delete: bool
    durable: bool
    exclusive: bool
    no_create: bool
    arguments: dict[str, str]

    def __init__(
        self,
        auto_delete: bool,
        durable: bool,
        exclusive: bool,
        no_create: bool,
        arguments: dict[str, str],
    ) -> None: ...

class AsyncConnection:
    def __init__(
        self,
        config: Config,
        publish_confirmations: PublishConfirmations,
        auto_ack: bool,
        prefetch_count: Optional[int] = None,
    ) -> None: ...

    def publish(
        self,
        exchange_name: str,
        routing_key: str,
        body: Union[bytes, str],
        content_type: str,
        content_encoding: ContentEncoding,
        command_timeout: Optional[int] = None,
        delivery_mode: DeliveryMode = DeliveryMode.Transient,
        expiration: Optional[int] = None,
    ) -> Future[None]: ...

    def subscribe(
        self,
        handler: Callable[[Message], Awaitable[None]],
        routing_key: str,
        exchange_name: str,
        exchange_type: str,
        queue_name: str,
        process_timeout: Optional[int] = None,
        command_timeout: Optional[int] = None,
        queue_options: QueueOptions = ...,
    ) -> Future[None]: ...

    def rpc_server(
        self,
        handler: Callable[[Message], Awaitable[Union[Message, bytes]]],
        routing_key: str,
        exchange_name: str,
        exchange_type: str,
        queue_name: str,
        process_timeout: Optional[int] = None,
        command_timeout: Optional[int] = None,
        queue_options: QueueOptions = ...,
    ) -> Future[None]: ...

    def rpc_client(
        self,
        exchange_name: str,
        routing_key: str,
        body: Union[bytes, str],
        content_type: str,
        content_encoding: ContentEncoding,
        response_timeout_millis: int,
        command_timeout: Optional[int] = None,
        delivery_mode: DeliveryMode = DeliveryMode.Transient,
        expiration: Optional[int] = None,
    ) -> Future[bytes]: ...

    def update_secret(
        self,
        new_secret: str,
        reason: str,
        command_timeout: Optional[int] = None,
    ) -> Future[None]: ...

    def close(self) -> Future[None]: ...