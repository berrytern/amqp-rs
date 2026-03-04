from typing import Callable, Optional, Awaitable, Union
from concurrent.futures import Future
from enum import Enum


class Message:
    body: bytes
    content_type: Optional[str]

    @staticmethod
    def new(body: bytes, content_type: Optional[str] = None) -> "Message": ...

    

class DeliveryMode(Enum):
    Transient = 1
    Persistent = 2

class ContentEncoding(Enum):
    Zstd = 'zstd',
    Lz4 = 'lz4',
    Zlib = 'zlib',
    Null = 'null'

class ConfigOptions:
    queue_name: str
    rpc_exchange_name: str
    rpc_queue_name: str
    def __init__(self, queue_name: str, rpc_exchange_name: str, rpc_queue_name: str) -> None: ...

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
    def __init__(self, config: Config, qos_config: QoSConfig) -> None:
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
        handler: Callable[[bytes], None],
        process_timeout: Optional[int] = None,
        command_timeout: int = 16,
    ) -> Future[None]:
        """
        Register a provider to listen on queue of bus

        Args:
            exchange_name: exchange name
            routing_key: routing_key name
            handler: message handler, it will be called when a message is received
            process_timeout: timeout in seconds for waiting for process the received message
            command_timeout: timeout for waiting for command execution
        Returns:
            None: None

        Examples:
            >>> async def handle(body) -> None:
                    print(f"received message: {body}")
            >>> exchange_name = "example"
            >>> routing_key = "user.find3"
            >>> process_timeout = 20
            >>> command_timeout = 16
            >>> await eventbus.subscribe(exchange_name, routing_key, handle, process_timeout, command_timeout)
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