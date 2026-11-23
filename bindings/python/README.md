# amqp-rs

[![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)
<a href="https://pypi.org/project/amqp-rs" target="_blank">
    <img src="https://img.shields.io/pypi/v/amqp-rs?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
<a href="https://pypi.org/project/amqp-rs" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/amqp-rs.svg?color=%2334D058" alt="Supported Python versions">
</a>

**amqp-rs** is a high-performance Python AMQP client library developed in Rust using PyO3 and Tokio. It provides native asynchronous primitives for RabbitMQ with zero-copy buffer capabilities, resilient connection actors, fail-fast topology declaration, and automated resource management.

---

## Features

- **Asynchronous & Thread-Safe**: Powered by Tokio and PyO3 with full support for **CPython 3.9–3.15**, including Free-Threaded Python (**no-GIL 3.13t / 3.14t / 3.15t**).
- **Zero-Copy Buffer Protocol (PEP 3118)**: Native `Message` class implements Python's buffer protocol (`memoryview(msg)` / `msg.memoryview()`), eliminating memory copies when integrating with binary parsers or serializers like `orjson`.
- **Zero-Allocation Message Re-use**: Direct `Message` support in `publish()` and `rpc_client()` sharing underlying Rust `Arc<[u8]>` payloads without FFI re-allocation.
- **Zero-Copy Delivery Metadata**: Access `routing_key`, `exchange`, `redelivered`, `delivery_tag`, `correlation_id`, `message_id`, `app_id`, `timestamp`, and lazy `headers` directly on `Message`.
- **Fail-Fast Topology Declaration**: Declaratively set up exchanges, queues (including TTL, max-length, priority, and DLX options), and bindings via `declare_topology()` and `declare_exchange()`, or customize consumer defaults in `ConfigOptions`.
- **Declarative Requeue Control**: Raise `RequeueError` in a subscriber handler to trigger `Basic.Nack(requeue=True)`; all other exceptions reject with `requeue=False` (routing to DLX if configured).
- **Pub/Sub & Batching**: High-throughput messaging with explicit `publish_batch()` / `subscribe_batch()` and optional transparent micro-batching (`BatchConfig`) to amortize FFI crossings.
- **RPC Client & Server**: Low-latency bidirectional Remote Procedure Calls using direct reply-to with correlation ID tracking.
- **Compression**: Native payload compression for `zstd`, `zlib`, and `lz4`.
- **TLS/SSL Support**: Encrypted communication with mutual TLS (`TlsAdaptor`).
- **Graceful Shutdown**: Drain consumers cleanly without message loss via `dispose()`.

---

## Installation

```bash
uv add amqp-rs
# or
pip install amqp-rs
```

---

## Quickstart

### 1. Publishing & Subscribing (Pub/Sub)

```python
import asyncio
from amqp_rs import (
    AsyncEventbus,
    Config,
    ConfigOptions,
    Message,
    QoSConfig,
)

async def main():
    options = ConfigOptions(
        queue_name="orders_queue",
        rpc_exchange_name="rpc_ex",
        rpc_queue_name="rpc_q",
    )
    config = Config("localhost", 5672, "guest", "guest", "/", options=options)
    bus = AsyncEventbus(config, QoSConfig.default())

    # Subscribe to a topic
    async def order_handler(msg: Message):
        # Zero-copy access via memoryview or bytes property (cached)
        print(f"[{msg.exchange} -> {msg.routing_key}] {msg.body.decode('utf-8')}")

    await bus.subscribe("orders_exchange", "orders.created", order_handler)

    # Publish an event (accepts bytes, str, bytearray, or Message)
    await bus.publish(
        "orders_exchange",
        "orders.created",
        b'{"order_id": 42, "item": "laptop"}',
        content_type="application/json",
    )

    await asyncio.sleep(1)
    await bus.dispose()

asyncio.run(main())
```

---

### 2. High-Performance Zero-Copy Handling & Typed Handlers

Handlers passed to `subscribe()`, `subscribe_batch()`, and `provide_resource()` automatically receive the argument type matching their first parameter annotation (inspected once at registration time, zero runtime overhead per message):

- **`Message`** (or unannotated): full message object with `.body`, `.memoryview()`, and delivery metadata.
- **`bytes`**: raw payload bytes directly (skips allocating the `Message` wrapper).
- **`memoryview`**: zero-copy PEP 3118 buffer view directly over the Rust payload.
- **`str`**: UTF-8 decoded payload string.

```python
import orjson
from amqp_rs import Message

async def with_message(msg: Message):
    data = orjson.loads(msg.memoryview())

async def with_bytes(body: bytes):
    data = orjson.loads(body)

async def with_memoryview(view: memoryview):
    data = orjson.loads(view)

async def with_str(text: str):
    print(text)
```

*(For `subscribe_batch()`, annotate as `list[bytes]`, `list[memoryview]`, `list[str]`, or `list[Message]`.)*

---

### 3. Topology Declaration & Consumer Options

Declare exchanges, queues, and bindings up front (fail-fast) using `Topology`, `ExchangeOptions`, and `QueueOptions`:

```python
from amqp_rs import (
    ExchangeOptions,
    ExchangeType,
    QueueOptions,
    Topology,
)

topology = (
    Topology()
    .exchange("orders_dlx", ExchangeOptions.direct())
    .exchange("orders_exchange", ExchangeOptions(exchange_type=ExchangeType.Topic, durable=True))
    .queue("orders_dlq", QueueOptions(durable=True))
    .queue(
        "orders_queue",
        QueueOptions(
            durable=True,
            dead_letter_exchange="orders_dlx",
            dead_letter_routing_key="orders.dead",
            message_ttl=60_000,
            max_length=10_000,
        ),
    )
    .bind("orders_dlq", "orders_dlx", "orders.dead")
    .bind("orders_queue", "orders_exchange", "orders.#")
)

await bus.declare_topology(topology)
```

You can also customize default exchange/queue options used by `subscribe()` and `provide_resource()` via `ConfigOptions`:

```python
options = ConfigOptions(
    queue_name="orders_queue",
    rpc_exchange_name="rpc_ex",
    rpc_queue_name="rpc_q",
    sub_exchange=ExchangeOptions.direct(),
    sub_queue=QueueOptions(durable=True, message_ttl=30_000),
    rpc_exchange=ExchangeOptions.direct(),
    rpc_queue=QueueOptions(durable=True),
)
```

---

### 4. Declarative Requeue Control (`RequeueError`)

When `sub_auto_ack=False` (default), subscriber handlers control acknowledgement automatically:

```python
from amqp_rs import Message, RequeueError

async def handler(msg: Message):
    if temporary_outage():
        # Sends Basic.Nack(requeue=True)
        raise RequeueError("Database temporarily unavailable")
    if invalid_payload(msg.body):
        # Any other exception sends Basic.Nack(requeue=False) -> routes to DLX if configured
        raise ValueError("Invalid message")
    # Returning normally sends Basic.Ack
```

---

### 5. Remote Procedure Calls (RPC)

```python
async def rpc_example():
    # RPC Server
    async def handle_rpc(request: Message):
        print(f"RPC Request ({request.correlation_id}): {request.body}")
        return b'{"status": "ok"}'

    await bus.provide_resource("user.get", handle_rpc)

    # RPC Client
    response = await bus.rpc_client(
        "rpc_ex",
        "user.get",
        b'{"user_id": 1}',
    )
    print(f"RPC Response: {response}")
```

---

### 6. Batch Publishing & Auto-Batching

For high-throughput publishing of many small messages, use `publish_batch()` for a single FFI crossing or enable transparent micro-batching with `BatchConfig`:

```python
from amqp_rs import BatchConfig

# Explicit batch publish
await bus.publish_batch(
    "orders_exchange",
    "orders.created",
    [b'{"id": 1}', b'{"id": 2}', b'{"id": 3}'],
)

# Transparent auto-batching
batch_cfg = BatchConfig(
    enabled=True,
    max_batch_size=100,
    max_delay_ms=2,
    max_payload_bytes=1024,
)
bus = AsyncEventbus(config, QoSConfig.default(), batch_config=batch_cfg)
```

---

## License

This project is licensed under the [Apache 2.0 License](LICENSE).
