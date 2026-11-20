# amqp-rs

[![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)
<a href="https://pypi.org/project/amqp-rs" target="_blank">
    <img src="https://img.shields.io/pypi/v/amqp-rs?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
<a href="https://pypi.org/project/amqp-rs" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/amqp-rs.svg?color=%2334D058" alt="Supported Python versions">
</a>

**amqp-rs** is a high-performance Python AMQP client library developed in Rust using PyO3 and Tokio. It provides native asynchronous primitives for RabbitMQ with zero-copy buffer capabilities, resilient connection actors, and automated resource declaration.

---

## Features

- **Asynchronous & Thread-Safe**: Powered by Tokio and PyO3 with full support for CPython 3.8+ including Free-Threaded Python (no-GIL 3.13t / 3.14t).
- **Zero-Copy Buffer Protocol (PEP 3118)**: Native `Message` class implements Python's buffer protocol (`memoryview(msg)` / `msg.memoryview()`), eliminating memory copies when integrating with binary parsers or serializers like `orjson`.
- **Zero-Allocation Message Re-use**: Direct `Message` support in `publish()` and `rpc_client()` sharing underlying Rust `Arc<[u8]>` payloads without FFI re-allocation.
- **Automatic Resource Declaration**: Declarative setup for exchanges, queues, and Dead Letter Exchanges (DLQ).
- **Pub/Sub & Auto-Batching**: High-throughput message streaming with optional transparent micro-batching (`BatchConfig`) to amortize FFI crossings.
- **RPC Client & Server**: Low-latency bidirectional Remote Procedure Calls using direct reply-to with correlation ID tracking.
- **Compression**: Native payload compression for `zstd`, `zlib`, and `lz4`.
- **TLS/SSL Support**: Encrypted communication with mutual TLS support.
- **Graceful Shutdown**: Drain consumers cleanly without message loss.

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
    ContentEncoding,
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
        payload = msg.body
        print(f"Received order: {payload.decode('utf-8')}")

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

### 2. High-Performance Zero-Copy Handling

For high-throughput applications handling large payloads, access the payload via `msg.memoryview()` or pass `msg` directly to buffer-compatible parsers:

```python
import orjson
from amqp_rs import Message

async def fast_handler(msg: Message):
    # Zero-copy parsing directly from the Rust buffer:
    data = orjson.loads(msg.memoryview())
    # Or pass msg directly (PEP 3118 buffer protocol):
    # data = orjson.loads(msg)
```

---

### 3. Remote Procedure Calls (RPC)

```python
async def rpc_example():
    # RPC Server
    async def handle_rpc(request: Message):
        print(f"RPC Request: {request.body}")
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

### 4. Auto-Batching for High-Throughput Publishing

When publishing thousands of small messages concurrently, enable `BatchConfig` to amortize FFI crossings automatically:

```python
from amqp_rs import BatchConfig

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
