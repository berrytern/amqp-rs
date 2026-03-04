# amqp-rs

[![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)
<a href="https://pypi.org/project/amqp-rs" target="_blank">
    <img src="https://img.shields.io/pypi/v/amqp-rs?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
<a href="https://pypi.org/project/amqp-rs" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/amqp-rs.svg?color=%2334D058" alt="Supported Python versions">
</a>

**amqp-rs** is a Python client library with a high level of abstraction for manipulating messages in RabbitMQ.

---

### What is "amqprs"
It is a Python extension developed in Rust using PyO3. It acts as a wrapper for the [amqp-client-rust](https://github.com/berrytern/amqp-client-rust) library, inheriting its performance, stability, and asynchronous capabilities.

### Features
- **Thread Safe**: Built on Rust's memory safety guarantees.
- **Asynchronous API**: Powered by `tokio` and `pyo3-asyncio` for high-performance I/O.
- **Automatic Management**: Handles the creation and management of queues, exchanges, and channels automatically.
- **Persistence**: Built-in connection persistence and automatic reconnection.
- **Flexible Exchanges**: Full support for direct, topic, and fanout exchange types.
- **RPC Support**: Native abstractions for Remote Procedure Calls (RPC).
- **Compression**: Built-in support for `zstd`, `zlib`, and `lz4` encoding.
- **TLS/SSL**: Robust encryption support including mutual TLS (client authentication).
- **Graceful Shutdown**: Ability to stop consumers cleanly and process remaining messages before closing.

---

### Installation

You can install `amqp-rs` directly from PyPI using `uv` or `pip`:

```bash
uv add amqp-rs
```

### Configuration
The library uses specialized objects to manage connection behavior and Quality of Service (QoS):


#### Connection Settings

- `ConfigOptions`: Defines primary queue names and RPC exchange/queue settings.

- `Config`: Contains connection parameters such as `host`, `port`, `username`, `password`, and `virtual_host`.

#### QoS and Confirmations (`QoSConfig`)

You can fine-tune performance and reliability using QoSConfig:

- Confirmations: Enable or disable publisher confirmations for standard publishing or RPC (`pub_confirm`, `rpc_client_confirm`, `rpc_server_confirm`).

- Acknowledge: Set automatic acknowledgement for subscriptions or RPC handlers (`sub_auto_ack`, `rpc_server_auto_ack`).

- Prefetch: Control the flow by setting prefetch counts for different connection types to manage how many unacknowledged messages the client can hold.