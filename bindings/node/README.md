# amqp-rs (Node.js)

[![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](../../LICENSE)

**amqp-rs** is a high-performance Node.js AMQP client library written in Rust using [NAPI-RS](https://napi.rs/) and Tokio. It provides native asynchronous primitives for RabbitMQ with resilient connection actors, fail-fast topology declaration, and full TypeScript definitions.

---

## Features

- **Native Rust Core**: Powered by Tokio and `amqp-client-rust` with prebuilt N-API binaries.
- **Full TypeScript Support**: Auto-generated `.d.ts` definitions for all APIs, options, and enums.
- **Fail-Fast Topology Declaration**: Declare exchanges, queues (with TTL, max-length, priority, and DLX), and bindings via `declareTopology()` and `declareExchange()`, or customize consumer defaults in `ConfigOptions`.
- **Delivery Metadata**: Access `routingKey`, `exchange`, `redelivered`, `deliveryTag`, `correlationId`, `messageId`, `appId`, `timestamp`, and `headers` on every `Message`.
- **Pub/Sub & RPC**: High-throughput publish/subscribe and bidirectional RPC using direct reply-to.
- **Compression & TLS**: Built-in `zstd`, `zlib`, and `lz4` compression plus mutual TLS (`TlsAdaptor`).
- **Graceful Shutdown**: Cleanly drain consumers and close connections with `dispose()`.

---

## Installation

```bash
npm install amqp-rs
# or
yarn add amqp-rs
```

---

## Quickstart

### 1. Publishing & Subscribing (Pub/Sub)

```ts
import {
  AsyncEventbus,
  Config,
  ContentEncoding,
  DeliveryMode,
  type QoSConfig,
} from 'amqp-rs'

const config = new Config('localhost', 5672, 'guest', 'guest', '/', {
  queueName: 'orders_queue',
  rpcExchangeName: 'rpc_ex',
  rpcQueueName: 'rpc_q',
})

const qos: QoSConfig = {
  pubConfirm: true,
  rpcClientConfirm: true,
  rpcServerConfirm: false,
  subAutoAck: false,
  rpcServerAutoAck: false,
  rpcClientAutoAck: true,
}

const bus = await AsyncEventbus.connect(config, qos)

await bus.subscribe('orders_exchange', 'orders.created', async (msg) => {
  console.log(`[${msg.exchange} -> ${msg.routingKey}]`, msg.body.toString('utf8'))
})

await bus.publish(
  'orders_exchange',
  'orders.created',
  Buffer.from(JSON.stringify({ orderId: 42 })),
  'application/json',
  ContentEncoding.Null,
  16,
  DeliveryMode.Transient,
)

await bus.dispose()
```

---

### 2. Topology Declaration

Declare exchanges, queues, and bindings explicitly before publishing or consuming:

```ts
import { ExchangeType } from 'amqp-rs'

await bus.declareTopology({
  exchanges: [
    { name: 'orders_dlx', options: { exchangeType: ExchangeType.Direct, durable: true } },
    { name: 'orders_exchange', options: { exchangeType: ExchangeType.Topic, durable: true } },
  ],
  queues: [
    { name: 'orders_dlq', options: { durable: true } },
    {
      name: 'orders_queue',
      options: {
        durable: true,
        deadLetterExchange: 'orders_dlx',
        deadLetterRoutingKey: 'orders.dead',
        messageTtl: 60_000,
        maxLength: 10_000,
      },
    },
  ],
  bindings: [
    { queueName: 'orders_dlq', exchangeName: 'orders_dlx', routingKey: 'orders.dead' },
    { queueName: 'orders_queue', exchangeName: 'orders_exchange', routingKey: 'orders.#' },
  ],
})
```

---

### 3. Remote Procedure Calls (RPC)

```ts
await bus.provideResource('user.get', async (request) => {
  console.log(`RPC Request (${request.correlationId}):`, request.body.toString('utf8'))
  return Buffer.from(JSON.stringify({ status: 'ok' }))
})

const response = await bus.rpcClient(
  'rpc_ex',
  'user.get',
  Buffer.from(JSON.stringify({ userId: 1 })),
  'application/json',
  ContentEncoding.Null,
  20_000,
  32,
  DeliveryMode.Transient,
)

console.log('RPC Response:', response.toString('utf8'))
```

---

## Development

```bash
yarn install
yarn build
yarn test
```

## License

This project is licensed under the [Apache 2.0 License](../../LICENSE).
