import { createRequire } from 'node:module'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { ConfigOptions, Message, QoSConfig } from '../index.d.ts'

const require = createRequire(import.meta.url)
const native = require('../index.js')

export const AsyncEventbus = native.AsyncEventbus as typeof import('../index.d.ts').AsyncEventbus
export const Config = native.Config as typeof import('../index.d.ts').Config
export const TlsAdaptor = native.TlsAdaptor as typeof import('../index.d.ts').TlsAdaptor
export const ContentEncoding = {
  Zstd: 'Zstd',
  Lz4: 'Lz4',
  Zlib: 'Zlib',
  Null: 'Null',
} as const
export const DeliveryMode = {
  Transient: 1,
  Persistent: 2,
} as const

export type { ConfigOptions, Message, QoSConfig }

const __dirname = dirname(fileURLToPath(import.meta.url))
export const certsDir = resolve(__dirname, '../../../.certs/amqp')

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

export function randomSuffix(): string {
  return Math.random().toString(36).substring(2, 10)
}

export function createQosConfig(overrides?: Partial<QoSConfig>): QoSConfig {
  return {
    pubConfirm: true,
    rpcClientConfirm: true,
    rpcServerConfirm: true,
    subAutoAck: true,
    rpcServerAutoAck: true,
    rpcClientAutoAck: true,
    subPrefetch: undefined,
    rpcServerPrefetch: undefined,
    rpcClientPrefetch: undefined,
    ...overrides,
  }
}

export function createConfig(
  optionsOverrides?: Partial<ConfigOptions>,
  tlsAdaptor?: InstanceType<typeof TlsAdaptor> | null,
  port = 5672,
): InstanceType<typeof Config> {
  const suffix = randomSuffix()
  const options: ConfigOptions = {
    queueName: `test_queue_${suffix}`,
    rpcExchangeName: `test_rpc_exchange_${suffix}`,
    rpcQueueName: `test_rpc_queue_${suffix}`,
    ...optionsOverrides,
  }
  return new Config('localhost', port, 'guest', 'guest', '/', options, tlsAdaptor ?? null)
}
