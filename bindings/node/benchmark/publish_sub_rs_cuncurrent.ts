import { createRequire } from 'node:module'
const require = createRequire(import.meta.url)
const { AsyncEventbus, Config, TlsAdaptor, ContentEncoding, DeliveryMode } = require('../index.js')

// 2. Explicitly import interfaces/types so Node.js drops them at runtime
import type { ConfigOptions, QoSConfig, Message } from '../index.js'
import { resolve, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const certsDir = resolve(__dirname, '../../../.certs/amqp')
const tlsOn = true

const sleep = (seconds: number) => new Promise((resolve) => setTimeout(resolve, seconds * 1000))

const run = async () => {
  const options: ConfigOptions = {
    queueName: 'test_queue',
    rpcExchangeName: 'test_exchange',
    rpcQueueName: 'test_rpc_queue',
  }
  let config: Config
  if (tlsOn) {
    const tlsAdaptor = TlsAdaptor.withClientAuth(
      resolve(certsDir, 'ca.pem'),
      resolve(certsDir, 'rabbitmq_cert.pem'),
      resolve(certsDir, 'rabbitmq_key.pem'),
      'localhost',
    )
    config = new Config('localhost', 5671, 'guest', 'guest', '/', options, tlsAdaptor)
  } else {
    config = new Config('localhost', 5672, 'guest', 'guest', '/', options, null)
  }
  const qosConfig: QoSConfig = {
    pubConfirm: true,
    rpcClientConfirm: true,
    rpcServerConfirm: true,
    subAutoAck: true,
    rpcServerAutoAck: true,
    rpcClientAutoAck: true,
    subPrefetch: undefined,
    rpcServerPrefetch: undefined,
    rpcClientPrefetch: undefined,
  }
  const eventbus = await AsyncEventbus.connect(config, qosConfig)
  await sleep(1)
  const exchange_name = options.rpcExchangeName
  const routing_key = 'abc.example'
  const handler = async (_message: Message) => {}
  await eventbus.subscribe(exchange_name, routing_key, handler, null, null)
  await sleep(3)
  let sended = []
  const total = 300_000
  const payload = JSON.stringify('Hello, RPC!')
  const before = performance.now()

  // Queue all futures
  for (let i = 0; i < total; i++) {
    sended.push(
      eventbus.publish(
        exchange_name,
        routing_key,
        payload,
        'application/json',
        ContentEncoding.Null,
        100,
        DeliveryMode.Transient,
        null,
      ),
    )
  }

  // Wait for all confirmations
  await Promise.all(sended)
  const after = performance.now()
  console.log(`Time taken for ${total / 1_000}k messages: ${(after - before) / 1000} seconds`)
  console.log(`Mean messages per second for ${total / 1_000}k messages: ${total / ((after - before) / 1000)}`)
  await eventbus.dispose()
  const end = performance.now()
  console.log(`all time: ${(end - before) / 1000} seconds`)
  console.log(`time to dispose: ${(end - after) / 1000} seconds`)
}
await run()
