import test from 'ava'
import {
  AsyncEventbus,
  ContentEncoding,
  createConfig,
  createQosConfig,
  DeliveryMode,
  type Message,
  randomSuffix,
  sleep,
} from './helper.ts'

function extractMessage(arg1: any, arg2?: any): Message {
  if (arg1 === null && arg2 !== undefined) {
    return arg2
  }
  return arg1
}

test('should perform basic RPC request and receive response', async (t) => {
  const rpcExchangeName = `rpc_ex_${randomSuffix()}`
  const config = createConfig({ rpcExchangeName })
  const qosConfig = createQosConfig()
  const eventbus = await AsyncEventbus.connect(config, qosConfig)

  const routingKey = `rpc.math.echo_${randomSuffix()}`

  await eventbus.provideResource(
    routingKey,
    (arg1: any, arg2?: any) => {
      const msg = extractMessage(arg1, arg2)
      const incomingText = msg.body.toString('utf-8')
      return {
        body: Buffer.from(`echo:${incomingText}`),
        contentType: 'text/plain',
      }
    },
    null,
    null,
  )

  await sleep(100)

  const response = await eventbus.rpcClient(
    rpcExchangeName,
    routingKey,
    'ping',
    'text/plain',
    ContentEncoding.Null,
    5000,
    5,
    DeliveryMode.Transient,
    null,
  )

  t.true(Buffer.isBuffer(response))
  t.is(response.toString('utf-8'), 'echo:ping')

  await eventbus.dispose()
})

test('should perform JSON RPC calculation', async (t) => {
  const rpcExchangeName = `rpc_calc_ex_${randomSuffix()}`
  const config = createConfig({ rpcExchangeName })
  const qosConfig = createQosConfig()
  const eventbus = await AsyncEventbus.connect(config, qosConfig)

  const routingKey = `rpc.math.add_${randomSuffix()}`

  await eventbus.provideResource(
    routingKey,
    (arg1: any, arg2?: any) => {
      const msg = extractMessage(arg1, arg2)
      const input = JSON.parse(msg.body.toString('utf-8')) as { a: number; b: number }
      const sum = input.a + input.b
      return {
        body: Buffer.from(JSON.stringify({ result: sum })),
        contentType: 'application/json',
      }
    },
    null,
    null,
  )

  await sleep(100)

  const payload = JSON.stringify({ a: 15, b: 27 })
  const response = await eventbus.rpcClient(
    rpcExchangeName,
    routingKey,
    payload,
    'application/json',
    ContentEncoding.Null,
    5000,
    5,
    DeliveryMode.Transient,
    null,
  )

  const parsed = JSON.parse(response.toString('utf-8'))
  t.is(parsed.result, 42)

  await eventbus.dispose()
})

test('should handle RPC requests with compression encodings', async (t) => {
  const rpcExchangeName = `rpc_comp_ex_${randomSuffix()}`
  const config = createConfig({ rpcExchangeName })
  const qosConfig = createQosConfig()
  const eventbus = await AsyncEventbus.connect(config, qosConfig)

  const routingKey = `rpc.compressed_${randomSuffix()}`

  await eventbus.provideResource(
    routingKey,
    (arg1: any, arg2?: any) => {
      const msg = extractMessage(arg1, arg2)
      return {
        body: Buffer.from(`response to: ${msg.body.toString('utf-8')}`),
        contentType: 'text/plain',
      }
    },
    null,
    null,
  )

  await sleep(100)

  const encodings = [ContentEncoding.Zstd, ContentEncoding.Lz4, ContentEncoding.Zlib]

  for (const encoding of encodings) {
    const response = await eventbus.rpcClient(
      rpcExchangeName,
      routingKey,
      `hello ${encoding}`,
      'text/plain',
      encoding,
      5000,
      5,
      DeliveryMode.Transient,
      null,
    )

    t.is(response.toString('utf-8'), `response to: hello ${encoding}`)
  }

  await eventbus.dispose()
})

test('should handle concurrent RPC requests', async (t) => {
  const rpcExchangeName = `rpc_concurrent_ex_${randomSuffix()}`
  const config = createConfig({ rpcExchangeName })
  const qosConfig = createQosConfig()
  const eventbus = await AsyncEventbus.connect(config, qosConfig)

  const routingKey = `rpc.concurrent_${randomSuffix()}`

  await eventbus.provideResource(
    routingKey,
    (arg1: any, arg2?: any) => {
      const msg = extractMessage(arg1, arg2)
      const data = JSON.parse(msg.body.toString('utf-8'))
      return {
        body: Buffer.from(JSON.stringify({ doubled: data.val * 2, id: data.id })),
        contentType: 'application/json',
      }
    },
    null,
    null,
  )

  await sleep(100)

  const count = 20
  const promises: Promise<Buffer>[] = []

  for (let i = 0; i < count; i++) {
    const payload = JSON.stringify({ id: i, val: i + 1 })
    promises.push(
      eventbus.rpcClient(
        rpcExchangeName,
        routingKey,
        payload,
        'application/json',
        ContentEncoding.Null,
        10000,
        5,
        DeliveryMode.Transient,
        null,
      ),
    )
  }

  const responses = await Promise.all(promises)
  t.is(responses.length, count)

  for (let i = 0; i < count; i++) {
    const parsed = JSON.parse(responses[i].toString('utf-8'))
    t.is(parsed.doubled, (parsed.id + 1) * 2)
  }

  await eventbus.dispose()
})
