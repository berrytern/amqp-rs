import test from 'ava'
import {
  AsyncEventbus,
  ContentEncoding,
  createConfig,
  createQosConfig,
  DeliveryMode,
  randomSuffix,
  sleep,
} from './helper.ts'

test('should deliver Message directly as first arg with lazy metadata', async (t) => {
  const config = createConfig()
  const qosConfig = createQosConfig({ subAutoAck: false })
  const eventbus = await AsyncEventbus.connect(config, qosConfig)

  const exchange = `direct_ex_${randomSuffix()}`
  const routingKey = 'orders.created'

  let resolveMsg: (val: { body: string; rk?: string; ex?: string; tag?: number }) => void
  const msgPromise = new Promise<{ body: string; rk?: string; ex?: string; tag?: number }>((r) => {
    resolveMsg = r
  })

  await eventbus.subscribe(exchange, routingKey, async (msg) => {
    resolveMsg({
      body: msg.body.toString('utf-8'),
      rk: msg.routingKey ?? undefined,
      ex: msg.exchange ?? undefined,
      tag: msg.deliveryTag ?? undefined,
    })
  })

  await sleep(100)
  await eventbus.publish(exchange, routingKey, 'direct-payload', 'text/plain', ContentEncoding.Null, 5, DeliveryMode.Transient)

  const res = await msgPromise
  t.is(res.body, 'direct-payload')
  t.is(res.rk, routingKey)
  t.is(res.ex, exchange)
  t.truthy(res.tag && res.tag > 0)

  await eventbus.dispose()
})

test('should support publishBatch and subscribeBatch', async (t) => {
  const config = createConfig()
  const qosConfig = createQosConfig({ subAutoAck: false })
  const eventbus = await AsyncEventbus.connect(config, qosConfig)

  const exchange = `batch_ex_${randomSuffix()}`
  const routingKey = 'batch.events'
  const total = 40
  const received: string[] = []

  let resolveDone: () => void
  const donePromise = new Promise<void>((r) => {
    resolveDone = r
  })

  await eventbus.subscribeBatch(
    exchange,
    routingKey,
    async (messages) => {
      for (const m of messages) {
        received.push(m.body.toString('utf-8'))
      }
      if (received.length >= total) {
        resolveDone()
      }
    },
    20,
    25,
  )

  await sleep(100)
  const batch = Array.from({ length: total }, (_, i) => Buffer.from(`msg-${i}`))
  await eventbus.publishBatch(exchange, routingKey, batch, 'text/plain', ContentEncoding.Null, 10, DeliveryMode.Transient)

  await Promise.race([
    donePromise,
    new Promise((_, reject) => setTimeout(() => reject(new Error('Batch timeout')), 5000)),
  ])

  t.is(received.length, total)
  await eventbus.dispose()
})

test('should support async RPC handlers returning Buffer or string directly', async (t) => {
  const rpcExchangeName = `rpc_async_ex_${randomSuffix()}`
  const config = createConfig({ rpcExchangeName })
  const qosConfig = createQosConfig()
  const eventbus = await AsyncEventbus.connect(config, qosConfig)

  const rkBuf = `rpc.buf.${randomSuffix()}`
  const rkStr = `rpc.str.${randomSuffix()}`

  await eventbus.provideResource(rkBuf, async (msg) => {
    await sleep(5)
    return Buffer.from(`buf:${msg.body.toString('utf-8')}`)
  })

  await eventbus.provideResource(rkStr, async (msg) => {
    await sleep(5)
    return `str:${msg.body.toString('utf-8')}`
  })

  await sleep(100)

  const resBuf = await eventbus.rpcClient(rpcExchangeName, rkBuf, 'hello', 'text/plain', ContentEncoding.Null, 5000, 5, DeliveryMode.Transient)
  const resStr = await eventbus.rpcClient(rpcExchangeName, rkStr, 'world', 'text/plain', ContentEncoding.Null, 5000, 5, DeliveryMode.Transient)

  t.is(resBuf.toString('utf-8'), 'buf:hello')
  t.is(resStr.toString('utf-8'), 'str:world')

  await eventbus.dispose()
})
