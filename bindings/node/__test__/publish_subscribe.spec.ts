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

test('should publish a string message and receive it via subscription', async (t) => {
  const config = createConfig()
  const qosConfig = createQosConfig()
  const eventbus = await AsyncEventbus.connect(config, qosConfig)

  const exchange = `pubsub_ex_${randomSuffix()}`
  const routingKey = 'pubsub.basic.key'
  const payload = 'hello from node binding test'

  let receivedMessage: Message | null = null
  let resolveReceived: (msg: Message) => void
  const receivedPromise = new Promise<Message>((resolve) => {
    resolveReceived = resolve
  })

  await eventbus.subscribe(
    exchange,
    routingKey,
    async (arg1: any, arg2?: any) => {
      const msg = extractMessage(arg1, arg2)
      receivedMessage = msg
      resolveReceived(msg)
    },
    null,
    null,
  )

  await sleep(100)

  await eventbus.publish(
    exchange,
    routingKey,
    payload,
    'text/plain',
    ContentEncoding.Null,
    5,
    DeliveryMode.Transient,
    null,
  )

  const result = await Promise.race([
    receivedPromise,
    new Promise<null>((_, reject) => setTimeout(() => reject(new Error('Subscription timed out')), 5000)),
  ])

  t.truthy(result)
  t.truthy(receivedMessage)
  t.is(result!.body.toString('utf-8'), payload)

  await eventbus.dispose()
})

test('should publish a Buffer payload with custom content type', async (t) => {
  const config = createConfig()
  const qosConfig = createQosConfig()
  const eventbus = await AsyncEventbus.connect(config, qosConfig)

  const exchange = `pubsub_buf_ex_${randomSuffix()}`
  const routingKey = 'pubsub.buffer.key'
  const rawBytes = Buffer.from([0x01, 0x02, 0x03, 0x04, 0xff, 0xfe])

  let resolveReceived: (msg: Message) => void
  const receivedPromise = new Promise<Message>((resolve) => {
    resolveReceived = resolve
  })

  await eventbus.subscribe(
    exchange,
    routingKey,
    async (arg1: any, arg2?: any) => {
      const msg = extractMessage(arg1, arg2)
      resolveReceived(msg)
    },
    null,
    null,
  )

  await sleep(100)

  await eventbus.publish(
    exchange,
    routingKey,
    rawBytes,
    'application/octet-stream',
    ContentEncoding.Null,
    5,
    DeliveryMode.Transient,
    null,
  )

  const received = await receivedPromise
  t.true(Buffer.isBuffer(received.body))
  t.deepEqual(received.body, rawBytes)

  await eventbus.dispose()
})

test('should publish and receive messages with different ContentEncodings', async (t) => {
  const config = createConfig()
  const qosConfig = createQosConfig()
  const eventbus = await AsyncEventbus.connect(config, qosConfig)

  const encodings = [
    { name: 'Null', encoding: ContentEncoding.Null },
    { name: 'Zstd', encoding: ContentEncoding.Zstd },
    { name: 'Lz4', encoding: ContentEncoding.Lz4 },
    { name: 'Zlib', encoding: ContentEncoding.Zlib },
  ] as const

  for (const item of encodings) {
    const exchange = `pubsub_enc_${item.name}_${randomSuffix()}`
    const routingKey = `pubsub.encoding.${item.name}`
    const payload = JSON.stringify({ message: `testing encoding ${item.name}`, timestamp: Date.now() })

    let resolveMsg: (msg: Message) => void
    const msgPromise = new Promise<Message>((res) => {
      resolveMsg = res
    })

    await eventbus.subscribe(
      exchange,
      routingKey,
      async (arg1: any, arg2?: any) => {
        const msg = extractMessage(arg1, arg2)
        resolveMsg(msg)
      },
      null,
      null,
    )

    await sleep(50)

    await eventbus.publish(
      exchange,
      routingKey,
      payload,
      'application/json',
      item.encoding,
      5,
      DeliveryMode.Transient,
      null,
    )

    const received = await msgPromise
    t.is(received.body.toString('utf-8'), payload, `Failed for encoding ${item.name}`)
  }

  await eventbus.dispose()
})

test('should respect topic routing keys correctly', async (t) => {
  const config = createConfig()
  const qosConfig = createQosConfig()
  const eventbus = await AsyncEventbus.connect(config, qosConfig)

  const exchange = `pubsub_topic_ex_${randomSuffix()}`
  let matchedCount = 0

  await eventbus.subscribe(
    exchange,
    'orders.europe.#',
    async () => {
      matchedCount++
    },
    null,
    null,
  )

  await sleep(100)

  // Matching message
  await eventbus.publish(
    exchange,
    'orders.europe.created',
    'Order created in Europe',
    'text/plain',
    ContentEncoding.Null,
    5,
    DeliveryMode.Transient,
    null,
  )

  // Non-matching message
  await eventbus.publish(
    exchange,
    'orders.asia.created',
    'Order created in Asia',
    'text/plain',
    ContentEncoding.Null,
    5,
    DeliveryMode.Transient,
    null,
  )

  await sleep(400)

  t.is(matchedCount, 1)

  await eventbus.dispose()
})

test('should publish and consume multiple concurrent messages', async (t) => {
  const config = createConfig()
  const qosConfig = createQosConfig()
  const eventbus = await AsyncEventbus.connect(config, qosConfig)

  const exchange = `pubsub_multi_ex_${randomSuffix()}`
  const routingKey = 'pubsub.multi'
  const total = 50
  const received: number[] = []

  let resolveAll: () => void
  const allPromise = new Promise<void>((resolve) => {
    resolveAll = resolve
  })

  await eventbus.subscribe(
    exchange,
    routingKey,
    async (arg1: any, arg2?: any) => {
      const msg = extractMessage(arg1, arg2)
      const data = JSON.parse(msg.body.toString('utf-8'))
      received.push(data.index)
      if (received.length === total) {
        resolveAll()
      }
    },
    null,
    null,
  )

  await sleep(100)

  const publishPromises: Promise<void>[] = []
  for (let i = 0; i < total; i++) {
    const payload = JSON.stringify({ index: i, text: `message-${i}` })
    publishPromises.push(
      eventbus.publish(
        exchange,
        routingKey,
        payload,
        'application/json',
        ContentEncoding.Null,
        10,
        DeliveryMode.Transient,
        null,
      ),
    )
  }

  await Promise.all(publishPromises)
  await allPromise

  t.is(received.length, total)

  await eventbus.dispose()
})
