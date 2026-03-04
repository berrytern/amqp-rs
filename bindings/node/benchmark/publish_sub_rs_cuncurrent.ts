// 1. Import runtime values (Classes, Enums) using the default export workaround
import { AsyncEventbus, Config, TlsAdaptor, ContentEncoding, DeliveryMode } from '../index.js'

// 2. Explicitly import interfaces/types so Node.js drops them at runtime
import type { ConfigOptions, QoSConfig, Message } from '../index.js'

const tlsOn = true;

const sleep = (seconds: number) => new Promise(resolve => setTimeout(resolve, seconds * 1000))

const run = async () => {
    const options: ConfigOptions = { queueName: 'test_queue', rpcExchangeName: 'test_exchange', rpcQueueName: 'test_rpc_queue' }
    let config: Config;
    if (tlsOn) {
        const path = "../../"
        const tlsAdaptor = TlsAdaptor.withClientAuth(path + ".certs/amqp/ca.pem", path + ".certs/amqp/rabbitmq_cert.pem", path + ".certs/amqp/rabbitmq_key.pem", "localhost")
        config = new Config('localhost', 5671, 'guest', 'guest', '/', options, tlsAdaptor)
    } else {
        config = new Config('localhost', 5672, 'guest', 'guest', '/', options, null)
    }
    let qosConfig: QoSConfig = { pubConfirm: true, rpcClientConfirm: true, rpcServerConfirm: true, subAutoAck: false, rpcServerAutoAck: false, rpcClientAutoAck: false }
    const eventbus = await AsyncEventbus.connect(config, qosConfig)
    await sleep(1)
    const exchange_name = options.rpcExchangeName
    const routing_key = "abc.example"
    const handler = async (err: Error | null, message: Message) => {
    }
    await eventbus.subscribe(exchange_name, routing_key, handler, null, null)
    await sleep(3)
    let sended = []
    const total = 300_000
    const payload = 'Hello, RPC!'
    const before = Date.now()
    // Queue all futures
    for (let i = 0; i < total; i++) {
        sended.push(eventbus.publish(exchange_name, routing_key, payload, "application/json", ContentEncoding.Null, 100, DeliveryMode.Transient, null))
    }
    
    // Wait for all confirmations
    await Promise.all(sended)
    const after = Date.now()
    console.log(`Time taken for ${total / 1_000}k messages: ${(after - before) / 1000} seconds`)
    console.log(`Mean messages per second for ${total / 1_000}k messages: ${total / ((after - before) / 1000)}`)
    await eventbus.dispose()
    const end = Date.now()
    console.log(`all time: ${(end - before) / 1000} seconds`)
    console.log(`time to dispose: ${(end - after) / 1000} seconds`) 
}
await run() 