import { resolve } from 'node:path'
import test from 'ava'
import { AsyncEventbus, certsDir, createConfig, createQosConfig, randomSuffix, sleep, TlsAdaptor } from './helper.ts'

test('should connect and dispose without TLS', async (t) => {
  const config = createConfig()
  const qosConfig = createQosConfig()

  const eventbus = await AsyncEventbus.connect(config, qosConfig)
  t.truthy(eventbus)

  const exchange = `conn_test_ex_${randomSuffix()}`
  const routingKey = 'conn.test'

  await eventbus.subscribe(exchange, routingKey, async () => {}, null, null)

  await t.notThrowsAsync(async () => {
    await eventbus.dispose()
  })
})

test('should connect and dispose with TLS (client auth)', async (t) => {
  const caCert = resolve(certsDir, 'ca.pem')
  const clientCert = resolve(certsDir, 'rabbitmq_cert.pem')
  const clientKey = resolve(certsDir, 'rabbitmq_key.pem')

  const tlsAdaptor = TlsAdaptor.withClientAuth(caCert, clientCert, clientKey, 'localhost')
  t.truthy(tlsAdaptor)

  const config = createConfig({}, tlsAdaptor, 5671)
  const qosConfig = createQosConfig()

  const eventbus = await AsyncEventbus.connect(config, qosConfig)
  t.truthy(eventbus)

  await sleep(200)

  const exchange = `tls_test_ex_${randomSuffix()}`
  const routingKey = 'tls.test'

  await eventbus.subscribe(exchange, routingKey, async () => {}, null, null)

  await t.notThrowsAsync(async () => {
    await eventbus.dispose()
  })
})
