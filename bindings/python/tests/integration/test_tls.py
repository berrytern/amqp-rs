import pytest
from amqp_rs import Config, ConfigOptions, AsyncEventbus, QoSConfig, TlsAdaptor
import asyncio

@pytest.mark.asyncio
async def test_tls():
    options = ConfigOptions(queue_name='test_queue', rpc_exchange_name='test_exchange', rpc_queue_name='test_rpc_queue')
    tls_adaptor = TlsAdaptor.with_client_auth("./.certs/amqp/ca.pem", "./.certs/amqp/rabbitmq_cert.pem", "./.certs/amqp/rabbitmq_key.pem", "localhost")
    config = Config(host='localhost', port=5671, username='guest', password='guest', virtual_host='/', options=options, tls_adaptor=tls_adaptor)
    eventbus = AsyncEventbus(config, QoSConfig(pub_confirm=True, rpc_client_confirm=True, rpc_server_confirm=True, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True, sub_prefetch=None, rpc_server_prefetch=None, rpc_client_prefetch=None))
    await asyncio.sleep(1)
    exchange_name = options.rpc_exchange_name
    routing_key = "abc.example"
    async def handler(_):
        pass
    await eventbus.subscribe(exchange_name, routing_key, handler, None, None)
    await eventbus.dispose()