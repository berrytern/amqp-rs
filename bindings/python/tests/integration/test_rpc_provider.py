import pytest
from amqp_rs import AsyncEventbus, Config, ConfigOptions, QoSConfig, ContentEncoding
from asyncio import Future, get_running_loop
from json import dumps


@pytest.mark.asyncio
async def test_provider():
    options = ConfigOptions(queue_name='test_queue', rpc_exchange_name='test_exchange', rpc_queue_name='test_rpc_queue')
    eventbus = AsyncEventbus(Config(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', options=options, tls_adaptor=None), QoSConfig(pub_confirm=True, rpc_client_confirm=True, rpc_server_confirm=True, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True, sub_prefetch=None, rpc_server_prefetch=None, rpc_client_prefetch=None))
    expected_result = "received message"
    future = Future(loop = get_running_loop())
    async def handle(body):
        future.set_result(expected_result)
        return body
    routing_key = "abc.example"
    body = dumps(["hi"])
    await eventbus.provide_resource(routing_key, handle)
    result = await eventbus.rpc_client(options.rpc_exchange_name, routing_key, body, "application/json", ContentEncoding.Null)
    assert future.done()
    assert future.result() == expected_result
    assert result == bytes(body, "utf-8")
    await eventbus.dispose()
    



@pytest.mark.asyncio
async def test_provider_error():
    options = ConfigOptions(queue_name='test_queue', rpc_exchange_name='test_exchange', rpc_queue_name='test_rpc_queue')
    eventbus = AsyncEventbus(Config(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', options=options, tls_adaptor=None), QoSConfig(pub_confirm=True, rpc_client_confirm=True, rpc_server_confirm=True, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True, sub_prefetch=None, rpc_server_prefetch=None, rpc_client_prefetch=None))
    expected_result = "received message"
    future = Future(loop = get_running_loop())
    async def handle(body):
        future.set_result(expected_result)
        raise Exception("errorad", "adasd")
    routing_key = "abc.example"
    body = dumps(["hi"])
    await eventbus.provide_resource(routing_key, handle)
    result = await eventbus.rpc_client(options.rpc_exchange_name, routing_key, body, "application/json", ContentEncoding.Null)
    assert future.done()
    assert future.result() == expected_result
    print(result)
    assert result == bytes("Exception: ('errorad', 'adasd')", "utf-8")
    await eventbus.dispose()
    
