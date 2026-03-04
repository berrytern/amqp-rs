import pytest
from amqp_rs import AsyncEventbus, Config, ConfigOptions, QoSConfig, ContentEncoding, Message
from asyncio import Future, sleep, get_running_loop
from json import dumps




@pytest.mark.asyncio
async def test_subscribe():
    future = Future(loop = get_running_loop())
    options = ConfigOptions(queue_name='test_queue', rpc_exchange_name='test_exchange', rpc_queue_name='test_rpc_queue')
    eventbus = AsyncEventbus(Config(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', options=options, tls_adaptor=None), QoSConfig(pub_confirm=True, rpc_client_confirm=True, rpc_server_confirm=True, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True, sub_prefetch=None, rpc_server_prefetch=None, rpc_client_prefetch=None))
    async def handle(body: Message):
        if not future.done():
            future.set_result(body)

    exchange_name = "test"
    routing_key = "test.action"
    message = b"received message"
    await eventbus.subscribe(exchange_name, routing_key, handle, None, None)
    await eventbus.publish(exchange_name, routing_key, message, None, ContentEncoding.Null, None)
    await future
    assert future.done()
    response: Message = future.result()
    assert isinstance(response, Message)
    assert response.body == message
    await eventbus.dispose()