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

@pytest.mark.asyncio
async def test_fail_fast_on_disconnect():
    options = ConfigOptions('q', 'rq', 're', fail_fast_on_disconnect=True)
    config = Config('127.0.0.1', 59998, 'guest', 'guest', '/', options)
    qos = QoSConfig(False, False, False, False, False, False)
    bus = AsyncEventbus(config, qos)
    with pytest.raises(Exception) as exc_info:
        await bus.publish('ex', 'rk', b'test', 'text/plain', ContentEncoding.Null, 100)
    assert 'Connection is unavailable' in str(exc_info.value)
    await bus.dispose()

@pytest.mark.asyncio
async def test_max_pending_commands_zero():
    options = ConfigOptions('q', 'rq', 're', max_pending_commands=0)
    config = Config('127.0.0.1', 59998, 'guest', 'guest', '/', options)
    qos = QoSConfig(False, False, False, False, False, False)
    bus = AsyncEventbus(config, qos)
    with pytest.raises(Exception) as exc_info:
        await bus.publish('ex', 'rk', b'test', 'text/plain', ContentEncoding.Null, 100)
    assert 'Connection is unavailable' in str(exc_info.value)
    await bus.dispose()

def test_message_memoryview_and_constructor():
    m = Message(b'test buffer view', 'text/plain')
    assert m.body == b'test buffer view'
    assert m.content_type == 'text/plain'
    mv = m.memoryview()
    assert isinstance(mv, memoryview)
    assert bytes(mv) == b'test buffer view'
    assert memoryview(m).tobytes() == b'test buffer view'

    m_ba = Message.new(bytearray(b'bytearray buffer'), 'application/octet-stream')
    assert m_ba.body == b'bytearray buffer'
    assert m_ba.memoryview().tobytes() == b'bytearray buffer'

@pytest.mark.asyncio
async def test_publish_bytearray():
    future = Future(loop=get_running_loop())
    options = ConfigOptions(queue_name='test_ba_q', rpc_exchange_name='test_ba_ex', rpc_queue_name='test_ba_rq')
    eventbus = AsyncEventbus(Config(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', options=options, tls_adaptor=None), QoSConfig(pub_confirm=True, rpc_client_confirm=True, rpc_server_confirm=True, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True))
    
    async def handle(body: Message):
        if not future.done():
            future.set_result(body)

    exchange_name = "test_ba"
    routing_key = "test_ba.action"
    message = bytearray(b"bytearray payload content")
    await eventbus.subscribe(exchange_name, routing_key, handle, None, None)
    await eventbus.publish(exchange_name, routing_key, message, "application/octet-stream", ContentEncoding.Null, None)
    await future
    assert future.done()
    response: Message = future.result()
    assert isinstance(response, Message)
    assert response.body == bytes(message)
    await eventbus.dispose()

@pytest.mark.asyncio
async def test_publish_and_rpc_with_message_object():
    future = Future(loop=get_running_loop())
    options = ConfigOptions(queue_name='test_msg_obj_q', rpc_exchange_name='test_msg_obj_ex', rpc_queue_name='test_msg_obj_rq')
    eventbus = AsyncEventbus(Config(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', options=options, tls_adaptor=None), QoSConfig(pub_confirm=True, rpc_client_confirm=True, rpc_server_confirm=True, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True))

    async def handle_rpc(msg: Message):
        mv = msg.memoryview()
        assert len(mv) > 0
        return Message(b"rpc-msg-response", "application/json")

    await eventbus.provide_resource("rpc.msg.test", handle_rpc)

    # 1. Test passing Message to rpc_client
    req_msg = Message(b"rpc-msg-request", "application/json")
    rpc_res = await eventbus.rpc_client("test_msg_obj_ex", "rpc.msg.test", req_msg)
    assert rpc_res == b"rpc-msg-response"

    # 2. Test passing Message to publish
    async def handle_sub(msg: Message):
        if not future.done():
            future.set_result(msg)

    await eventbus.subscribe("test_msg_pub", "test.msg.key", handle_sub)
    pub_msg = Message(b"pub-msg-content", "text/plain")
    await eventbus.publish("test_msg_pub", "test.msg.key", pub_msg)
    await future
    assert future.done()
    rec_msg: Message = future.result()
    assert rec_msg.body == b"pub-msg-content"
    await eventbus.dispose()


