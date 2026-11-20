import pytest
from amqp_rs import AsyncEventbus, Config, ConfigOptions, QoSConfig
from asyncio import Future, wait_for, get_running_loop
from json import dumps




@pytest.mark.asyncio
async def test_subscribe():
    options = ConfigOptions(queue_name='test_queue', rpc_exchange_name='test_exchange', rpc_queue_name='test_rpc_queue')
    eventbus = AsyncEventbus(Config(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', options=options, tls_adaptor=None), QoSConfig(pub_confirm=False, rpc_client_confirm=True, rpc_server_confirm=True, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True, sub_prefetch=None, rpc_server_prefetch=None, rpc_client_prefetch=None))
    expected_result = "received message"
    future = Future(loop = get_running_loop())

    async def handle(_):
        if not future.done():
            future.set_result("received message")

    exchange_name = "example"
    routing_key = "abc.example"
    body = dumps(["hi"])
    await eventbus.subscribe(exchange_name, routing_key, handle)
    print(await eventbus.publish(exchange_name, routing_key, body))
    print("message published")
    await wait_for(future, timeout=1)
    assert future.done()
    assert future.result() == expected_result
    await eventbus.dispose()



@pytest.mark.asyncio
async def test_subscribe_topic():
    options = ConfigOptions(queue_name='test_queue', rpc_exchange_name='test_exchange', rpc_queue_name='test_rpc_queue')
    eventbus = AsyncEventbus(Config(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', options=options, tls_adaptor=None), QoSConfig(pub_confirm=True, rpc_client_confirm=True, rpc_server_confirm=True, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True, sub_prefetch=None, rpc_server_prefetch=None, rpc_client_prefetch=None))
    future = Future(loop = get_running_loop())

    async def handle(_):
        if not future.done():
            future.set_result("received message")

    exchange_name = "example"
    routing_key = "a.example"
    body = dumps(["hi"])
    await eventbus.subscribe(exchange_name, routing_key, handle)
    await eventbus.publish(exchange_name, "abc.example", body, command_timeout=2)
    assert not future.done()
    await eventbus.dispose()


@pytest.mark.asyncio
async def test_subscribe_batch_dispatch():
    import uuid
    from amqp_rs import BatchConfig
    uid = uuid.uuid4().hex[:8]
    options = ConfigOptions(queue_name=f'q_bdisp_{uid}', rpc_exchange_name=f'ex_{uid}', rpc_queue_name=f'rpc_{uid}')
    bc = BatchConfig(enabled=True, max_batch_size=50, max_delay_ms=0)
    eventbus = AsyncEventbus(
        Config(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', options=options, tls_adaptor=None),
        QoSConfig(pub_confirm=True, rpc_client_confirm=True, rpc_server_confirm=True, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True),
        batch_config=bc
    )
    received = []
    done_event = Future(loop=get_running_loop())

    async def handle(msg):
        received.append(msg.body)
        if len(received) >= 20 and not done_event.done():
            done_event.set_result(True)

    ex = f"ex_{uid}"
    rk = f"rk_{uid}"
    await eventbus.subscribe(ex, rk, handle, batch_dispatch=True)

    for i in range(20):
        await eventbus.publish(ex, rk, dumps({"i": i}))

    await wait_for(done_event, timeout=5)
    assert len(received) == 20
    await eventbus.dispose()


@pytest.mark.asyncio
async def test_subscribe_batch_bulk():
    import uuid
    from amqp_rs import BatchConfig
    uid = uuid.uuid4().hex[:8]
    options = ConfigOptions(queue_name=f'q_bulk_{uid}', rpc_exchange_name=f'ex_{uid}', rpc_queue_name=f'rpc_{uid}')
    bc = BatchConfig(enabled=True, max_batch_size=50, max_delay_ms=0)
    eventbus = AsyncEventbus(
        Config(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', options=options, tls_adaptor=None),
        QoSConfig(pub_confirm=True, rpc_client_confirm=True, rpc_server_confirm=True, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True),
        batch_config=bc
    )
    received = []
    done_event = Future(loop=get_running_loop())

    async def batch_handle(messages):
        received.extend(messages)
        if len(received) >= 20 and not done_event.done():
            done_event.set_result(True)

    ex = f"ex_{uid}"
    rk = f"rk_{uid}"
    await eventbus.subscribe_batch(ex, rk, batch_handle, batch_size=20, max_delay_ms=0)

    for i in range(20):
        await eventbus.publish(ex, rk, dumps({"i": i}))

    await wait_for(done_event, timeout=5)
    assert len(received) == 20
    await eventbus.dispose()