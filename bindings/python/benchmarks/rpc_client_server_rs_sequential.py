from amqp_rs import Config, ConfigOptions, AsyncEventbus, QoSConfig, ContentEncoding

import asyncio
from time import time_ns


async def run():
    options = ConfigOptions(queue_name='test_queue', rpc_exchange_name='test_exchange', rpc_queue_name='test_rpc_queue')
    config = Config(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', options=options)
    eventbus = AsyncEventbus(config, QoSConfig(pub_confirm=True, rpc_client_confirm=True, rpc_server_confirm=True, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True, sub_prefetch=None, rpc_server_prefetch=None, rpc_client_prefetch=None))
    await asyncio.sleep(1)
    exchange_name = options.rpc_exchange_name
    routing_key = "abc.example"
    async def handler(body):
        return body
    await eventbus.provide_resource(routing_key, handler)
    await asyncio.sleep(3)
    before = time_ns()
    for _ in range(0, 300_000):
        await eventbus.rpc_client(exchange_name, routing_key, 'Hello, RPC!', 'application/json', ContentEncoding.Null, 50_000, 100)
    after = time_ns()
    print(f"Time taken for 300k messages: {(after - before) / 1_000_000_000} seconds")
    print(f"Mean messages per second for 300k messages: {300_000 / ((after - before) / 1_000_000_000)}")
    await eventbus.dispose()
    # Time taken for 300k messages: 149.954470144 seconds
    # Mean messages per second for 300k messages: 2000.6072490664171
asyncio.run(run())