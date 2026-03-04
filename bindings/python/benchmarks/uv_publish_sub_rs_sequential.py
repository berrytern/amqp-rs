from amqp_rs import Config, ConfigOptions, AsyncEventbus, QoSConfig

import asyncio
import uvloop
uvloop.install()
from time import time_ns
from json import dumps


async def run():
    options = ConfigOptions(queue_name='test_queue', rpc_exchange_name='test_exchange', rpc_queue_name='test_rpc_queue')
    config = Config(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', options=options)
    eventbus = AsyncEventbus(config, QoSConfig(pub_confirm=True, rpc_client_confirm=True, rpc_server_confirm=True, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True, sub_prefetch=None, rpc_server_prefetch=None, rpc_client_prefetch=None))
    await asyncio.sleep(1)
    exchange_name = options.rpc_exchange_name
    routing_key = "abc.example"
    async def handler(body):
        pass
    await eventbus.subscribe(exchange_name, routing_key, handler)
    await asyncio.sleep(3)
    before = time_ns()
    
    for _ in range(0, 300_000):
        await eventbus.publish(exchange_name, routing_key, dumps('Hello, RPC!'))
    
    after = time_ns()
    print(f"Time taken for 300k messages: {(after - before) / 1_000_000_000} seconds")
    print(f"Mean messages per second for 300k messages: {300_000 / ((after - before) / 1_000_000_000)}")
    #Time taken for 300k messages: 54.575097755 seconds
    #Mean messages per second for 300k messages: 5497.012599899831
    await eventbus.dispose()
asyncio.run(run())