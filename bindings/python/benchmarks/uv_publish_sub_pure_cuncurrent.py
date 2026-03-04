from amqp_client_python import Config, Options, AsyncEventbusRabbitMQ

import asyncio
import uvloop
uvloop.install()
from time import time_ns


async def run():
    options = Options(queue_name='test_queue', rpc_exchange_name='test_exchange', rpc_queue_name='test_rpc_queue')
    config = Config(options=options)
    eventbus = AsyncEventbusRabbitMQ(
        config, None,
        pub_publisher_confirms=True, rpc_client_publisher_confirms=True, rpc_server_publisher_confirms=False, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True)
    await asyncio.sleep(1)
    exchange_name = options.rpc_exchange_name
    routing_key = "abc.example"
    async def handler(_):
        pass#await eventbus.publish(exchange_name+"2", routing_key+"2", 'Hello, RPC!', "application/json", 100)
    async def handler2(_):
        pass
    await eventbus.subscribe(exchange_name, routing_key, handler, None, None)
    #await eventbus.subscribe(exchange_name+"2", routing_key+"2", handler2, None, None)
    await asyncio.sleep(3)
    #await eventbus.dispose()
    sended = []
    total = 300_000
    before = time_ns()
    sended = [eventbus.publish(exchange_name, routing_key, 'Hello, RPC!', "application/json", 2000,2000) for _ in range(0, total)]
    await asyncio.gather(*sended)
    after = time_ns()
    print(f"Time taken for 300k messages: {(after - before) / 1_000_000_000} seconds")
    print(f"Mean messages per second for 300k messages: {300_000 / ((after - before) / 1_000_000_000)}")
    await eventbus.dispose(False)
    end = time_ns()
    print(f"all time: {(end - before) / 1_000_000_000} seconds")
    print(f"time to dispose: {(end - after) / 1_000_000_000} seconds")
    #Time taken for 300k messages: 75.57593714 seconds
    #Mean messages per second for 300k messages: 3969.5174330986806
asyncio.run(run())