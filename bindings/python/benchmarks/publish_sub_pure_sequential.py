from amqp_client_python import Config, Options, AsyncEventbusRabbitMQ

import asyncio
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
    def handler(body):
        pass 
    await eventbus.subscribe(exchange_name, routing_key, handler)
    await asyncio.sleep(3)
    before = time_ns()
    for _ in range(0, 300_000):
        await eventbus.publish(exchange_name, routing_key, 'Hello, RPC!', "application/json")
    after = time_ns()
    print(f"Time taken for 300k messages: {(after - before) / 1_000_000_000} seconds")
    print(f"Mean messages per second for 300k messages: {300_000 / ((after - before) / 1_000_000_000)}")
    await eventbus.dispose(False)
    #Time taken for 300k messages: 87.85540282 seconds
    #Mean messages per second for 300k messages: 3414.701775537315
asyncio.run(run())