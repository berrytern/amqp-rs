from amqp_client_python import Config, Options, AsyncEventbusRabbitMQ

import asyncio
import uvloop
from time import perf_counter
from json import dumps
uvloop.install()

tls_on = True

async def run():
    try:
        options = Options(queue_name='test_queue', rpc_exchange_name='test_exchange', rpc_queue_name='test_rpc_queue')
        config = Config(options=options)
        eventbus = AsyncEventbusRabbitMQ(
        config, None,
        pub_publisher_confirms=True, rpc_client_publisher_confirms=True, rpc_server_publisher_confirms=False, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True)
        await asyncio.sleep(1)
        exchange_name = options.rpc_exchange_name
        routing_key = "abc.example"
        async def handler(message):
            return dumps('Hello, RPC!')
        await eventbus.provide_resource(routing_key, handler, None, None)
        await asyncio.sleep(3)
        sended = []
        total = 50_000
        payload = dumps('Hello, RPC!'*1000)
        before = perf_counter()
    
        # Queue all futures
        sended = [eventbus.rpc_client(exchange_name, routing_key, payload, "application/json", 160, 160) for _ in range(total)]
        
        # Wait for all confirmations
        await asyncio.gather(*sended)
        after = perf_counter()
        print(f"Time taken for {total // 1_000}k messages: {(after - before)} seconds")
        print(f"Mean messages per second for {total // 1_000}k messages: {total / ((after - before))}")
        await eventbus.dispose()
        end = perf_counter()
        print(f"all time: {(end - before)} seconds")
        print(f"time to dispose: {(end - after)} seconds")
    finally:
        await eventbus.dispose()
asyncio.run(run())