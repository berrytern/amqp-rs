from amqp_rs import Config, ConfigOptions, AsyncEventbus, QoSConfig, ContentEncoding, TlsAdaptor, Message

import asyncio
import uvloop
from time import perf_counter
from json import dumps
uvloop.install()

tls_on = True

async def run():
    try:
        options = ConfigOptions(queue_name='test_queue', rpc_exchange_name='test_exchange', rpc_queue_name='test_rpc_queue')
        if tls_on:
            tls_adaptor = TlsAdaptor.with_client_auth("./.certs/amqp/ca.pem", "./.certs/amqp/rabbitmq_cert.pem", "./.certs/amqp/rabbitmq_key.pem", "localhost")
            config = Config(host='localhost', port=5671, username='guest', password='guest', virtual_host='/', options=options, tls_adaptor=tls_adaptor)
        else:
            config = Config(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', options=options, tls_adaptor=None)
        eventbus = AsyncEventbus(config, QoSConfig(pub_confirm=True, rpc_client_confirm=True, rpc_server_confirm=True, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True, sub_prefetch=None, rpc_server_prefetch=None, rpc_client_prefetch=None))
        await asyncio.sleep(1)
        exchange_name = options.rpc_exchange_name
        routing_key = "abc.example"
        async def handler(message: Message):
            return message
        await eventbus.provide_resource(routing_key, handler, None, None)
        await asyncio.sleep(3)
        sended = []
        total = 300_000
        payload = dumps('Hello, RPC!')
        before = perf_counter()
    
        # Queue all futures
        sended = [eventbus.rpc_client(exchange_name, routing_key, payload, "application/json", ContentEncoding.Null, 160000) for _ in range(total)]
        
        # Wait for all confirmations
        await asyncio.gather(*sended)
        after = perf_counter()
        print(f"Time taken for {total // 1_000}k messages: {(after - before)} seconds")
        print(f"Mean messages per second for {total // 1_000}k messages: {total / ((after - before))}")
        await eventbus.dispose()
        end = perf_counter()
        print(f"all time: {(end - before)} seconds")
        print(f"time to dispose: {(end - after)} seconds")
        #Time taken for 300k messages: 12.519942352002545 seconds
        #Mean messages per second for 300k messages: 23961.771673175113
    finally:
        await eventbus.dispose()
asyncio.run(run())