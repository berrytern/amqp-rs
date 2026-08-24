from amqp_rs import Config, ConfigOptions, AsyncEventbus, QoSConfig, ContentEncoding, TlsAdaptor, Message
from threading import Thread
import asyncio
import uvloop
from time import perf_counter
from json import dumps
import os

from pathlib import Path

CERTS_DIR = Path(__file__).resolve().parents[3] / ".certs" / "amqp"
tls_on = True

cpu_count = os.cpu_count() or 4
uvloop.install()

options = ConfigOptions(queue_name='test_queue', rpc_exchange_name='test_exchange', rpc_queue_name='test_rpc_queue')
if tls_on:
    tls_adaptor = TlsAdaptor.with_client_auth(str(CERTS_DIR / "ca.pem"), str(CERTS_DIR / "rabbitmq_cert.pem"), str(CERTS_DIR / "rabbitmq_key.pem"), "localhost")
    config = Config(host='localhost', port=5671, username='guest', password='guest', virtual_host='/', options=options, tls_adaptor=tls_adaptor)
else:
    config = Config(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', options=options, tls_adaptor=None)
eventbus = AsyncEventbus(config, QoSConfig(pub_confirm=True, rpc_client_confirm=True, rpc_server_confirm=True, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True, sub_prefetch=None, rpc_server_prefetch=None, rpc_client_prefetch=None))
routing_key = "abc.example"
exchange_name = options.rpc_exchange_name
process_count = cpu_count
total_messages = 300_000

async def run(messages: int):
    payload = dumps('Hello, RPC!')
    sended = [eventbus.publish(exchange_name, routing_key, payload, "application/json", ContentEncoding.Null, 100) for _ in range(messages)]
    await asyncio.gather(*sended)

def run_process(messages: int):
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run(messages))
    finally:
        loop.close()

async def handler(message: Message):
    pass

async def main():
    await eventbus.subscribe(exchange_name, routing_key, handler, None, None)
    await asyncio.sleep(3) # wait for subscribe to be ready

    messages_per_worker = total_messages // process_count
    before = perf_counter()
    tasks = [
        asyncio.to_thread(run_process, messages_per_worker)
        for _ in range(process_count)
    ]
    await asyncio.gather(*tasks)
    after = perf_counter()

    print(f"Time taken for {total_messages // 1_000}k messages: {(after - before)} seconds")
    print(f"Mean messages per second for {total_messages // 1_000}k messages: {total_messages / ((after - before))}")

    await eventbus.dispose()
    end = perf_counter()
    print(f"all time: {(end - before)} seconds")
    print(f"time to dispose: {(end - after)} seconds")

if __name__ == '__main__':
    asyncio.run(main())