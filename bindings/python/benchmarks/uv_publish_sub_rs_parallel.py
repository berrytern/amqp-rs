from amqp_rs import Config, ConfigOptions, AsyncEventbus, QoSConfig, TlsAdaptor
from threading import Thread
import asyncio
import uvloop
from time import perf_counter, sleep
from json import dumps
import os
cpu_count = os.cpu_count()
uvloop.install()

options = ConfigOptions(queue_name='test_queue', rpc_exchange_name='test_exchange', rpc_queue_name='test_rpc_queue')
tls_adaptor = None #TlsAdaptor(None, "path/to/cert.pem", "path/to/key.pem", None)
config = Config(host='localhost', port=5672, username='guest', password='guest', virtual_host='/', options=options, tls_adaptor=None)
eventbus = AsyncEventbus(config, QoSConfig(pub_confirm=True, rpc_client_confirm=True, rpc_server_confirm=True, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True, sub_prefetch=None, rpc_server_prefetch=None, rpc_client_prefetch=None))
routing_key = "abc.example"
exchange_name = options.rpc_exchange_name
sleep(1) # wait for subscribe to be ready
process_count = cpu_count
total_messages = 300_000
async def run(messages: int):
    sended = []
    payload = bytes(dumps('Hello, RPC!'), "utf-8")
    sended = [eventbus.publish('test_exchange', "abc.example", payload) for _ in range(messages)]
    await asyncio.gather(*sended)
    #await eventbus.dispose()
def run_process(messages):
    import uvloop
    uvloop.install()
    asyncio.run(run(messages))

async def handler(message):
    pass

# 4. Guard the main execution block (Required for multiprocessing in Python)
if __name__ == '__main__':
    async def subscribe():
        await eventbus.subscribe(options.rpc_exchange_name, routing_key, handler, None, None)
    asyncio.run(subscribe())
    sleep(3) # wait for subscribe to be ready

    processes = [
        Thread(
            None,
            run_process,
            None,
            (int(total_messages/process_count),)
        )
        for _ in range(process_count)
    ]
    for p in processes:
        p.start()
    before = perf_counter()
    for p in processes:
        p.join(20)
    after = perf_counter()
    async def dispose():
        await eventbus.dispose()
    #asyncio.run(dispose())
    print(f"Time taken for {total_messages // 1_000}k messages: {(after - before)} seconds")
    print(f"Mean messages per second for {total_messages // 1_000}k messages: {total_messages / ((after - before))}")
    end = perf_counter()
    print(f"all time: {(end - before)} seconds")
    print(f"time to dispose: {(end - after)} seconds")
    #Time taken for 300k messages: 10.52721416499844 seconds
    #Mean messages per second for 300k messages: 28497.56785583971
    asyncio.run(dispose())
    del eventbus
    del config
    del options