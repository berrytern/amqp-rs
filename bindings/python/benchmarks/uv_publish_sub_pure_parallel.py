from amqp_client_python import Config, Options, rabbitmq
from threading import Thread
import uvloop
uvloop.install()
from time import perf_counter, sleep
from json import dumps


options = Options(queue_name='test_queue', rpc_exchange_name='test_exchange', rpc_queue_name='test_rpc_queue')
config = Config(options=options)
eventbus = rabbitmq.eventbus_wrapper_rabbitmq.EventbusWrapperRabbitMQ(
config, None,
pub_publisher_confirms=True, rpc_client_publisher_confirms=True, rpc_server_publisher_confirms=False, sub_auto_ack=True, rpc_server_auto_ack=True, rpc_client_auto_ack=True)
exchange_name = options.rpc_exchange_name
routing_key = "abc.example"
sleep(1) # wait for subscribe to be ready
process_count = 6
total_messages = 5_000
def run(messages: int):
    sended = []
    payload = dumps('Hello, RPC!')
    sended = [eventbus.publish('test_exchange', "abc.example", payload, "application/json", 100) for _ in range(messages)]
    for future in sended:
        future.result()
    #await eventbus.dispose()
def run_thread(messages):
    run(messages)


if __name__ == '__main__':
    eventbus.subscribe(options.rpc_exchange_name, routing_key, lambda x:None, None, None).result()
    sleep(3) # wait for subscribe to be ready

    processes = [
        Thread(
            None,
            run_thread,
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
    # eventbus.dispose()
    print(f"Time taken for {total_messages // 1_000}k messages: {(after - before)} seconds")
    print(f"Mean messages per second for {total_messages // 1_000}k messages: {total_messages / ((after - before))}")
    end = perf_counter()
    print(f"all time: {(end - before)} seconds")
    print(f"time to dispose: {(end - after)} seconds")
    # Failling