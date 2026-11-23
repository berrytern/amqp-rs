from __future__ import annotations

import functools
import uuid
from asyncio import Future, get_running_loop, wait_for

import pytest
from amqp_rs import AsyncEventbus, BatchConfig, Config, ConfigOptions, Message, QoSConfig


def _make_bus(uid: str, batch_enabled: bool = False) -> AsyncEventbus:
    options = ConfigOptions(
        queue_name=f"q_ht_{uid}",
        rpc_exchange_name=f"rpc_ex_ht_{uid}",
        rpc_queue_name=f"rpc_q_ht_{uid}",
    )
    cfg = Config(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
        virtual_host="/",
        options=options,
        tls_adaptor=None,
    )
    qos = QoSConfig(
        pub_confirm=True,
        rpc_client_confirm=True,
        rpc_server_confirm=True,
        sub_auto_ack=True,
        rpc_server_auto_ack=True,
        rpc_client_auto_ack=True,
    )
    bc = BatchConfig(enabled=batch_enabled, max_batch_size=10, max_delay_ms=0)
    return AsyncEventbus(cfg, qos, batch_config=bc)


@pytest.mark.asyncio
async def test_subscribe_typed_handlers():
    """Verify bytes, memoryview, str, Message, and unannotated handlers (with PEP 563 active)."""
    uid = uuid.uuid4().hex[:8]
    bus = _make_bus(uid)
    ex = f"ex_ht_{uid}"

    f_bytes: Future = Future(loop=get_running_loop())
    f_mv: Future = Future(loop=get_running_loop())
    f_str: Future = Future(loop=get_running_loop())
    f_msg: Future = Future(loop=get_running_loop())
    f_unann: Future = Future(loop=get_running_loop())

    async def on_bytes(body: bytes):
        if not f_bytes.done():
            f_bytes.set_result((type(body), body))

    async def on_mv(view: memoryview):
        if not f_mv.done():
            f_mv.set_result((type(view), bytes(view)))

    async def on_str(text: str):
        if not f_str.done():
            f_str.set_result((type(text), text))

    async def on_msg(msg: Message):
        if not f_msg.done():
            f_msg.set_result((type(msg), msg.body, msg.routing_key))

    async def on_unannotated(msg):
        if not f_unann.done():
            f_unann.set_result((type(msg), msg.body))

    await bus.subscribe(ex, f"rk.bytes.{uid}", on_bytes)
    await bus.subscribe(ex, f"rk.mv.{uid}", on_mv)
    await bus.subscribe(ex, f"rk.str.{uid}", on_str)
    await bus.subscribe(ex, f"rk.msg.{uid}", on_msg)
    await bus.subscribe(ex, f"rk.unann.{uid}", on_unannotated)

    await bus.publish(ex, f"rk.bytes.{uid}", b"hello-bytes")
    await bus.publish(ex, f"rk.mv.{uid}", b"hello-mv")
    await bus.publish(ex, f"rk.str.{uid}", "hello-str-áéí")
    await bus.publish(ex, f"rk.msg.{uid}", b"hello-msg")
    await bus.publish(ex, f"rk.unann.{uid}", b"hello-unann")

    assert await wait_for(f_bytes, timeout=3) == (bytes, b"hello-bytes")
    assert await wait_for(f_mv, timeout=3) == (memoryview, b"hello-mv")
    assert await wait_for(f_str, timeout=3) == (str, "hello-str-áéí")
    assert await wait_for(f_msg, timeout=3) == (Message, b"hello-msg", f"rk.msg.{uid}")
    assert await wait_for(f_unann, timeout=3) == (Message, b"hello-unann")

    await bus.dispose()


@pytest.mark.asyncio
async def test_subscribe_batch_and_dispatch_typed():
    uid = uuid.uuid4().hex[:8]
    bus = _make_bus(uid, batch_enabled=True)
    ex = f"ex_htb_{uid}"

    f_disp: Future = Future(loop=get_running_loop())
    f_bulk: Future = Future(loop=get_running_loop())

    async def on_disp_str(text: str):
        if not f_disp.done():
            f_disp.set_result((type(text), text))

    async def on_bulk_bytes(items: list[bytes]):
        if not f_bulk.done():
            f_bulk.set_result(([type(x) for x in items], items))

    await bus.subscribe(ex, f"rk.disp.{uid}", on_disp_str, batch_dispatch=True)
    await bus.subscribe_batch(ex, f"rk.bulk.{uid}", on_bulk_bytes, batch_size=2, max_delay_ms=50)

    await bus.publish(ex, f"rk.disp.{uid}", "batch-dispatch-str")
    await bus.publish(ex, f"rk.bulk.{uid}", b"item-1")
    await bus.publish(ex, f"rk.bulk.{uid}", b"item-2")

    assert await wait_for(f_disp, timeout=3) == (str, "batch-dispatch-str")
    types_list, vals = await wait_for(f_bulk, timeout=3)
    assert all(t is bytes for t in types_list)
    assert b"item-1" in vals

    await bus.dispose()


@pytest.mark.asyncio
async def test_rpc_and_edge_cases_partial_method_wraps():
    uid = uuid.uuid4().hex[:8]
    bus = _make_bus(uid)
    rpc_ex = f"rpc_ex_ht_{uid}"

    # RPC with str annotation
    async def echo_str(text: str) -> str:
        assert isinstance(text, str)
        return f"echo:{text}"

    # RPC with bytes annotation wrapped with @functools.wraps
    def passthrough(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            return await fn(*args, **kwargs)
        return wrapper

    @passthrough
    async def echo_bytes(data: bytes) -> bytes:
        assert isinstance(data, bytes)
        return b"bytes:" + data

    # Bound method with memoryview annotation
    class Service:
        async def handle_mv(self, view: memoryview) -> bytes:
            assert isinstance(view, memoryview)
            return b"mv:" + bytes(view)

    # functools.partial with pre-bound prefix
    async def prefixed(prefix: str, body: str) -> str:
        assert isinstance(body, str)
        return f"{prefix}:{body}"

    svc = Service()
    await bus.provide_resource(f"rpc.str.{uid}", echo_str)
    await bus.provide_resource(f"rpc.bytes.{uid}", echo_bytes)
    await bus.provide_resource(f"rpc.mv.{uid}", svc.handle_mv)
    await bus.provide_resource(f"rpc.partial.{uid}", functools.partial(prefixed, "pre"))

    assert await bus.rpc_client(rpc_ex, f"rpc.str.{uid}", "ola") == b"echo:ola"
    assert await bus.rpc_client(rpc_ex, f"rpc.bytes.{uid}", b"raw") == b"bytes:raw"
    assert await bus.rpc_client(rpc_ex, f"rpc.mv.{uid}", b"zero") == b"mv:zero"
    assert await bus.rpc_client(rpc_ex, f"rpc.partial.{uid}", "part") == b"pre:part"

    await bus.dispose()
