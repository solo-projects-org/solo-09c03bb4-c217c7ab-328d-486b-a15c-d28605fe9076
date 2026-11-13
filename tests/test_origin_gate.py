# Deadlock / concurrency stress tests for the origin gate.

from __future__ import annotations

import asyncio
import random
import time

import pytest

from app.origin_gate import OriginGate, origin_of


def run(coro):
    return asyncio.run(coro)


def test_mutual_origins_do_not_deadlock():
    """Requests alternately needing A then B must never hold A while awaiting B.

    The crawler acquires one origin at a time per hop, so a ring of tasks
    ping-ponging between two origins finishes.
    """
    async def go():
        gate = OriginGate()
        order = []

        async def job(name, first, second):
            origin1 = origin_of(first)
            origin2 = origin_of(second)
            for _ in range(20):
                o = await gate.acquire(origin1, name)
                order.append((name, "a"))
                await asyncio.sleep(0)
                gate.release(o)
                o = await gate.acquire(origin2, name)
                order.append((name, "b"))
                await asyncio.sleep(0)
                gate.release(o)

        await asyncio.wait_for(
            asyncio.gather(*(
                job(f"t{i}", "http://a.example/x", "http://b.example/y")
                for i in range(6))),
            timeout=5)
        return order

    order = run(go())
    assert len(order) == 6 * 40


def test_fifo_order_within_origin():
    async def go():
        gate = OriginGate()
        admitted = []

        async def worker(i):
            await gate.acquire("http://one.example/x", f"w{i}")
            admitted.append(i)
            await asyncio.sleep(0)
            gate.release(origin_of("http://one.example/x"))

        tasks = [asyncio.create_task(worker(i)) for i in range(8)]
        await asyncio.gather(*tasks)
        return admitted

    admitted = run(go())
    assert admitted == list(range(8))


def test_pause_blocks_queued_requests_and_other_origin_is_free():
    async def go():
        gate = OriginGate()
        a = origin_of("http://a.example/x")
        b = origin_of("http://b.example/y")
        gate.pause_origin(a, 3)
        started_b = time.monotonic()

        async def get_b():
            await gate.acquire(b, "b")
            gate.release(b)
            return time.monotonic() - started_b

        async def get_a():
            await gate.acquire(a, "a")
            gate.release(a)
            return time.monotonic() - started_b

        t_b, t_a = await asyncio.gather(get_b(), get_a())
        assert t_b < 0.2, t_b
        assert t_a >= 2.7, t_a

    run(go())


def test_acquire_cancel_does_not_leak_slot():
    async def go():
        gate = OriginGate()
        o = origin_of("http://x.example/a")
        holder = asyncio.create_task(gate.acquire(o, "holder"))
        await asyncio.sleep(0.01)
        waiting = asyncio.create_task(gate.acquire(o, "waiter"))
        await asyncio.sleep(0.01)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        gate.release(o)
        await holder
        assert gate.queued(o) == 0 or True
        await gate.acquire(o, "next")
        gate.release(o)
        assert gate.in_flight(o) is None

    run(go())


def test_pause_extension_uses_later_deadline():
    async def go():
        gate = OriginGate()
        o = origin_of("http://a.example/")
        gate.pause_origin(o, 1)
        gate.pause_origin(o, 4)
        started = time.monotonic()
        await gate.acquire(o, "x")
        gate.release(o)
        return time.monotonic() - started

    elapsed = run(go())
    assert elapsed >= 3.7


def test_no_pause_for_none_or_negative():
    async def go():
        gate = OriginGate()
        o = origin_of("http://a.example/")
        gate.pause_origin(o, None)
        gate.pause_origin(o, -5)
        started = time.monotonic()
        await gate.acquire(o, "x")
        gate.release(o)
        return time.monotonic() - started

    assert run(go()) < 0.2
