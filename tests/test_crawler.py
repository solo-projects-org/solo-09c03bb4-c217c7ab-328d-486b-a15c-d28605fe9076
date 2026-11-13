"""Behavioural tests for the origin gate and crawler.

They spin up the two real mock portals on ephemeral ports and assert on
server-side concurrency counters, so the guarantees are checked end to end
rather than against mocks.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
from typing import Iterator

import httpx
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.crawler import MAX_429_RETRIES, MAX_REDIRECTS, CrawlManager, fetch_one
from app.origin_gate import OriginGate, origin_of


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_http(url: str, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            httpx.get(url, timeout=0.5)
            return
        except httpx.HTTPError:
            time.sleep(0.15)
    raise RuntimeError(f"server did not come up: {url}")


SOURCE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "sources", "mock_source.py")


@pytest.fixture(scope="session")
def portals() -> Iterator[dict]:
    pa, pb = free_port(), free_port()
    procs = []
    for portal, port, peer in (("A", pa, pb), ("B", pb, pa)):
        env = dict(os.environ,
                   PORTAL=portal,
                   SELF_BASE=f"http://127.0.0.1:{port}",
                   PEER_BASE=f"http://127.0.0.1:{peer}",
                   PORTAL_NAME=f"招标公告源 {portal}")
        procs.append(subprocess.Popen(
            [sys.executable, SOURCE, "--portal", portal, "--port", str(port)],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    try:
        wait_http(f"http://127.0.0.1:{pa}/healthz")
        wait_http(f"http://127.0.0.1:{pb}/healthz")
        yield {"A": f"http://127.0.0.1:{pa}", "B": f"http://127.0.0.1:{pb}"}
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


def run(coro):
    return asyncio.run(coro)


async def fetch(base: str, path: str, gate: OriginGate, **kw):
    client = httpx.AsyncClient(timeout=30)
    try:
        return await fetch_one(client, gate, base + path,
                               asyncio.Semaphore(20), **kw)
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def test_extraction_fields(portals):
    result = run(fetch(portals["A"], "/notice/N2026-001", OriginGate()))
    assert result.status == "success"
    a = result.announcement
    assert a.announcement_id == "ZB-2026-N2026-001"
    assert a.title == "招标公告源 A公告 N2026-001"
    assert a.deadline_raw == "2026-12-01 09:30:00"


def test_parse_failure_reports_reason(portals):
    result = run(fetch(portals["A"], "/notice/broken/N2026-004", OriginGate()))
    assert result.status == "failed"
    assert "版式解析失败" in result.reason
    assert result.failure_kind == "parse"


def test_missing_field_is_a_failure(portals):
    result = run(fetch(portals["A"], "/notice/missing-field/N2026-005",
                       OriginGate()))
    assert result.status == "failed"
    assert "投标截止时间" in result.reason


def test_http_404_reports_status(portals):
    result = run(fetch(portals["A"], "/notice/404/N2026-006", OriginGate()))
    assert result.status == "failed"
    assert "HTTP 404" in result.reason


# ---------------------------------------------------------------------------
# Per-origin serialisation: the server-side max concurrency must stay at 1
# ---------------------------------------------------------------------------

def test_single_in_flight_per_origin(portals):
    async def go():
        gate = OriginGate()
        client = httpx.AsyncClient(timeout=30)
        sem = asyncio.Semaphore(10)
        urls = [portals["A"] + f"/notice/slow/N2026-S{i}?ms=300"
                for i in range(4)]
        try:
            results = await asyncio.gather(*(
                fetch_one(client, gate, u, sem) for u in urls))
        finally:
            await client.aclose()
        assert all(r.status == "success" for r in results)
        stats = httpx.get(portals["A"] + "/__stats").json()
        assert stats["max_active"] == 1, stats

    run(go())


def test_other_origin_continues_while_one_is_slow(portals):
    async def go():
        gate = OriginGate()
        client = httpx.AsyncClient(timeout=30)
        sem = asyncio.Semaphore(10)
        slow = fetch_one(client, gate,
                         portals["A"] + "/notice/slow/N2026-X?ms=1200", sem)
        fasts = [fetch_one(client, gate, portals["B"] + f"/notice/N2026-F{i}", sem)
                 for i in range(3)]
        try:
            slow_res, *fast_res = await asyncio.gather(slow, *fasts)
        finally:
            await client.aclose()
        assert slow_res.status == "success"
        assert all(r.status == "success" for r in fast_res)
        # The fast origin was never blocked by the slow one.
        b_stats = httpx.get(portals["B"] + "/__stats").json()
        assert b_stats["max_active"] == 1

    run(go())


# -----------------------------------------------------------------------------
# 429 handling
# -----------------------------------------------------------------------------

def test_429_pauses_origin_and_request_succeeds_after_retry(portals):
    async def go():
        httpx.post(portals["A"] + "/__reset")
        gate = OriginGate()
        started = time.monotonic()
        result = await fetch(portals["A"],
                             "/notice/limit/N2026-L1?wait=2&times=1", gate,
                             timeout=10)
        elapsed = time.monotonic() - started
        assert result.status == "success"
        assert result.retries_429 == 1
        assert elapsed >= 1.9, elapsed

    run(go())


def test_429_retry_budget_exhaustion(portals):
    async def go():
        result = await fetch(portals["A"],
                             "/notice/always-limit/N2026-L2?wait=1", OriginGate(),
                             timeout=10)
        assert result.status == "failed"
        assert result.failure_kind == "http_429"
        assert result.retries_429 == MAX_429_RETRIES
        assert result.attempts == MAX_429_RETRIES + 1

    run(go())


def test_queued_requests_also_wait_out_pause(portals):
    """A second, already-queued request must not hit the server during the pause."""
    async def go():
        httpx.post(portals["A"] + "/__reset")
        gate = OriginGate()
        client = httpx.AsyncClient(timeout=30)
        sem = asyncio.Semaphore(10)
        try:
            first = asyncio.create_task(fetch_one(
                client, gate,
                portals["A"] + "/notice/limit/N2026-L3?wait=2&times=1", sem))
            await asyncio.sleep(0.35)  # let the 429 land and the pause install
            second = asyncio.create_task(fetch_one(
                client, gate, portals["A"] + "/notice/N2026-Q1", sem))
            r1, r2 = await asyncio.gather(first, second)
            stats = httpx.get(portals["A"] + "/__stats").json()
            assert r1.status == "success" and r2.status == "success"
            assert r1.retries_429 == 1
            # The 429 hit, its retry, and the queued second item.  Both the
            # retry and the queued item must land after the 2s pause deadline.
            hit_times = [h["start"] for h in stats["requests"]
                         if h["path"].startswith("/notice/limit/N2026-L3")]
            queued_times = [h["start"] for h in stats["requests"]
                            if h["path"] == "/notice/N2026-Q1"]
            assert len(hit_times) == 2, stats["requests"]
            assert len(queued_times) == 1, stats["requests"]
            assert queued_times[0] - hit_times[0] >= 1.8
            assert hit_times[1] - hit_times[0] >= 1.8
        finally:
            await client.aclose()

    run(go())


def test_non_integer_retry_after_does_not_pause(portals):
    async def go():
        result = await fetch(portals["A"], "/notice/date-limit/N2026-L4",
                             OriginGate(), timeout=10)
        assert result.status == "failed"
        assert result.failure_kind == "http_429"
        assert result.retries_429 == MAX_429_RETRIES
        # Three rapid attempts, no multi-second pause.
        assert result.elapsed_ms < 3000

    run(go())


# -----------------------------------------------------------------------------
# Redirects
# -------------------------------------------------------------------------

def test_redirect_chain_across_origins_succeeds(portals):
    async def go():
        httpx.post(portals["A"] + "/__reset")
        httpx.post(portals["B"] + "/__reset")
        result = await fetch(portals["A"], "/notice/chain/0", OriginGate())
        assert result.status == "success"
        assert result.redirect_hops == MAX_REDIRECTS
        assert result.announcement.announcement_id == "ZB-2026-N2026-CHAIN-5"

    run(go())


def test_redirect_loop_hits_five_hop_cap(portals):
    async def go():
        httpx.post(portals["A"] + "/__reset")
        httpx.post(portals["B"] + "/__reset")
        gate = OriginGate()
        started = time.monotonic()
        result = await fetch(portals["A"], "/notice/loop", gate, timeout=8)
        elapsed = time.monotonic() - started
        assert result.status == "failed"
        assert result.failure_kind == "redirect_limit"
        assert result.redirect_hops == MAX_REDIRECTS
        # Mutual bouncing must not deadlock: it terminates quickly.
        assert elapsed < 3, elapsed
        # Each portal saw at most 1 concurrent request throughout.
        for base in (portals["A"], portals["B"]):
            stats = httpx.get(base + "/__stats").json()
            assert stats["max_active"] == 1, stats

    run(go())


def test_redirect_gate_applies_per_target_origin(portals):
    """While A is redirecting to B, a parallel B request must interleave."""
    async def go():
        gate = OriginGate()
        client = httpx.AsyncClient(timeout=30)
        sem = asyncio.Semaphore(10)
        try:
            chain = asyncio.create_task(fetch_one(
                client, gate, portals["A"] + "/notice/hop/0", sem, timeout=8))
            await asyncio.sleep(0.05)
            others = [asyncio.create_task(fetch_one(
                client, gate, portals["B"] + f"/notice/slow/N2026-R{i}?ms=200",
                sem)) for i in range(2)]
            r = await asyncio.gather(chain, *others)
        finally:
            await client.aclose()
        assert r[0].status == "failed" and r[0].failure_kind == "redirect_limit"
        assert all(x.status == "success" for x in r[1:])
        for base in (portals["A"], portals["B"]):
            assert httpx.get(base + "/__stats").json()["max_active"] == 1

    run(go())


def test_timeout_marks_failure(portals):
    async def go():
        gate = OriginGate()
        result = await fetch(portals["A"],
                             "/notice/slow/N2026-T1?ms=2500", gate,
                             timeout=0.5)
        assert result.status == "failed"
        assert result.failure_kind == "timeout"
        # The same gate must be free again: no leaked in-flight slot.
        follow = await fetch(portals["A"], "/notice/N2026-T2", gate, timeout=5)
        assert follow.status == "success"
        assert gate.in_flight(origin_of(portals["A"])) is None

    run(go())


# -----------------------------------------------------------------------------
# Batch manager: multiple batches, per-batch CSV, shared gates
# -----------------------------------------------------------------------------

def test_batches_run_concurrently_and_export_csv(portals):
    async def go():
        httpx.post(portals["A"] + "/__reset")
        mgr = CrawlManager(timeout=10)
        b1 = await mgr.create_batch([portals["A"] + "/notice/slow/N2026-B1?ms=500",
                                     portals["A"] + "/notice/N2026-B2"])
        b2 = await mgr.create_batch([portals["B"] + "/notice/N2026-C1",
                                     portals["B"] + "/notice/broken/N2026-C2",
                                     "ftp://example.com/x"])
        t0 = time.monotonic()
        await asyncio.gather(mgr.run_batch(b1.batch_id),
                             mgr.run_batch(b2.batch_id))
        elapsed = time.monotonic() - t0
        assert b1.status == "done" and b2.status == "done"
        # A's two items serialise: >= 0.5s. B runs in parallel, so overall
        # total stays near 0.5s rather than 0.5+something.
        assert elapsed < 2.0, elapsed
        assert b1.summary()["success"] == 2
        assert b2.summary()["failed"] == 2
        kinds = [i.failure_kind for i in b2.items if i.status == "failed"]
        assert kinds == ["http_status", "parse", "invalid_url"] or \
            "invalid_url" in kinds
        csv_text = b1.to_csv()
        assert csv_text.splitlines()[0].startswith('"url"')
        assert "ZB-2026-N2026-B1" in csv_text
        # Shared gate: A's peak concurrency is still 1 across both batches.
        stats = httpx.get(portals["A"] + "/__stats").json()
        assert stats["max_active"] == 1, stats
        await mgr.aclose()

    run(go())


def test_origin_normalisation():
    assert origin_of("http://Example.com/a") == origin_of("http://example.com:80/b")
    assert origin_of("https://host/x")[2] == 443
    with pytest.raises(ValueError):
        origin_of("ftp://host/x")


# -----------------------------------------------------------------------------
# Web layer (no Docker needed)
# -----------------------------------------------------------------------------

def test_web_paste_flow_and_csv(portals, monkeypatch):
    """End-to-end through the FastAPI app: paste -> batch -> poll -> CSV."""
    from fastapi.testclient import TestClient
    import importlib

    import app.main as main

    main.SOURCE_A_PUBLIC = portals["A"]
    main.SAMPLE_A = portals["A"]
    main.SAMPLE_B = portals["B"]

    # TestClient runs its own loop; make the app's manager use it.
    if main.manager._client is not None:
        pass
    with TestClient(main.app) as client:
        # Each TestClient port/loop needs its manager reset after the test.
        # Only invalid lines -> redirected back to the form with a message.
        resp = client.post("/collect", data={
            "urls": "# comment\nnot-a-url\nftp://example.com/x\n"})
        assert resp.status_code == 200
        assert "无法提交" in resp.text

        resp = client.post("/collect", data={
            "urls": f"{portals['A']}/notice/N2026-W2\n"
                    f"{portals['A']}/notice/broken/N2026-W3\n"},
            follow_redirects=False)
        assert resp.status_code == 303
        location = resp.headers["location"]
        batch_id = location.rsplit("/", 1)[1]

        for _ in range(80):
            data = client.get(f"/api/batches/{batch_id}").json()
            if data["batch"]["status"] in ("done", "aborted"):
                break
            time.sleep(0.1)
        assert data["batch"]["status"] == "done"
        assert data["batch"]["success"] == 1
        assert data["batch"]["failed"] == 1
        bad = [i for i in data["items"] if i["status"] == "failed"][0]
        assert "版式解析失败" in bad["reason"]

        # Detail page and CSV are served for the batch.
        assert client.get(f"/batches/{batch_id}").status_code == 200
        csv_resp = client.get(f"/batches/{batch_id}/csv")
        assert csv_resp.status_code == 200
        assert "attachment" in csv_resp.headers["content-disposition"]
        assert '"ZB-2026-N2026-W2"' in csv_resp.content.decode("utf-8-sig")

        # Unknown batch -> 404, not a traceback.
        assert client.get("/batches/NOPE").status_code == 404

    importlib.reload(main)
    # Other tests use asyncio.run; drop any client bound to TestClient's loop.
    main.manager._client = None
    main.manager.batches.clear()
