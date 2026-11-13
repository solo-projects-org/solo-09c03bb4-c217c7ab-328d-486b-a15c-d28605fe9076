"""Announcement crawler engine.

One crawl task is created per pasted URL.  The engine enforces:

* per-origin single in-flight request, acquired for every redirect hop,
* integer-second ``Retry-After`` pausing (429) for the *target* origin,
* at most two retries after a 429 for a single hop,
* at most five redirect hops per item,
* per-request timeout; any timeout or exhausted budget marks the item failed,
* failure reasons recorded per item and per batch.

Nothing is persisted: batches and tasks live in this process's memory only.
"""

from __future__ import annotations

import asyncio
import csv
import io
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, List, Optional
from urllib.parse import urljoin

import httpx

from .extract import Announcement, ExtractionError, parse_announcement
from .origin_gate import OriginGate, origin_label, origin_of

MAX_REDIRECTS = 5
MAX_429_RETRIES = 2
DEFAULT_TIMEOUT = 15.0
MAX_BODY_BYTES = 2 * 1024 * 1024


@dataclass
class ItemResult:
    url: str
    status: str = "pending"  # pending | success | failed
    announcement: Optional[Announcement] = None
    reason: Optional[str] = None
    final_url: Optional[str] = None
    redirect_hops: int = 0
    retries_429: int = 0
    attempts: int = 0
    elapsed_ms: int = 0
    failure_kind: Optional[str] = None

    def to_row(self) -> dict:
        a = self.announcement
        return {
            "url": self.url,
            "status": self.status,
            "announcement_id": a.announcement_id if a else "",
            "title": a.title if a else "",
            "deadline_raw": a.deadline_raw if a else "",
            "failure_reason": self.reason or "",
            "final_url": self.final_url or "",
            "redirect_hops": self.redirect_hops,
            "retries_429": self.retries_429,
            "attempts": self.attempts,
        }


class CrawlFailure(Exception):
    def __init__(self, kind: str, reason: str):
        super().__init__(reason)
        self.kind = kind
        self.reason = reason


def _parse_retry_after(headers: httpx.Headers) -> Optional[int]:
    """Return an integer Retry-After value, else None (e.g. HTTP-date)."""
    raw = headers.get("retry-after")
    if raw is None:
        return None
    raw = raw.strip()
    if not raw.isdigit():
        return None
    return int(raw)


async def fetch_one(
    client: httpx.AsyncClient,
    gate: OriginGate,
    url: str,
    semaphore: asyncio.Semaphore,
    timeout: float = DEFAULT_TIMEOUT,
) -> ItemResult:
    """Fetch a single URL through redirects, honouring per-origin gates."""
    result = ItemResult(url=url)
    started = time.monotonic()
    current = url

    try:
        for hop in range(MAX_REDIRECTS + 1):
            try:
                origin = origin_of(current)
            except ValueError as exc:
                raise CrawlFailure("invalid_url", f"无效 URL：{exc}") from exc

            retries_429 = 0
            while True:
                # Acquire the gate of the *current hop's* origin.  Release is
                # done in `finally` around the whole request+read cycle, so the
                # next request for this origin cannot start early.
                await gate.acquire(origin, label=current)
                retry_after: Optional[int] = None
                try:
                    async with semaphore:
                        resp = await client.get(current, follow_redirects=False,
                                                timeout=timeout,
                                                headers={"Accept": "text/html"})
                        # Read the body fully *while holding the gate*: the
                        # response body read is part of the in-flight cycle.
                        body = await resp.aread()
                    # Size check applies to every response, not just 200s.
                    if resp.status_code == 429:
                        # Install the pause *before* releasing the gate so no
                        # queued request for this origin can slip through.
                        retry_after = _parse_retry_after(resp.headers)
                        if retry_after is not None:
                            gate.pause_origin(origin, retry_after)
                    elif len(body) > MAX_BODY_BYTES:
                        raise CrawlFailure(
                            "too_large",
                            f"响应体过大（>{MAX_BODY_BYTES // 1024} KiB）")
                except httpx.TimeoutException as exc:
                    result.attempts += 1
                    raise CrawlFailure(
                        "timeout", f"请求超时（>{timeout:g}s）：{exc.__class__.__name__}"
                    ) from exc
                except httpx.HTTPError as exc:
                    result.attempts += 1
                    raise CrawlFailure(
                        "network", f"网络错误：{exc.__class__.__name__}"
                    ) from exc
                finally:
                    gate.release(origin)

                result.attempts += 1

                if resp.status_code == 429:
                    if retries_429 >= MAX_429_RETRIES:
                        raise CrawlFailure(
                            "http_429",
                            f"429 限流，重试 {MAX_429_RETRIES} 次后仍失败"
                            + (f"（Retry-After={retry_after}s）"
                               if retry_after is not None else "（无整数 Retry-After）"),
                        )
                    retries_429 += 1
                    result.retries_429 += 1
                    # Loop; the next acquire() will wait out the origin pause.
                    continue
                break
            else:  # pragma: no cover - loop always breaks or raises
                raise AssertionError("unreachable")

            if resp.status_code in (301, 302, 303, 307, 308):
                location = resp.headers.get("location")
                if not location:
                    raise CrawlFailure("redirect", "重定向响应缺少 Location")
                if hop == MAX_REDIRECTS:
                    raise CrawlFailure(
                        "redirect_limit",
                        f"超过最大重定向跳数（{MAX_REDIRECTS}）",
                    )
                result.redirect_hops = hop + 1
                current = urljoin(current, location)
                continue

            if resp.status_code != 200:
                snippet = ""
                try:
                    text = body.decode(resp.encoding or "utf-8", errors="replace")
                    snippet = " ".join(text.split())[:120]
                except Exception:
                    pass
                raise CrawlFailure(
                    "http_status",
                    f"HTTP {resp.status_code}"
                    + (f"：{snippet}" if snippet else ""),
                )

            charset = resp.charset_encoding or "utf-8"
            try:
                html = body.decode(charset, errors="replace")
            except LookupError:
                html = body.decode("utf-8", errors="replace")
            try:
                announcement = parse_announcement(html)
            except ExtractionError as exc:
                raise CrawlFailure("parse", f"版式解析失败：{exc}") from exc

            result.announcement = announcement
            result.final_url = current
            result.status = "success"
            return result
    except CrawlFailure as exc:
        result.status = "failed"
        result.reason = exc.reason
        result.failure_kind = exc.kind
        result.final_url = current
        return result
    finally:
        result.elapsed_ms = int((time.monotonic() - started) * 1000)


@dataclass
class Batch:
    batch_id: str
    created_at: float
    urls: List[str]
    items: List[ItemResult] = field(default_factory=list)
    status: str = "pending"  # pending | running | done
    timeout: float = DEFAULT_TIMEOUT
    # Monotonic submission order, persisted by the catalog store.  Used to
    # decide which batch's result wins per URL: the latest submitted batch.
    seq: int = 0

    def summary(self) -> dict:
        success = sum(1 for i in self.items if i.status == "success")
        failed = sum(1 for i in self.items if i.status == "failed")
        return {
            "batch_id": self.batch_id,
            "created_at": self.created_at,
            "status": self.status,
            "total": len(self.urls),
            "success": success,
            "failed": failed,
            "pending": len(self.urls) - success - failed,
        }

    def to_csv(self) -> str:
        out = io.StringIO()
        writer = csv.DictWriter(
            out,
            fieldnames=[
                "url", "status", "announcement_id", "title", "deadline_raw",
                "failure_reason", "final_url", "redirect_hops", "retries_429",
                "attempts",
            ],
            quoting=csv.QUOTE_ALL,
        )
        writer.writeheader()
        for item in self.items:
            writer.writerow(item.to_row())
        return out.getvalue()


class CrawlManager:
    """In-memory batch registry and crawl driver.

    ``seq_provider`` returns the next globally monotonic batch sequence
    (persisted by the catalog store) so "latest submitted batch" still means
    something after a restart.  ``item_callback`` is awaited once per item
    that reaches a terminal state; a successful item is handed to the
    verification catalog there.
    """

    def __init__(self, timeout: float = DEFAULT_TIMEOUT,
                 max_concurrency: int = 8,
                 seq_provider: Optional[Callable[[], Awaitable[int]]] = None,
                 item_callback: Optional[Callable[[Batch, ItemResult],
                                                  Awaitable[None]]] = None):
        self.batches: Dict[str, Batch] = {}
        self.gate = OriginGate()
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._client: Optional[httpx.AsyncClient] = None
        self._seq = 0
        self._seq_provider = seq_provider
        self._item_callback = item_callback

    def _client_for(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout, connect=min(5.0, self.timeout)),
                limits=httpx.Limits(max_connections=20,
                                    max_keepalive_connections=10),
                headers={"User-Agent": "BidNoticeCollector/1.0"},
            )
        return self._client

    async def aclose(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()

    async def new_batch_id(self) -> str:
        self._seq += 1
        return f"B{self._seq:04d}"

    async def create_batch(self, urls: List[str],
                           timeout: Optional[float] = None) -> Batch:
        batch_id = await self.new_batch_id()
        if self._seq_provider is not None:
            seq = await self._seq_provider()
        else:
            seq = self._seq
        batch = Batch(
            batch_id=batch_id,
            created_at=time.time(),
            urls=list(urls),
            timeout=self.timeout if timeout is None else timeout,
            seq=seq,
        )
        batch.items = [ItemResult(url=u) for u in urls]
        self.batches[batch.batch_id] = batch
        return batch

    async def run_batch(self, batch_id: str) -> None:
        batch = self.batches[batch_id]
        batch.status = "running"
        client = self._client_for()

        async def work(idx: int, url: str):
            item = await fetch_one(client, self.gate, url, self.semaphore,
                                   timeout=batch.timeout)
            batch.items[idx] = item
            if self._item_callback is not None:
                # A catalog failure must never kill the crawl itself; the
                # store already rolled its transaction back, so no partial
                # catalog write survives. Log it for the operator.
                try:
                    await self._item_callback(batch, item)
                except Exception:
                    import logging
                    logging.getLogger("app.crawler").exception(
                        "catalog ingest failed for %s (transaction rolled back)",
                        url)

        try:
            await asyncio.gather(*(work(i, u) for i, u in enumerate(batch.urls)))
        finally:
            # A batch is "done" when every item reached a terminal state.
            if all(i.status in ("success", "failed") for i in batch.items):
                batch.status = "done"
            else:  # cancellation / unexpected error
                batch.status = "aborted"
                for i in batch.items:
                    if i.status == "pending":
                        i.status = "failed"
                        i.failure_kind = "aborted"
                        i.reason = "批次中止，未完成"

    def get_batch(self, batch_id: str) -> Optional[Batch]:
        return self.batches.get(batch_id)

    def gate_snapshot(self) -> Dict[str, dict]:
        return self.gate.as_dict()
