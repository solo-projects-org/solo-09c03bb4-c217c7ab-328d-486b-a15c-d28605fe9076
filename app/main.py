"""FastAPI web UI for the announcement collector.

Batches still live in memory and vanish when the process exits, but the
verification catalog (latest successful result per URL, per-announcement
state and clerk verifications) is persisted in a SQLite database inside the
mounted data volume, so it survives restarts.  Multiple batches can be
submitted and run concurrently; they all share the same per-origin gates.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from typing import List, Optional
from urllib.parse import quote, urlsplit

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from .crawler import DEFAULT_TIMEOUT, MAX_429_RETRIES, MAX_REDIRECTS, CrawlManager, ItemResult
from .origin_gate import origin_of
from .store import (
    InvalidChoiceError,
    StaleVersionError,
    UnknownAnnouncementError,
    CatalogStore,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
# Links shown in the browser (host-side) may differ from the URLs the
# collector must dial (container/network side).
SOURCE_A = os.environ.get("SOURCE_A_URL", "http://127.0.0.1:8001")
SOURCE_B = os.environ.get("SOURCE_B_URL", "http://127.0.0.1:8002")
SOURCE_A_PUBLIC = os.environ.get("SOURCE_A_PUBLIC_URL", SOURCE_A)
SOURCE_B_PUBLIC = os.environ.get("SOURCE_B_PUBLIC_URL", SOURCE_B)
# Base for the one-click sample list; defaults to the public URLs.
SAMPLE_A = os.environ.get("SAMPLE_BASE_A", SOURCE_A_PUBLIC)
SAMPLE_B = os.environ.get("SAMPLE_BASE_B", SOURCE_B_PUBLIC)

MAX_URLS_PER_BATCH = 200

# SQLite catalog. In the container the compose file points this at the named
# volume (/data/catalog.db); outside Docker it defaults to a local ./data dir.
CATALOG_DB = os.environ.get("CATALOG_DB", os.path.join(os.getcwd(), "data",
                                                        "catalog.db"))
store = CatalogStore(db_path=CATALOG_DB)


async def _on_item_done(batch, item: ItemResult) -> None:
    """Hand a finished item to the persistent catalog.

    Only successes are ingested: a failed crawl must not remove a value an
    earlier successful batch already stored.
    """
    if item.status != "success" or item.announcement is None:
        return
    a = item.announcement
    await store.ingest_success(
        batch_id=batch.batch_id,
        batch_seq=batch.seq,
        submitted_url=item.url,
        announcement_id=a.announcement_id,
        title=a.title,
        deadline_raw=a.deadline_raw,
        final_url=item.final_url,
    )


manager = CrawlManager(
    timeout=float(os.environ.get("REQUEST_TIMEOUT", DEFAULT_TIMEOUT)),
    seq_provider=store.next_batch_seq,
    item_callback=_on_item_done,
)
# Strong references to background batch tasks (see /collect).
_tasks: dict[str, asyncio.Task] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    store.open()
    try:
        yield
    finally:
        await manager.aclose()
        store.close()


app = FastAPI(title="招标公告采集", lifespan=lifespan)
templates = Jinja2Templates(directory=TEMPLATES_DIR)
templates.env.globals.update(
    source_a=SOURCE_A_PUBLIC,
    source_b=SOURCE_B_PUBLIC,
    max_redirects=MAX_REDIRECTS,
    max_retries=MAX_429_RETRIES,
)


def _fmt_time(ts: float) -> str:
    from datetime import datetime
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


templates.env.filters["timestamp"] = _fmt_time


def parse_url_text(text: str) -> tuple[List[str], List[str]]:
    """Split pasted text into URLs. Returns (urls, rejected_lines).

    Blank lines and lines starting with ``#`` are ignored.  Anything else must
    be a single absolute http(s) URL; otherwise it is reported as invalid and
    not included in the batch.
    """
    urls: List[str] = []
    rejected: List[str] = []
    for raw in text.splitlines():
        line = raw.strip().strip('"').strip("'")
        if not line or line.startswith("#"):
            continue
        # Tolerate "序号<TAB>URL" or whitespace-separated pastes: take the
        # first token that looks like a URL.
        tokens = line.split()
        candidate = None
        for tok in tokens:
            if tok.startswith("http://") or tok.startswith("https://"):
                candidate = tok
                break
        if candidate is None:
            rejected.append(line)
            continue
        try:
            parts = urlsplit(candidate)
            if parts.scheme not in ("http", "https") or not parts.hostname:
                rejected.append(candidate)
                continue
            port = parts.port  # may raise ValueError
            if port is not None and not (0 < port < 65536):
                rejected.append(candidate)
                continue
            origin_of(candidate)
        except ValueError:
            rejected.append(candidate)
            continue
        urls.append(candidate)
    if len(urls) > MAX_URLS_PER_BATCH:
        rejected.extend(urls[MAX_URLS_PER_BATCH:])
        urls = urls[:MAX_URLS_PER_BATCH]
    return urls, rejected


@app.get("/", response_class=HTMLResponse)
async def index(request: Request, invalid: Optional[str] = None):
    batches = list(manager.batches.values())
    batches.sort(key=lambda b: b.created_at, reverse=True)
    return templates.TemplateResponse(request, "index.html", {
        "batches": batches,
        "invalid": invalid or "",
        "limits": {"max_urls": MAX_URLS_PER_BATCH},
        "samples": sample_text(),
        "verify_samples": verification_sample_text(),
    })


def sample_text() -> str:
    """Ready-to-paste rehearsal list covering every behaviour."""
    return "\n".join([
        "# 正常公告（两站各一条）",
        f"{SAMPLE_A}/notice/N2026-001",
        f"{SAMPLE_B}/notice/N2026-101",
        "",
        "# 慢响应：同一站点串行，响应体读完后才发下一条",
        f"{SAMPLE_A}/notice/slow/N2026-002?ms=2500",
        f"{SAMPLE_B}/notice/slow/N2026-102?ms=1500",
        "",
        "# 429：首次返回 Retry-After=3，暂停该站后续排队请求，重试一次后成功",
        f"{SAMPLE_A}/notice/limit/N2026-003?wait=3&times=1",
        "",
        "# 429 持续出现：重试两次后失败",
        f"{SAMPLE_B}/notice/always-limit/N2026-103?wait=1",
        "",
        "# 跨站跳转 A→B→A→B→A→B，5 跳后成功",
        f"{SAMPLE_A}/notice/chain/0",
        "",
        "# 两站互跳的无限循环：5 跳上限后失败，不能卡死",
        f"{SAMPLE_A}/notice/loop",
        "",
        "# 版式/字段/状态类失败",
        f"{SAMPLE_A}/notice/broken/N2026-004",
        f"{SAMPLE_A}/notice/missing-field/N2026-005",
        f"{SAMPLE_B}/notice/404/N2026-106",
    ])


@app.post("/collect")
async def collect(request: Request,
                  urls: str = Form(""),
                  timeout: Optional[str] = Form(None),
                  action: str = Form("start")):
    """Create a batch from pasted URLs and start it in the background."""
    parsed, rejected = parse_url_text(urls)
    if not parsed:
        msg = "没有识别到有效的 http/https URL"
        if rejected:
            msg += "；无法识别的行：" + "、".join(rejected[:5])
        return RedirectResponse(f"/?invalid={quote(msg)}", status_code=303)

    if action == "preview":
        # Return a preview without creating a batch.
        return templates.TemplateResponse(request, "preview.html", {
            "parsed": parsed, "rejected": rejected,
            "max_urls": MAX_URLS_PER_BATCH,
        })

    parsed_timeout = None
    if timeout:
        try:
            parsed_timeout = float(timeout)
            if not (0.1 <= parsed_timeout <= 300):
                raise ValueError
        except ValueError:
            return RedirectResponse(
                "/?invalid=" + quote("超时必须是 0.1–300 秒之间的数字"),
                status_code=303)

    batch = await manager.create_batch(parsed, timeout=parsed_timeout)
    # Multiple batches may run at once; the per-origin gates keep each site to
    # one in-flight request regardless of how many batches are active.
    # Keep a strong reference: a bare create_task() may be garbage-collected
    # while still running (documented in the asyncio docs).
    _tasks[batch.batch_id] = asyncio.create_task(manager.run_batch(batch.batch_id))

    def _forget(_):
        _tasks.pop(batch.batch_id, None)

    _tasks[batch.batch_id].add_done_callback(_forget)
    return RedirectResponse(f"/batches/{batch.batch_id}", status_code=303)


@app.get("/batches/{batch_id}", response_class=HTMLResponse)
async def batch_detail(request: Request, batch_id: str):
    batch = manager.get_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="批次不存在（任务只存内存，重启后清空）")
    return templates.TemplateResponse(request, "batch.html", {
        "batch": batch,
        "summary": batch.summary(),
        "origins": manager.gate_snapshot(),
    })


@app.get("/api/batches/{batch_id}")
async def batch_api(batch_id: str):
    batch = manager.get_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="batch not found")
    return JSONResponse({
        "batch": batch.summary(),
        "items": [
            {
                "url": i.url,
                "status": i.status,
                "reason": i.reason,
                "failure_kind": i.failure_kind,
                "final_url": i.final_url,
                "redirect_hops": i.redirect_hops,
                "retries_429": i.retries_429,
                "attempts": i.attempts,
                "elapsed_ms": i.elapsed_ms,
                "announcement_id": i.announcement.announcement_id if i.announcement else None,
                "title": i.announcement.title if i.announcement else None,
                "deadline_raw": i.announcement.deadline_raw if i.announcement else None,
            }
            for i in batch.items
        ],
        "origins": manager.gate_snapshot(),
    })


@app.get("/api/origins")
async def origins_api():
    return JSONResponse(manager.gate_snapshot())


@app.get("/batches/{batch_id}/csv")
async def batch_csv(batch_id: str):
    batch = manager.get_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="batch not found")
    if batch.status == "pending":
        raise HTTPException(status_code=409, detail="批次尚未开始")
    # UTF-8 BOM keeps Excel on Chinese Windows from mangling the headers.
    payload = "﻿" + batch.to_csv()
    filename = f"announcements_{batch.batch_id}.csv"
    return Response(
        content=payload.encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/batches/{batch_id}/json")
async def batch_json(batch_id: str):
    batch = manager.get_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="batch not found")
    return JSONResponse({"summary": batch.summary(),
                         "items": [i.to_row() for i in batch.items]})


# ---------------------------------------------------------------------------
# Verification catalog (核定目录)
# ---------------------------------------------------------------------------

def verification_sample_text() -> str:
    """Ready-to-paste list for the two-source conflict/verification demo."""
    return "\n".join([
        "# 同一公告编号、两站内容一致：直接可供数",
        f"{SAMPLE_A}/notice/shared/N2026-501",
        f"{SAMPLE_B}/notice/shared/N2026-501",
        "",
        "# 同一公告编号、两站标题不一致：暂停供数，等待核定",
        f"{SAMPLE_A}/notice/conflict-title/N2026-510",
        f"{SAMPLE_B}/notice/conflict-title/N2026-510",
        "",
        "# 同一公告编号、截止时间原文不一致：暂停供数，等待核定",
        f"{SAMPLE_A}/notice/conflict-deadline/N2026-511",
        f"{SAMPLE_B}/notice/conflict-deadline/N2026-511",
        "",
        "# 第二条一致公告：重采不改内容时，已有核定保留",
        f"{SAMPLE_A}/notice/shared/N2026-502",
        f"{SAMPLE_B}/notice/shared/N2026-502",
    ])


@app.get("/catalog", response_class=HTMLResponse)
async def catalog_page(request: Request, error: Optional[str] = None,
                       done: Optional[str] = None):
    snap = await store.snapshot()
    return templates.TemplateResponse(request, "catalog.html", {
        "snap": snap,
        "error": error or "",
        "done": done or "",
        "db_path": CATALOG_DB,
        "sample": verification_sample_text(),
    })


@app.get("/api/catalog")
async def catalog_api():
    return JSONResponse(await store.snapshot())


@app.post("/catalog/{announcement_id}/verify")
async def catalog_verify(announcement_id: str,
                         chosen_url: str = Form(...),
                         version: int = Form(...)):
    try:
        result = await store.verify(announcement_id, chosen_url, version)
    except StaleVersionError as exc:
        # The page was opened before a later committed crawl; never apply a
        # verification against a catalog state the clerk did not look at.
        return RedirectResponse(
            "/catalog?error=" + quote(f"核定被拒绝：{exc}"), status_code=303)
    except UnknownAnnouncementError:
        return RedirectResponse(
            "/catalog?error=" + quote("核定被拒绝：该公告编号已不在目录中"),
            status_code=303)
    except InvalidChoiceError:
        return RedirectResponse(
            "/catalog?error=" + quote("核定被拒绝：所选 URL 不是该编号当前来源"),
            status_code=303)
    return RedirectResponse(
        "/catalog?done=" + quote(
            f"已按来源 {result['chosen_url']} 核定 {announcement_id}"),
        status_code=303)


@app.get("/catalog.csv")
async def catalog_csv():
    # One committed snapshot is serialised under the store lock: concurrent
    # crawls/verifications cannot make a single export mix states.
    payload = await store.catalog_csv()
    return Response(
        content=("﻿" + payload).encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition":
                 'attachment; filename="verification_catalog.csv"'},
    )
