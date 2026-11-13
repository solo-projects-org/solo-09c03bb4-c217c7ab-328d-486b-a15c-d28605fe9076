"""Two deliberately rude local announcement sources used for rehearsals.

Both portals serve the *same* fixed HTML layout.  Each one lets the operator
rehearse slow responses, 429 throttling and cross-site redirects.

Portal A (port 8001) routes
    /notice/{code}             normal page
    /notice/slow/{code}?ms=    sleeps ``ms`` (default 2500) before headers
    /notice/slowbody/{code}    sends headers, then dribbles the body
    /notice/limit/{code}       first ``times`` hits -> 429 + Retry-After ``wait``
                               (defaults come from /__control), then the page
    /notice/always-limit/{code} always 429 (retries get exhausted)
    /notice/date-limit/{code}  429 with an HTTP-date Retry-After (not integer)
    /notice/chain/{n}          cross-site redirect chain A<->B; n==5 serves a
                               page, so a start at 0 succeeds after 5 hops
    /notice/loop               A->B->A->B... forever (redirect cap fires)
    /notice/hop/{n}            cross-site chain that never settles
    /notice/broken/{code}      200 but layout mismatch -> parse failure
    /notice/missing-field/{c}  layout without the deadline field
    /notice/404/{code}         404

Portal B (port 8002) serves the same routes (the redirect helpers point at the
peer), so A and B bounce requests at each other.

Control endpoints (both portals)
    GET  /__admin              operator page with clickable examples
    GET  /__control            ?slow_ms=&limit_wait=&limit_times=
    GET  /__stats              counters: hits per code, active requests,
                               max concurrency seen, request log summary
    POST /__reset              clears 429 counters and stats
"""

from __future__ import annotations

import argparse
import asyncio
import html
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional, Tuple

import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, StreamingResponse

PORTAL = os.environ.get("PORTAL", "A")
PORT = int(os.environ.get("PORT", "8001"))
SELF_BASE = os.environ.get("SELF_BASE", f"http://127.0.0.1:{PORT}")
PEER_BASE = os.environ.get("PEER_BASE", "http://127.0.0.1:8002")
NAME = os.environ.get("PORTAL_NAME", f"招标公告源 {PORTAL}")
CHAIN_LANDING = 5

app = FastAPI(title=f"{NAME} (mock source)")
app.state.portal = PORTAL


# --------------------------------------------------------------------------
# Fixed layout
# --------------------------------------------------------------------------

def page(announcement_id: str, title: str, deadline: str,
         body: str = "", omit_deadline: bool = False) -> str:
    deadline_dd = "" if omit_deadline else (
        f"<dt>投标截止时间</dt><dd>{html.escape(deadline)}</dd>")
    return f"""<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>{html.escape(title)}</title></head>
<body>
<div class="announcement" data-announcement-id="{html.escape(announcement_id)}">
  <h1 class="title">{html.escape(title)}</h1>
  <dl class="meta">
    <dt>公告编号</dt><dd>{html.escape(announcement_id)}</dd>
    {deadline_dd}
  </dl>
  <div class="content">{body}</div>
</div>
</body>
</html>
"""


def notice_page(code: str, title: str, deadline: str) -> str:
    return page(f"ZB-2026-{code}", title, deadline,
                body=f"<p>本地演练公告 {html.escape(code)} 的正文。</p>")


# --------------------------------------------------------------------------
# Server-side statistics: accurate "request in flight" tracking
# --------------------------------------------------------------------------

@dataclass
class Stats:
    active: int = 0
    max_active: int = 0
    total: int = 0
    limit_hits: Dict[str, int] = field(default_factory=dict)
    requests: List[dict] = field(default_factory=list)
    max_requests_log: int = 200

    def record(self, path: str, started: float) -> None:
        if len(self.requests) >= self.max_requests_log:
            return
        self.requests.append({"path": path,
                              "start": round(started, 4)})


stats = Stats()


class ConcurrencyTracker:
    """Raw ASGI middleware: count from request start until body fully sent."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        stats.active += 1
        stats.total += 1
        stats.max_active = max(stats.max_active, stats.active)
        stats.record(scope.get("path", ""), time.monotonic())
        completed = False

        async def wrapped_send(message):
            nonlocal completed
            if (message["type"] == "http.response.body"
                    and not message.get("more_body", False)
                    and not completed):
                completed = True
                stats.active = max(0, stats.active - 1)
            await send(message)

        try:
            await self.app(scope, receive, wrapped_send)
        finally:
            if not completed:
                completed = True
                stats.active = max(0, stats.active - 1)


app.add_middleware(ConcurrencyTracker)


# --------------------------------------------------------------------------
# 429 bookkeeping
# --------------------------------------------------------------------------

@dataclass
class Controls:
    slow_ms: int = 2500
    limit_wait: int = 2
    limit_times: int = 1


controls = Controls()
limit_hits: Dict[str, int] = {}

# Editable per-portal announcement overrides used by the verification demo.
# Key: shared announcement code (both portals expose the same code with the
# same announcement id, so re-crawling either portal lands in the same catalog
# group).  Value: (title, deadline).
editable: Dict[str, Tuple[str, str]] = {}


def take_limit(code: str, times: int) -> bool:
    """Return True when this hit should be throttled."""
    if times <= 0:
        return False
    n = limit_hits.get(code, 0) + 1
    limit_hits[code] = n
    stats.limit_hits[code] = n
    return n <= times


def rate_limited(code: str, wait: int, times: int) -> PlainTextResponse:
    resp = PlainTextResponse(
        f"429 Too Many Requests: {code}，请在 {wait} 秒后重试\n",
        status_code=429,
    )
    resp.headers["Retry-After"] = str(int(wait))
    return resp


@app.get("/__admin", response_class=HTMLResponse)
async def admin():
    base = SELF_BASE
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>{NAME} 演练控制台</title></head><body>
<h1>{NAME}（{base}）</h1>
<p>对方站：{PEER_BASE}；我的角色：{PORTAL}</p>
<h2>可用于粘贴的公告 URL</h2>
<ul>
<li><a href="/notice/N2026-001">正常公告</a></li>
<li><a href="/notice/slow/N2026-002?ms={controls.slow_ms}">慢响应（{controls.slow_ms}ms，会超过 15s 超时？看设置）</a></li>
<li><a href="/notice/slowbody/N2026-002b?ms=20&chunks=5">慢响应体（逐块发送）</a></li>
<li><a href="/notice/limit/N2026-003?wait={controls.limit_wait}&times={controls.limit_times}">
    429（Retry-After={controls.limit_wait}s，前 {controls.limit_times} 次）</a></li>
<li><a href="/notice/always-limit/N2026-003b?wait=1">持续 429（重试耗尽后失败）</a></li>
<li><a href="/notice/date-limit/N2026-003c">429 但 Retry-After 是 HTTP 日期（非整数秒，不等）</a></li>
<li><a href="/notice/chain/0">跨站跳转链 A↔B（5 跳后成功）</a></li>
<li><a href="/notice/hop/0">跨站跳转链 A↔B（永不落地，触发 5 跳上限）</a></li>
<li><a href="/notice/loop">跨站无限循环 A→B→A…（两站互跳，不应卡死）</a></li>
<li><a href="/notice/broken/N2026-004">版式不匹配（解析失败）</a></li>
<li><a href="/notice/missing-field/N2026-005">缺少投标截止时间</a></li>
<li><a href="/notice/404/N2026-006">404</a></li>
</ul>
<h2>核定目录演练（两站同编号）</h2>
<ul>
<li><a href="/notice/shared/N2026-501">一致公告 N2026-501（两站默认同标题同截止）</a></li>
<li><a href="/notice/conflict-title/N2026-510">标题冲突 N2026-510（同编号、同截止、各站标题不同）</a></li>
<li><a href="/notice/conflict-deadline/N2026-511">截止时间冲突 N2026-511（同编号、同标题、截止原文不同）</a></li>
</ul>
<p class="hint" style="font-size:13px;color:#555">在线修改本端口的共享公告（制造/修复冲突，演示重采后核定失效）：<br>
<code>GET /__edit?code=N2026-501&amp;title=新标题&amp;deadline=2026-12-20 09:30:00</code>；
查看当前覆盖：<a href="/__edit">/__edit</a>；<code>POST /__reset</code> 清空覆盖。</p>
<h2>默认参数</h2>
<form method="get" action="/__control">
慢响应毫秒 <input name="slow_ms" value="{controls.slow_ms}" size="6">
429 等待秒 <input name="limit_wait" value="{controls.limit_wait}" size="4">
429 次数 <input name="limit_times" value="{controls.limit_times}" size="4">
<input type="submit" value="保存">
</form>
<p>当前统计：<a href="/__stats">/__stats</a>；清空：POST /__reset。</p>
</body></html>"""


@app.get("/__control")
async def control(slow_ms: Optional[int] = None,
                  limit_wait: Optional[int] = None,
                  limit_times: Optional[int] = None):
    if slow_ms is not None:
        controls.slow_ms = max(0, slow_ms)
    if limit_wait is not None:
        controls.limit_wait = max(0, limit_wait)
    if limit_times is not None:
        controls.limit_times = max(0, limit_times)
    return JSONResponse(asdict(controls))


@app.get("/__stats")
async def stats_endpoint():
    return JSONResponse({
        "name": NAME,
        "portal": PORTAL,
        "self": SELF_BASE,
        "peer": PEER_BASE,
        "active": stats.active,
        "max_active": stats.max_active,
        "total": stats.total,
        "controls": asdict(controls),
        "limit_hits": limit_hits,
        "requests": stats.requests[-50:],
    })


@app.post("/__reset")
async def reset():
    limit_hits.clear()
    stats.limit_hits.clear()
    stats.active = 0
    stats.max_active = 0
    stats.total = 0
    stats.requests.clear()
    editable.clear()
    return JSONResponse({"reset": True})


@app.get("/healthz")
async def healthz():
    return {"ok": True, "portal": PORTAL, "self": SELF_BASE}


@app.get("/", response_class=HTMLResponse)
async def index():
    return (f'<html><body><h1>{NAME}</h1>'
            f'<p><a href="/__admin">演练控制台</a></p></body></html>')


# --------------------------------------------------------------------------
# Notice routes
# --------------------------------------------------------------------------

# Two-segment routes must be registered before "/notice/{code}",
# otherwise the catch-all wins for paths like "/notice/loop".

@app.get("/notice/loop")
async def notice_loop():
    """Infinite cross-site loop. The client's 5-hop budget must stop it."""
    return RedirectResponse(f"{PEER_BASE}/notice/loop", status_code=302)


# --------------------------------------------------------------------------
# Verification-catalog demo pages
#
# The same *code* on both portals renders the same announcement id, so the
# catalog groups the two submissions together.  Default content is identical
# (consistent); portal-specific routes deliberately disagree, and the shared
# page can be edited at runtime on one portal only to create or repair a
# conflict without restarting anything.
# --------------------------------------------------------------------------

SHARED_DEFAULTS: Dict[str, Tuple[str, str]] = {
    "N2026-501": ("两站一致公告 N2026-501", "2026-12-10 09:30:00"),
    "N2026-502": ("两站一致公告 N2026-502", "2026-12-11 14:00:00"),
}


def _shared_content(code: str) -> Tuple[str, str]:
    if code in editable:
        return editable[code]
    return SHARED_DEFAULTS.get(
        code, (f"两站一致公告 {code}", "2026-12-10 09:30:00"))


@app.get("/notice/shared/{code}")
async def notice_shared(code: str):
    """Same announcement id and identical default text on both portals."""
    title, deadline = _shared_content(code)
    return HTMLResponse(notice_page(code, title, deadline))


@app.get("/notice/conflict-title/{code}")
async def notice_conflict_title(code: str):
    """Same announcement id and deadline, portal-specific title."""
    return HTMLResponse(notice_page(
        code, f"{NAME}转载版标题 {code}", "2026-12-12 10:00:00"))


@app.get("/notice/conflict-deadline/{code}")
async def notice_conflict_deadline(code: str):
    """Same announcement id and title, portal-specific deadline raw text."""
    deadline = ("2026-12-13 09:30:00" if PORTAL == "A"
                else "2026年12月13日 上午9时30分")
    return HTMLResponse(notice_page(
        code, f"截止时间有出入的公告 {code}", deadline))


@app.api_route("/__edit", methods=["GET", "POST"])
async def edit_notice(code: Optional[str] = None,
                      title: Optional[str] = None,
                      deadline: Optional[str] = None):
    """Read or change this portal's copy of a shared notice."""
    if code is not None and (title is not None or deadline is not None):
        old_title, old_deadline = _shared_content(code)
        editable[code] = (title if title is not None else old_title,
                          deadline if deadline is not None else old_deadline)
    current = {c: {"title": t, "deadline": d}
               for c, (t, d) in editable.items()}
    return JSONResponse({"portal": PORTAL, "editable": current})


@app.get("/notice/{code}")
async def notice(code: str):
    return HTMLResponse(notice_page(code, f"{NAME}公告 {code}",
                                    "2026-12-01 09:30:00"))


@app.get("/notice/slow/{code}")
async def notice_slow(code: str, ms: Optional[int] = Query(None)):
    await asyncio.sleep(max(0, controls.slow_ms if ms is None else ms) / 1000)
    return HTMLResponse(notice_page(code, f"{NAME}慢响应公告 {code}",
                                    "2026-12-02 10:00:00"))


@app.get("/notice/slowbody/{code}")
async def notice_slowbody(code: str, ms: int = Query(20),
                          chunks: int = Query(5)):
    """Send headers immediately, then trickle the body.

    Useful to prove the client holds the origin gate until the body is fully
    read: the concurrency counter stays at 1 while the body dribbles.
    """
    delay = max(0, ms) / 1000.0
    payload = notice_page(code, f"{NAME}慢响应体公告 {code}",
                          "2026-12-03 11:00:00")
    chunk_size = max(1, len(payload) // max(1, chunks))

    async def gen():
        for i in range(0, len(payload), chunk_size):
            await asyncio.sleep(delay)
            yield payload[i:i + chunk_size]

    return StreamingResponse(gen(), media_type="text/html; charset=utf-8")


@app.get("/notice/limit/{code}")
async def notice_limit(code: str, wait: Optional[int] = Query(None),
                       times: Optional[int] = Query(None)):
    w = controls.limit_wait if wait is None else wait
    t = controls.limit_times if times is None else times
    if take_limit(code, t):
        return rate_limited(code, w, t)
    return HTMLResponse(notice_page(code, f"{NAME}限流后公告 {code}",
                                    "2026-12-04 12:00:00"))


@app.get("/notice/always-limit/{code}")
async def notice_always_limit(code: str, wait: int = Query(1)):
    return rate_limited(code, wait, 1)


@app.get("/notice/date-limit/{code}")
async def notice_date_limit(code: str):
    resp = PlainTextResponse("429，Retry-After 为 HTTP 日期\n", status_code=429)
    resp.headers["Retry-After"] = "Wed, 21 Oct 2026 07:28:00 GMT"
    return resp


@app.get("/notice/chain/{n}")
async def notice_chain(n: int):
    """A<->B chain; n == CHAIN_LANDING serves the page."""
    if n >= CHAIN_LANDING:
        return HTMLResponse(notice_page(f"N2026-CHAIN-{n}",
                                        f"{NAME}跳转后公告",
                                        "2026-12-05 13:00:00"))
    return RedirectResponse(f"{PEER_BASE}/notice/chain/{n + 1}", status_code=302)


@app.get("/notice/hop/{n}")
async def notice_hop(n: int):
    """Never lands: n grows past the client's redirect budget."""
    if n > 50:
        return HTMLResponse(notice_page(f"N2026-HOP-{n}", "不该到达",
                                        "2026-12-06 14:00:00"))
    return RedirectResponse(f"{PEER_BASE}/notice/hop/{n + 1}", status_code=302)


@app.get("/notice/broken/{code}")
async def notice_broken(code: str):
    return HTMLResponse(f"<!doctype html><html><body><p>公告 {code} "
                        f"（版式已改版，没有 .announcement 容器）</p></body></html>")


@app.get("/notice/missing-field/{code}")
async def notice_missing_field(code: str):
    return HTMLResponse(page(f"ZB-2026-{code}", f"{NAME}缺字段公告 {code}",
                             "", omit_deadline=True))


@app.get("/notice/404/{code}")
async def notice_404(code: str):
    return PlainTextResponse(f"未找到公告 {code}\n", status_code=404)


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def main(argv=None):
    global PORTAL, PORT, SELF_BASE, PEER_BASE, NAME
    parser = argparse.ArgumentParser()
    parser.add_argument("--portal", default=PORTAL)
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--self-base", default=SELF_BASE)
    parser.add_argument("--peer-base", default=PEER_BASE)
    parser.add_argument("--name", default=NAME)
    args, _ = parser.parse_known_args(argv)
    PORTAL = args.portal.upper()
    PORT = args.port
    SELF_BASE = args.self_base
    PEER_BASE = args.peer_base
    NAME = args.name
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")


if __name__ == "__main__":
    main()
