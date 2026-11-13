# 招标公告采集台

FastAPI + httpx 的采集网页。采购数据员把公告 URL 清单粘贴到页面，点“开始采集”，
系统按固定 HTML 版式提取**公告编号、标题、投标截止时间原文**，按批次下载 CSV，
逐条显示失败原因。任务只存内存，可连续提交多批。

## 启动

```bash
docker compose up --build
# 采集台        http://localhost:8000
# 本地公告源 A  http://localhost:8001/__admin
# 本地公告源 B  http://localhost:8002/__admin
```

网页操作样例（含每个演练场景的预期结果）见 [docs/WALKTHROUGH.md](docs/WALKTHROUGH.md)。

不用 Docker 时：

```bash
pip install -r requirements.txt
PORTAL=A PORT=8001 SELF_BASE=http://127.0.0.1:8001 PEER_BASE=http://127.0.0.1:8002 \
  python -m uvicorn sources.mock_source:app --port 8001 &
PORTAL=B PORT=8002 SELF_BASE=http://127.0.0.1:8002 PEER_BASE=http://127.0.0.1:8001 \
  python -m uvicorn sources.mock_source:app --port 8002 &
python -m uvicorn app.main:app --port 8000
```

## 固定的公告版式

两个本地源使用同一版式（可以换成真实的固定模板，只改 `app/extract.py`）：

```html
<div class="announcement" data-announcement-id="ZB-2026-N2026-001">
  <h1 class="title">…标题…</h1>
  <dl class="meta">
    <dt>公告编号</dt><dd>ZB-2026-N2026-001</dd>
    <dt>投标截止时间</dt><dd>2026-12-01 09:30:00</dd>
  </dl>
</div>
```

三条字段缺一即判“版式解析失败”，原因写明缺哪个字段。截止时间**按原文保存**，
不做时区/格式转换。

## 调度与失败规则

| 规则 | 实现 |
| --- | --- |
| 站点标识 | `origin = (协议, 主机, 端口)`，默认端口归一化（`http://h` == `http://h:80`） |
| 每站串行 | 每个 origin 一个 FIFO gate；**响应体读完/连接关闭后才释放**，才允许下一条 |
| 多批次共享 | 所有批次共用一个 `CrawlManager` 的 gate 池，不按批次隔离 |
| 429 | 读整数秒 `Retry-After` 后暂停该 origin，**已排队请求也等待**；HTTP 日期或缺失则按“无整数”处理、不暂停 |
| 429 重试 | 每条每跳最多 2 次重试（共 3 次请求），仍 429 → 失败 |
| 跳转 | `follow_redirects=False` 手动逐跳；每跳先取得**目标站**的 gate，不持有站点 A 的同时等站点 B → 两站互跳不死锁 |
| 跳数上限 | 每条最多 5 跳，超过 → 失败 |
| 超时 | 每条可单独设超时（默认 15s）；超时/网络错误 → 失败，且已释放 gate |
| 结果 | 成功/失败逐条记录；失败含 `failure_kind` 与中文原因 |

`GET /api/origins` 与批次页的“各 origin 调度状态”可实时看到每个站点的在途 URL、
排队数、429 暂停剩余秒数。

## 产出

- `GET /batches/{id}/csv`：按批下载，UTF-8 BOM，全字段加引号，列为
  `url, status, announcement_id, title, deadline_raw, failure_reason, final_url, redirect_hops, retries_429, attempts`
- `GET /batches/{id}/json`、`GET /api/batches/{id}`：程序可读结果
- 首页列出全部批次；失败原因在详情页逐条显示

## 本地演练源

两个源（`sources/mock_source.py`）都能制造：

- 慢响应：`/notice/slow/{code}?ms=`（延迟响应头）与
  `/notice/slowbody/{code}?ms=&chunks=`（先回头、再逐块吐响应体）
- 429：`/notice/limit/{code}?wait=&times=`（前 N 次限流后放行）、
  `/notice/always-limit/{code}`、`/notice/date-limit/{code}`（非整数 Retry-After）
- 跨站跳转：`/notice/chain/{n}`（A↔B，第 5 跳落地）、`/notice/loop`（无限互跳）、
  `/notice/hop/{n}`（永不落地）
- 失败版式：`/notice/broken/{code}`、`/notice/missing-field/{code}`、`/notice/404/{code}`

每个源还有 `/__admin`（可点示例与参数表单）、`/__control`、`/__stats`（含
`active`/`max_active`，从请求开始计到响应体发完，用来验证“每站一条在途”）、
`POST /__reset`。

## 测试

```bash
python -m pytest tests -q
```

测试会临时启动两个真实 mock 源，验证：提取、同站串行（服务端 `max_active == 1`）、
跨站并发、429 暂停（含已排队请求确实等到暂停结束）、重试耗尽、非整数
Retry-After、跨站 5 跳成功、无限互跳 5 跳失败且不卡死、超时释放 gate、
多批次并发与 CSV。
