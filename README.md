# 招标公告采集台

FastAPI + httpx 的采集网页。采购数据员把公告 URL 清单粘贴到页面，点“开始采集”，
系统按固定 HTML 版式提取**公告编号、标题、投标截止时间原文**，按批次下载 CSV，
逐条显示失败原因。同一公告被多站转载时，新增的**核定目录**按公告编号汇合各来源，
冲突时暂停供数、由数据员在网页核定。批次任务只存内存；核定目录与核定结果存数据卷内
SQLite，重启后仍有效。可连续提交多批。

## 启动

```bash
docker compose up --build
# 采集台        http://localhost:8000
# 核定目录      http://localhost:8000/catalog
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
CATALOG_DB=./data/catalog.db \
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

## 核定目录（多站转载的供数闸门）

每条**采集成功**的结果按提交 URL 写入数据卷内 SQLite（`CATALOG_DB`，默认
`/data/catalog.db`），并按公告编号在 `/catalog` 汇合。规则：

| 规则 | 实现 |
| --- | --- |
| 每 URL 取最后成功批次 | 持久化单调批次序号 `batch_seq`；序号更小的迟到旧批不能覆盖同 URL 已有值；失败采集不入库，因此不会撤掉已有值 |
| 一致即可供数 | 同编号所有来源的标题、截止时间原文完全一致 → `来源一致`，进入目录 CSV |
| 冲突暂停供数 | 任一来源标题或截止原文不同 → `来源冲突 · 暂停供数`，列出每个来源的原文和提交 URL，数据员选一份核定 |
| 核定 | 核定表单带页面版本号；版本匹配才写入，选定来源的标题/截止原文成为供数值，状态变 `已核定` |
| 变化撤销核定 | 来源集合变化或任一标题/截止原文变化 → 指纹改变，版本号 +1 并删除原核定；**原样重采**指纹不变，版本不动、核定保留 |
| 拒绝旧页面提交 | 打开页面后来源曾变化，旧表单的版本号即过期，提交一律 303 回目录并提示；即使内容后来恢复原样（版本号只增不减），旧版本号仍被拒绝 |
| 目录 CSV | `GET /catalog.csv`：UTF-8 BOM、全字段加引号，**每个公告编号一行，只含一致或已核定项**；导出在同一把锁内取一个已提交快照，并发采集/核定/下载不会让单次导出混入两种状态 |
| 事务与持久化 | 所有写操作单条 `BEGIN IMMEDIATE` 事务，任一失败整次回滚；数据库在挂载卷内，容器重启后目录、核定、批次序号仍有效 |

- `GET /catalog`：核定目录页（冲突项可直接选来源核定，可勾选 5 秒自动刷新）
- `GET /api/catalog`：目录 JSON 快照
- `GET /catalog.csv`：目录 CSV
- `POST /catalog/{公告编号}/verify`：表单字段 `chosen_url`、`version`

原有批次采集与 `GET /batches/{id}/csv` 批次 CSV 完全保留，两套产出并存。

### 核定演练源（两个本地源各一份同编号页面）

- `/notice/shared/{code}`：两站同编号、默认同标题同截止（一致）
- `/notice/conflict-title/{code}`：同编号同截止、各站标题不同（标题冲突）
- `/notice/conflict-deadline/{code}`：同编号同标题、截止原文不同（截止冲突）
- `GET /__edit?code=&title=&deadline=`：只改**本站**的 shared 公告内容，
  用来在线制造/修复冲突，演示“重采后核定失效”；`POST /__reset` 清空覆盖

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
- 核定演练：`/notice/shared/{code}`（一致）、`/notice/conflict-title/{code}`、
  `/notice/conflict-deadline/{code}`，以及 `GET /__edit` 在线改本站公告内容

每个源还有 `/__admin`（可点示例与参数表单）、`/__control`、`/__stats`（含
`active`/`max_active`，从请求开始计到响应体发完，用来验证“每站一条在途”）、
`POST /__reset`。

## 数据持久化

- 批次与逐条采集结果只存采集台进程内存，重启清空（与原行为一致，批次 CSV 仍可下载）。
- 核定目录（每 URL 最后成功批次、每编号版本/指纹、人工核定、批次序号）存
  `CATALOG_DB` 指向的 SQLite；compose 用命名卷 `catalog-data` 挂到 `/data`，
  `docker compose restart` 或重建容器后目录与核定仍在。

## 测试

```bash
python -m pytest tests -q
```

测试会临时启动两个真实 mock 源，验证：提取、同站串行（服务端 `max_active == 1`）、
跨站并发、429 暂停（含已排队请求确实等到暂停结束）、重试耗尽、非整数
Retry-After、跨站 5 跳成功、无限互跳 5 跳失败且不卡死、超时释放 gate、
多批次并发与 CSV；以及核定目录的全部规则：最后批次胜出/旧批不覆盖/失败不撤值、
一致供数与冲突暂停、核定、来源或内容变化撤销核定而原样重采保留、旧页面版本号
拒绝（含恢复原样后仍拒绝）、写入失败整次回滚、重启持久化、目录 CSV 只含可供数项、
并发采集/核定/导出的快照一致性。
