# 网页操作样例（演练手册）

以下步骤假设已执行 `docker compose up --build`，采集台在
<http://localhost:8000>，两个本地公告源分别是
A：<http://localhost:8001>、B：<http://localhost:8002>。

## 0. 先看一眼两个公告源

分别打开 <http://localhost:8001/__admin> 和 <http://localhost:8002/__admin>。
每个控制台列出本源的演练 URL（正常、慢响应、429、跨站跳转、坏版式等），
并显示当前 `slow_ms` / `limit_wait` / `limit_times` 默认值，可直接改。

## 1. 采集台首页

打开 <http://localhost:8000>：

1. 在“粘贴公告 URL 清单”文本框里每行粘一个 URL。空行和 `#` 开头的注释行
   会被忽略；也可以直接把带序号、制表符的行粘进去，系统取行内第一个
   `http(s)` 开头的片段。
2. “单条超时（秒）”可留空（默认 15 秒，等于容器里的 `REQUEST_TIMEOUT`）。
3. 点“开始采集”。页面立刻跳到 `/batches/B0001` 批次详情页。
4. 想先核对清单，点“仅预览 URL”：会列出将被提交的 URL 和被忽略的行，
   确认后再点“确认开始采集”。
5. 不想手打清单时，点“填入演练样例”，文本框会填好覆盖全部行为的样例
   （容器内的 `source-a` / `source-b` 地址）。

**可连续提交多批**：回到首页，再粘一批、再点“开始采集”即可。批次详情页
每 2 秒自动刷新；多个批次同时在跑，互不阻塞，但同一站点的请求仍然按
“每站最多一条在途”串行。

## 2. 看批次详情页

`/batches/B0001` 页面包含：

- 顶部统计：总数 / 成功 / 失败 / 未完成，状态徽标（排队中、采集中、已完成、已中止）。
- 逐条结果表：URL（发生跳转时附最终 URL）、状态、公告编号、标题、
  **投标截止时间原文**、说明栏里的**失败原因**、跳转次数、429 重试次数。
- “各 origin 调度状态”表：每个 (协议, 主机, 端口) 当前在途的是哪条 URL、
  排队多少条、429 暂停还剩几秒（实时刷新）。
- “下载 CSV”链接：批次结束后（或进行中）都可下载，UTF-8 BOM + 全字段加引号，
  Excel 直接打开不乱码。CSV 列：
  `url, status, announcement_id, title, deadline_raw, failure_reason,
  final_url, redirect_hops, retries_429, attempts`。
- JSON：`/batches/B0001/json`（页面数据）与 `/api/batches/B0001`（含
  `failure_kind`、`elapsed_ms`、`origins` 实时状态）。

## 3. 五个演练场景

### 3.1 正常提取 + 同站串行

粘贴：

```
http://source-a:8001/notice/N2026-001
http://source-b:8002/notice/N2026-101
http://source-a:8001/notice/slow/N2026-002?ms=2500
http://source-a:8001/notice/slow/N2026-003?ms=2500
```

预期：
- 四条全部成功，标题/编号/截止时间原样入表。
- A 站三条（正常 + 两条慢）**串行**执行，详情页“各 origin 调度状态”
  里 A 的同时在途数始终是 1；B 站的一条不受 A 的慢响应拖累，很快完成。
- 两个源的 `/__stats` 里 `max_active` 都是 1（服务端从请求开始到
  **响应体发完**都记账，所以这验证了“读完响应体才接下一跳”）。

### 3.2 429 暂停 + 已排队请求等待

```
http://source-a:8001/notice/limit/N2026-003?wait=3&times=1
http://source-a:8001/notice/N2026-004
http://source-b:8002/notice/N2026-104
```

预期：
- `limit/...` 第一次收到 429（`Retry-After: 3`），该条自动重试一次后成功，
  说明栏显示“429 重试 1 次”，耗时约 3 秒。
- 同批 A 站第二条（`N2026-004`）**已经排进 A 站队列**，也必须等这 3 秒暂停
  结束才发出，因此它也在约 3 秒后才开始（详情页 origin 表能看到
  `429 暂停剩余` 在倒数）。
- B 站的那条不受影响，应立即完成。
- 想改暂停秒数：在源 A 控制台把 `limit_wait` 改成别的整数，或直接改 URL
  的 `wait=` 参数。

### 3.3 429 重试耗尽 → 失败原因

```
http://source-b:8002/notice/always-limit/N2026-105?wait=1
```

预期：每条收到 429 后**最多重试两次**（共 3 次请求），仍 429 则标失败，
失败原因写“429 限流，重试 2 次后仍失败（Retry-After=1s）”。
对照：`/notice/date-limit/...` 返回 429 但 `Retry-After` 是 HTTP 日期而非
整数秒，系统不猜测暂停时长（不暂停），同样重试两次后失败。

### 3.4 跨站跳转链（5 跳内成功）

```
http://source-a:8001/notice/chain/0
```

预期：A→B→A→B→A→B 共 5 跳，最后落在 B 的 `/notice/chain/5`，成功；
详情页 URL 下方显示 `→ http://source-b:8002/notice/chain/5`，说明栏“跳转 5 次”。
每一跳都先取得**目标站**的调度许可再发请求，所以跳转过程中两站仍然各只有
一条在途。

### 3.5 两站互跳的无限循环 / 超跳数

```
http://source-a:8001/notice/loop
```

预期：A 与 B 互相 302，系统在**第 5 跳**时停止并标失败，原因
“超过最大重定向跳数（5）”；整个过程约 1 秒内结束，**不会卡死**，两站
`/__stats` 的 `max_active` 仍是 1。这验证了“每跳都遵守目标站限制”和
“两站互跳不卡死”。

### 3.6 超时与版式失败

```
http://source-a:8001/notice/slow/N2026-006?ms=20000
http://source-a:8001/notice/broken/N2026-007
http://source-a:8001/notice/missing-field/N2026-008
http://source-b:8002/notice/404/N2026-109
```

在首页把单条超时设为 3 秒再提交，预期四条全部失败，原因分别是：
`请求超时（>3s）：ReadTimeout`、
`版式解析失败：未找到公告容器（.announcement）`、
`版式解析失败：缺少字段：投标截止时间`、
`HTTP 404：未找到公告 N2026-109`。
超时后该站的调度许可会释放，同批其他 URL 继续正常执行。

## 4. 下载 CSV

批次详情页点“下载 CSV”，得到 `announcements_B0001.csv`。
也可以用 `curl -O -J http://localhost:8000/batches/B0001/csv`。
进行中的批次也能下载当前快照（未完成的行 `status=pending`）。

## 5. 注意

- 批次与结果**只存内存**：重启采集台容器后所有批次清空。
- 采集台只能访问容器网络内的地址；样例里的 `source-a:8001` /
  `source-b:8002` 就是给采集台用的。如果从宿主机手动访问源站点，用
  `localhost:8001` / `localhost:8002`。
- 从宿主机直接跑的等价命令（不使用 Docker）见 README。
