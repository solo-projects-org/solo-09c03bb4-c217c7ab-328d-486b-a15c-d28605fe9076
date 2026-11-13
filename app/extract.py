"""HTML extraction for the fixed announcement layout.

The two bundled sources deliberately use the *same* markup contract, which
mirrors the fixed intranet layout the data-entry clerk works with:

    <div class="announcement" data-announcement-id="ZB-2026-...">
      <h1 class="title">…</h1>
      <dl class="meta">
        <dt>公告编号</dt><dd>…</dd>
        <dt>投标截止时间</dt><dd>2026-12-01 09:30:00</dd>
      </dl>
    </div>
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from bs4 import BeautifulSoup


@dataclass
class Announcement:
    announcement_id: str
    title: str
    deadline_raw: str


class ExtractionError(Exception):
    """Raised when a page does not match the fixed layout."""


def parse_announcement(html: str) -> Announcement:
    soup = BeautifulSoup(html, "html.parser")

    node = soup.select_one(".announcement")
    if node is None:
        raise ExtractionError("未找到公告容器（.announcement）")

    # Identifier: prefer the machine attribute, fall back to the <dd> row.
    announcement_id = (node.get("data-announcement-id") or "").strip()
    title_node = node.select_one("h1.title")
    if title_node is None:
        raise ExtractionError("未找到标题（h1.title）")
    title = title_node.get_text(strip=True)

    fields = {}
    for dt in node.select("dl.meta dt"):
        dd = dt.find_next_sibling("dd")
        if dd is not None:
            fields[dt.get_text(strip=True)] = dd.get_text(strip=True)

    if not announcement_id:
        announcement_id = fields.get("公告编号", "")
    deadline_raw = fields.get("投标截止时间", "")

    missing = []
    if not announcement_id:
        missing.append("公告编号")
    if not title:
        missing.append("标题")
    if not deadline_raw:
        missing.append("投标截止时间")
    if missing:
        raise ExtractionError("缺少字段：" + "、".join(missing))

    return Announcement(announcement_id, title, deadline_raw)


def extract_all(html: str) -> List[Announcement]:
    """Parse all announcements in a page (kept for index-style pages)."""
    soup = BeautifulSoup(html, "html.parser")
    nodes = soup.select(".announcement")
    if not nodes:
        raise ExtractionError("未找到公告容器（.announcement）")
    out = []
    for node in nodes:
        # Wrap each node so the single-announcement parser applies.
        wrapper = BeautifulSoup(str(node), "html.parser")
        out.append(parse_announcement(str(wrapper)))
    return out
