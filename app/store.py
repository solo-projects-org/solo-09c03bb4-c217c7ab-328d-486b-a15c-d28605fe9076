"""SQLite-backed verification catalog (核定目录).

For every successfully crawled URL the catalog keeps *one* row: the result of
the latest submitted batch for that URL.  Rows are grouped by announcement id
and drive the supply decision:

* all sources agree on title and deadline raw text  -> ``consistent`` (可供数)
* sources disagree                                  -> ``conflict``   (暂停供数)
* a clerk has picked one source on the catalog page -> ``verified``   (可供数)

Guarantees implemented here:

* Latest batch wins per URL.  A late, older batch never overwrites a value a
  newer batch already committed; a failed crawl never deletes an existing
  value (failures are simply not ingested).
* A content fingerprint per announcement id detects any change of the source
  set or of the title/deadline text.  Any real change bumps a monotonic
  version and revokes the existing verification.  Re-crawling identical
  content changes nothing, so the verification survives.
* Verification forms carry the version they were rendered with.  A submit
  whose version is stale is rejected, even if the content was changed and
  later restored (each change bumps the version again).
* Every write runs in one ``BEGIN IMMEDIATE`` transaction; any error rolls
  the whole write back.  Reads (including the CSV export) take a single
  snapshot so concurrent crawls/verifications/exports never mix states.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from starlette.concurrency import run_in_threadpool

# Catalog supply states.
CONSISTENT = "consistent"
CONFLICT = "conflict"
VERIFIED = "verified"

STATE_LABELS = {
    CONSISTENT: "来源一致",
    CONFLICT: "冲突暂停",
    VERIFIED: "已核定",
}

CSV_FIELDS = [
    "announcement_id", "state", "title", "deadline_raw",
    "chosen_url", "source_urls", "verified_at",
]


class StaleVersionError(Exception):
    """The catalog page was rendered before a later committed change."""


class UnknownAnnouncementError(Exception):
    """The announcement id no longer exists in the catalog."""


class InvalidChoiceError(Exception):
    """The chosen URL is not one of the current sources of the announcement."""


@dataclass
class CatalogStore:
    """Thread-safe SQLite wrapper for the verification catalog."""

    db_path: str
    # A process-wide re-entrant lock makes the single sqlite connection safe
    # across the worker threads that run_in_threadpool dispatches to, and
    # serialises whole transactions (no write can interleave another).
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False)
    _conn: Optional[sqlite3.Connection] = field(default=None, init=False)
    _closed: bool = field(default=False, init=False)

    # -- lifecycle ---------------------------------------------------------

    def open(self) -> "CatalogStore":
        with self._lock:
            if self._conn is not None:
                return self
            parent = os.path.dirname(os.path.abspath(self.db_path))
            os.makedirs(parent, exist_ok=True)
            conn = sqlite3.connect(
                self.db_path, check_same_thread=False, isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            self._create_schema(conn)
            self._conn = conn
            self._closed = False
            return self

    def close(self) -> None:
        with self._lock:
            if self._conn is None:
                self._closed = True
                return
            self._conn.close()
            self._conn = None
            self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    @staticmethod
    def _create_schema(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            -- One row per submitted URL: the latest successful batch result.
            CREATE TABLE IF NOT EXISTS crawl_results (
                submitted_url    TEXT PRIMARY KEY,
                announcement_id  TEXT NOT NULL,
                title            TEXT NOT NULL,
                deadline_raw     TEXT NOT NULL,
                final_url        TEXT,
                batch_id         TEXT NOT NULL,
                batch_seq        INTEGER NOT NULL,
                fetched_at       REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_crawl_announcement
                ON crawl_results(announcement_id);

            -- Per-announcement monotonic version + content fingerprint.
            CREATE TABLE IF NOT EXISTS id_state (
                announcement_id TEXT PRIMARY KEY,
                version         INTEGER NOT NULL,
                fingerprint     TEXT NOT NULL
            );

            -- Clerk verification, one row per announcement id.
            CREATE TABLE IF NOT EXISTS verifications (
                announcement_id TEXT PRIMARY KEY,
                chosen_url      TEXT NOT NULL,
                chosen_title    TEXT NOT NULL,
                chosen_deadline TEXT NOT NULL,
                fingerprint     TEXT NOT NULL,
                version         INTEGER NOT NULL,
                created_at      REAL NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('batch_seq', '0')")

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            # Lazy open: route handlers work even without lifespan startup.
            self.open()
        assert self._conn is not None
        return self._conn

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _fingerprint(rows: List[sqlite3.Row]) -> str:
        """Hash the full source set: URLs plus title/deadline raw text."""
        payload = sorted(
            (r["submitted_url"], r["title"], r["deadline_raw"]) for r in rows)
        blob = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def _recompute(self, conn: sqlite3.Connection, announcement_id: str) -> None:
        """Refresh fingerprint/version for one id and revoke stale verifies.

        Called inside an open write transaction.  The version bumps whenever
        the fingerprint changes; the verification is revoked when its stored
        fingerprint no longer matches the current source content.
        """
        rows = conn.execute(
            "SELECT * FROM crawl_results WHERE announcement_id = ?",
            (announcement_id,)).fetchall()
        if not rows:
            # The last source for this id disappeared (its URL now returns a
            # different announcement): drop the whole id.
            conn.execute("DELETE FROM id_state WHERE announcement_id = ?",
                         (announcement_id,))
            conn.execute("DELETE FROM verifications WHERE announcement_id = ?",
                         (announcement_id,))
            return

        fp = self._fingerprint(rows)
        state = conn.execute(
            "SELECT * FROM id_state WHERE announcement_id = ?",
            (announcement_id,)).fetchone()
        if state is None:
            conn.execute(
                "INSERT INTO id_state(announcement_id, version, fingerprint)"
                " VALUES (?, 1, ?)", (announcement_id, fp))
        elif state["fingerprint"] != fp:
            conn.execute(
                "UPDATE id_state SET version = version + 1, fingerprint = ?"
                " WHERE announcement_id = ?", (fp, announcement_id))

        verify = conn.execute(
            "SELECT fingerprint FROM verifications WHERE announcement_id = ?",
            (announcement_id,)).fetchone()
        if verify is not None and verify["fingerprint"] != fp:
            # Source set or content changed after the verification: revoke it.
            conn.execute("DELETE FROM verifications WHERE announcement_id = ?",
                         (announcement_id,))

    # -- batch sequence (persisted across restarts) ------------------------

    def next_batch_seq_sync(self) -> int:
        with self._lock:
            conn = self._connect()
            conn.execute("BEGIN IMMEDIATE")
            try:
                seq = int(conn.execute(
                    "SELECT value FROM meta WHERE key = 'batch_seq'"
                ).fetchone()["value"]) + 1
                conn.execute(
                    "UPDATE meta SET value = ? WHERE key = 'batch_seq'",
                    (str(seq),))
                conn.execute("COMMIT")
                return seq
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def next_batch_seq(self) -> int:
        return await run_in_threadpool(self.next_batch_seq_sync)

    # -- ingestion ---------------------------------------------------------

    def ingest_success_sync(self, *, batch_id: str, batch_seq: int,
                            submitted_url: str, announcement_id: str,
                            title: str, deadline_raw: str,
                            final_url: Optional[str],
                            fetched_at: Optional[float] = None) -> bool:
        """Commit one successful crawl item.

        Returns True when catalog rows actually changed, False when the write
        was skipped (late older batch, or identical re-crawl).  The whole
        update is one transaction and rolls back on any error.
        """
        fetched_at = time.time() if fetched_at is None else fetched_at
        with self._lock:
            conn = self._connect()
            conn.execute("BEGIN IMMEDIATE")
            try:
                existing = conn.execute(
                    "SELECT * FROM crawl_results WHERE submitted_url = ?",
                    (submitted_url,)).fetchone()
                if existing is not None:
                    # Latest submitted batch wins per URL.
                    if existing["batch_seq"] >= batch_seq:
                        conn.execute("ROLLBACK")
                        return False
                    # Identical content from a newer batch is an as-is
                    # re-crawl: keep every existing value, version and verify.
                    same_content = (
                        existing["announcement_id"] == announcement_id
                        and existing["title"] == title
                        and existing["deadline_raw"] == deadline_raw
                        and (existing["final_url"] or "") == (final_url or ""))
                    if same_content:
                        conn.execute("ROLLBACK")
                        return False
                    affected = {existing["announcement_id"], announcement_id}
                    conn.execute(
                        "UPDATE crawl_results SET announcement_id = ?, title = ?,"
                        " deadline_raw = ?, final_url = ?, batch_id = ?,"
                        " batch_seq = ?, fetched_at = ? WHERE submitted_url = ?",
                        (announcement_id, title, deadline_raw, final_url,
                         batch_id, batch_seq, fetched_at, submitted_url))
                else:
                    # New URL.  If another URL already carries the exact same
                    # announcement content, this is an as-is re-crawl from a
                    # duplicate entry: nothing about the id changes, so skip
                    # version/verification bookkeeping entirely.
                    twin = conn.execute(
                        "SELECT 1 FROM crawl_results WHERE announcement_id = ?"
                        " AND title = ? AND deadline_raw = ?"
                        " AND IFNULL(final_url, '') = IFNULL(?, '') LIMIT 1",
                        (announcement_id, title, deadline_raw,
                         final_url)).fetchone()
                    if twin is not None:
                        conn.execute("ROLLBACK")
                        return False
                    # A brand-new URL still needs the announcement id created.
                    affected = {announcement_id}
                    conn.execute(
                        "INSERT INTO crawl_results(submitted_url, announcement_id,"
                        " title, deadline_raw, final_url, batch_id, batch_seq,"
                        " fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (submitted_url, announcement_id, title, deadline_raw,
                         final_url, batch_id, batch_seq, fetched_at))

                for ann_id in affected:
                    self._recompute(conn, ann_id)
                conn.execute("COMMIT")
                return True
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def ingest_success(self, **kwargs: Any) -> bool:
        return await run_in_threadpool(self.ingest_success_sync, **kwargs)

    # -- verification ------------------------------------------------------

    def verify_sync(self, announcement_id: str, chosen_url: str,
                    expected_version: int,
                    now: Optional[float] = None) -> Dict[str, Any]:
        with self._lock:
            conn = self._connect()
            conn.execute("BEGIN IMMEDIATE")
            try:
                state = conn.execute(
                    "SELECT * FROM id_state WHERE announcement_id = ?",
                    (announcement_id,)).fetchone()
                if state is None:
                    # The announcement disappeared; roll back before raising.
                    raise UnknownAnnouncementError(announcement_id)
                if state["version"] != expected_version:
                    # The page was opened before a later committed change.
                    raise StaleVersionError(
                        f"页面版本 {expected_version} 已过期，当前版本 "
                        f"{state['version']}，请刷新页面后重新核定")
                chosen = conn.execute(
                    "SELECT * FROM crawl_results WHERE announcement_id = ?"
                    " AND submitted_url = ?",
                    (announcement_id, chosen_url)).fetchone()
                if chosen is None:
                    raise InvalidChoiceError(chosen_url)

                rows = conn.execute(
                    "SELECT * FROM crawl_results WHERE announcement_id = ?",
                    (announcement_id,)).fetchall()
                fp = self._fingerprint(rows)
                created_at = time.time() if now is None else now
                conn.execute(
                    "INSERT INTO verifications(announcement_id, chosen_url,"
                    " chosen_title, chosen_deadline, fingerprint, version,"
                    " created_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
                    " ON CONFLICT(announcement_id) DO UPDATE SET"
                    " chosen_url = excluded.chosen_url,"
                    " chosen_title = excluded.chosen_title,"
                    " chosen_deadline = excluded.chosen_deadline,"
                    " fingerprint = excluded.fingerprint,"
                    " version = excluded.version,"
                    " created_at = excluded.created_at",
                    (announcement_id, chosen_url, chosen["title"],
                     chosen["deadline_raw"], fp, state["version"], created_at))
                conn.execute("COMMIT")
                return {
                    "announcement_id": announcement_id,
                    "chosen_url": chosen_url,
                    "chosen_title": chosen["title"],
                    "chosen_deadline": chosen["deadline_raw"],
                    "version": state["version"],
                    "created_at": created_at,
                }
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def verify(self, announcement_id: str, chosen_url: str,
                     expected_version: int) -> Dict[str, Any]:
        return await run_in_threadpool(
            self.verify_sync, announcement_id, chosen_url, expected_version)

    # -- reads -------------------------------------------------------------

    def snapshot_sync(self) -> Dict[str, Any]:
        """Return the whole catalog as one consistent committed snapshot."""
        with self._lock:
            conn = self._connect()
            conn.execute("BEGIN")
            try:
                rows = conn.execute(
                    "SELECT * FROM crawl_results ORDER BY announcement_id,"
                    " batch_seq DESC, submitted_url").fetchall()
                states = {
                    r["announcement_id"]: r
                    for r in conn.execute("SELECT * FROM id_state").fetchall()}
                verifies = {
                    r["announcement_id"]: r
                    for r in conn.execute(
                        "SELECT * FROM verifications").fetchall()}
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

        grouped: Dict[str, List[sqlite3.Row]] = {}
        for r in rows:
            grouped.setdefault(r["announcement_id"], []).append(r)

        entries: List[Dict[str, Any]] = []
        for ann_id, srcs in sorted(grouped.items()):
            titles = {r["title"] for r in srcs}
            deadlines = {r["deadline_raw"] for r in srcs}
            consistent = len(titles) == 1 and len(deadlines) == 1
            verify = verifies.get(ann_id)
            if verify is not None:
                state = VERIFIED
                title = verify["chosen_title"]
                deadline = verify["chosen_deadline"]
            elif consistent:
                state = CONSISTENT
                title = next(iter(titles))
                deadline = next(iter(deadlines))
            else:
                state = CONFLICT
                title = ""
                deadline = ""
            entries.append({
                "announcement_id": ann_id,
                "state": state,
                "state_label": STATE_LABELS[state],
                "version": states[ann_id]["version"],
                "fingerprint": states[ann_id]["fingerprint"],
                "consistent": consistent,
                "title": title,
                "deadline_raw": deadline,
                "sources": [dict(r) for r in srcs],
                "verification": (
                    {
                        "chosen_url": verify["chosen_url"],
                        "chosen_title": verify["chosen_title"],
                        "chosen_deadline": verify["chosen_deadline"],
                        "created_at": verify["created_at"],
                    }
                    if verify is not None else None),
            })
        supplyable = sum(1 for e in entries if e["state"] != CONFLICT)
        return {
            "generated_at": time.time(),
            "total": len(entries),
            "supplyable": supplyable,
            "suspended": len(entries) - supplyable,
            "entries": entries,
        }

    async def snapshot(self) -> Dict[str, Any]:
        return await run_in_threadpool(self.snapshot_sync)

    def catalog_csv_sync(self) -> str:
        """Build the catalog CSV from one committed snapshot.

        One row per announcement id; only consistent or verified entries are
        exported.  The snapshot and the serialisation happen under the same
        lock, so every export corresponds to a single committed state.
        """
        snap = self.snapshot_sync()
        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=CSV_FIELDS,
                                quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for e in snap["entries"]:
            if e["state"] == CONFLICT:
                continue
            verify = e["verification"]
            writer.writerow({
                "announcement_id": e["announcement_id"],
                "state": e["state_label"],
                "title": e["title"],
                "deadline_raw": e["deadline_raw"],
                "chosen_url": verify["chosen_url"] if verify else "",
                "source_urls": " | ".join(s["submitted_url"]
                                          for s in e["sources"]),
                "verified_at": (
                    _fmt_ts(verify["created_at"]) if verify else ""),
            })
        return out.getvalue()

    async def catalog_csv(self) -> str:
        return await run_in_threadpool(self.catalog_csv_sync)


def _fmt_ts(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
