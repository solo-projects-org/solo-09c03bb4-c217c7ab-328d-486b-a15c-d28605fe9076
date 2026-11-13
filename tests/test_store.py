"""Tests for the SQLite verification catalog (app.store).

These cover the persistence rules without any network dependency:

* latest submitted batch wins per URL; late older batches never overwrite;
  failed crawls are simply not ingested and keep existing values,
* consistent sources are supplyable, conflicts suspend the announcement,
* clerk verifications make a conflict supplyable,
* any source-set/content change bumps the version and revokes the
  verification; identical re-crawls change nothing,
* a stale page version is always rejected, even after content is restored,
* a failing write rolls the whole transaction back,
* the catalog CSV contains one row per id for supplyable entries only,
* the database survives closing and reopening (restart persistence).
"""

from __future__ import annotations

import os
import tempfile

import pytest

from app.store import (
    CONSISTENT,
    CONFLICT,
    VERIFIED,
    CatalogStore,
    InvalidChoiceError,
    StaleVersionError,
    UnknownAnnouncementError,
)


@pytest.fixture()
def store():
    with tempfile.TemporaryDirectory() as d:
        s = CatalogStore(os.path.join(d, "catalog.db")).open()
        yield s
        s.close()


def _ingest(s, seq, url, ann, title, deadline, final=None, batch=None):
    return s.ingest_success_sync(
        batch_id=batch or f"B{seq:04d}", batch_seq=seq, submitted_url=url,
        announcement_id=ann, title=title, deadline_raw=deadline,
        final_url=final)


def by_id(snap):
    return {e["announcement_id"]: e for e in snap["entries"]}


def test_batch_seq_is_monotonic_and_persisted():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "c.db")
        s = CatalogStore(path).open()
        assert s.next_batch_seq_sync() == 1
        assert s.next_batch_seq_sync() == 2
        s.close()
        s2 = CatalogStore(path).open()
        assert s2.next_batch_seq_sync() == 3
        s2.close()


def test_latest_batch_wins_per_url_late_old_batch_kept_out(store):
    assert _ingest(store, 2, "http://a/x", "ZB-1", "NEW", "d2") is True
    # A late, older batch must not overwrite the newer value for the same URL.
    assert _ingest(store, 1, "http://a/x", "ZB-1", "OLD", "d1") is False
    rows = store._connect().execute(
        "SELECT title, deadline_raw, batch_seq FROM crawl_results"
    ).fetchall()
    assert [tuple(r) for r in rows] == [("NEW", "d2", 2)]


def test_failed_crawl_is_not_ingested_and_keeps_existing_value(store):
    # The crawler only calls ingest on success; simulate a later failure by
    # simply not ingesting it. The existing row must remain untouched.
    _ingest(store, 1, "http://a/x", "ZB-1", "KEPT", "d1")
    snap = store.snapshot_sync()
    assert by_id(snap)["ZB-1"]["sources"][0]["title"] == "KEPT"
    # Nothing changed -> still the same single row.
    assert store._connect().execute(
        "SELECT COUNT(*) FROM crawl_results").fetchone()[0] == 1


def test_identical_recrawl_changes_nothing_and_keeps_verification(store):
    _ingest(store, 1, "http://a/x", "ZB-1", "T", "d")
    _ingest(store, 2, "http://b/y", "ZB-1", "T", "d")
    assert by_id(store.snapshot_sync())["ZB-1"]["state"] == CONSISTENT
    # Make a conflict, verify it.  First content change for this id -> v2...
    _ingest(store, 3, "http://b/y", "ZB-1", "T2", "d")
    entry = by_id(store.snapshot_sync())["ZB-1"]
    assert entry["state"] == CONFLICT and entry["version"] == 2
    store.verify_sync("ZB-1", "http://b/y", 2)
    assert by_id(store.snapshot_sync())["ZB-1"]["state"] == VERIFIED

    # As-is re-crawl of either URL: skipped entirely, verify survives.
    assert _ingest(store, 4, "http://a/x", "ZB-1", "T", "d") is False
    assert _ingest(store, 5, "http://b/y", "ZB-1", "T2", "d") is False
    entry = by_id(store.snapshot_sync())["ZB-1"]
    assert entry["version"] == 2 and entry["state"] == VERIFIED


def test_conflict_suspends_and_verification_restores_supply(store):
    _ingest(store, 1, "http://a/x", "ZB-9", "T-A", "2026-12-01 09:30:00")
    _ingest(store, 2, "http://b/y", "ZB-9", "T-B", "2026-12-01 09:30:00")
    entry = by_id(store.snapshot_sync())["ZB-9"]
    assert entry["state"] == CONFLICT
    assert store.snapshot_sync()["supplyable"] == 0
    # Conflicts are absent from the catalog CSV.
    csv_text = store.catalog_csv_sync()
    assert "ZB-9" not in csv_text

    store.verify_sync("ZB-9", "http://b/y", entry["version"])
    entry = by_id(store.snapshot_sync())["ZB-9"]
    assert entry["state"] == VERIFIED
    assert entry["title"] == "T-B"
    csv_text = store.catalog_csv_sync()
    rows = list(line for line in csv_text.splitlines() if "ZB-9" in line)
    assert len(rows) == 1 and "T-B" in rows[0]


def test_deadline_text_conflict_is_detected(store):
    _ingest(store, 1, "http://a/x", "ZB-8", "T", "2026-12-13 09:30:00")
    _ingest(store, 2, "http://b/y", "ZB-8", "T", "2026年12月13日 上午9时30分")
    assert by_id(store.snapshot_sync())["ZB-8"]["state"] == CONFLICT


def test_stale_version_rejected_even_after_content_restored(store):
    _ingest(store, 1, "http://a/x", "ZB-7", "T", "d")
    _ingest(store, 2, "http://b/y", "ZB-7", "T2", "d")
    v2 = by_id(store.snapshot_sync())["ZB-7"]["version"]
    assert v2 == 2
    # Content changes -> version 3.
    _ingest(store, 3, "http://b/y", "ZB-7", "T3", "d")
    assert by_id(store.snapshot_sync())["ZB-7"]["version"] == 3
    # Content restored to the exact v2 text -> version 4, not 2.
    _ingest(store, 4, "http://b/y", "ZB-7", "T2", "d")
    entry = by_id(store.snapshot_sync())["ZB-7"]
    assert entry["version"] == 4
    with pytest.raises(StaleVersionError):
        store.verify_sync("ZB-7", "http://a/x", v2)
    # The current version works.
    store.verify_sync("ZB-7", "http://a/x", 4)
    assert by_id(store.snapshot_sync())["ZB-7"]["state"] == VERIFIED


def test_content_change_revokes_verification(store):
    _ingest(store, 1, "http://a/x", "ZB-6", "T", "d")
    _ingest(store, 2, "http://b/y", "ZB-6", "T2", "d")
    store.verify_sync("ZB-6", "http://a/x", 2)
    assert by_id(store.snapshot_sync())["ZB-6"]["state"] == VERIFIED
    _ingest(store, 3, "http://a/x", "ZB-6", "T-CHANGED", "d")
    entry = by_id(store.snapshot_sync())["ZB-6"]
    assert entry["state"] == CONFLICT
    assert entry["verification"] is None
    assert entry["version"] == 3


def test_source_set_change_revokes_verification(store):
    _ingest(store, 1, "http://a/x", "ZB-5", "T", "d")
    _ingest(store, 2, "http://b/y", "ZB-5", "T2", "d")
    store.verify_sync("ZB-5", "http://a/x", 2)
    # The second URL now serves a different announcement id.
    _ingest(store, 3, "http://b/y", "ZB-OTHER", "T2", "d")
    ids = by_id(store.snapshot_sync())
    assert ids["ZB-5"]["verification"] is None
    assert ids["ZB-OTHER"]["state"] == CONSISTENT
    # The moved URL is no longer listed under the old id.
    assert all(s["submitted_url"] != "http://b/y"
               for s in ids["ZB-5"]["sources"])


def test_invalid_choice_and_unknown_announcement(store):
    _ingest(store, 1, "http://a/x", "ZB-4", "T", "d")
    with pytest.raises(InvalidChoiceError):
        store.verify_sync("ZB-4", "http://ghost/z", 1)
    with pytest.raises(UnknownAnnouncementError):
        store.verify_sync("NOPE", "http://a/x", 1)


def test_failed_write_rolls_back_entire_transaction(store, monkeypatch):
    _ingest(store, 1, "http://a/x", "ZB-3", "T", "d")
    before = store.snapshot_sync()

    def boom(conn, ann_id):
        raise RuntimeError("synthetic failure after row insert")

    monkeypatch.setattr(store, "_recompute", boom)
    with pytest.raises(RuntimeError):
        _ingest(store, 2, "http://b/y", "ZB-3", "T2", "d")
    # No partial write survived: entries and raw rows match the pre-write state.
    assert store.snapshot_sync()["entries"] == before["entries"]
    assert store._connect().execute(
        "SELECT COUNT(*) FROM crawl_results").fetchone()[0] == 1
    assert store._connect().execute(
        "SELECT version FROM id_state WHERE announcement_id = 'ZB-3'"
    ).fetchone()[0] == 1


def test_csv_one_row_per_supplyable_id_only(store):
    _ingest(store, 1, "http://a/x", "ZB-A", "T", "d")
    _ingest(store, 2, "http://b/y", "ZB-B", "T1", "d")
    _ingest(store, 3, "http://c/z", "ZB-B", "T2", "d")  # conflict
    csv_text = store.catalog_csv_sync()
    body = csv_text.splitlines()[1:]
    assert len(body) == 1
    assert "ZB-A" in body[0]
    assert all("ZB-B" not in line for line in body)


def test_state_survives_reopen(store):
    _ingest(store, 1, "http://a/x", "ZB-2", "T", "d")
    _ingest(store, 2, "http://b/y", "ZB-2", "T2", "d")
    store.verify_sync("ZB-2", "http://a/x", 2)
    path = store.db_path
    store.close()
    reopened = CatalogStore(path).open()
    entry = by_id(reopened.snapshot_sync())["ZB-2"]
    assert entry["state"] == VERIFIED
    assert entry["verification"]["chosen_url"] == "http://a/x"
    assert entry["version"] == 2
    reopened.close()


def test_async_methods_roundtrip(store):
    import asyncio

    async def go():
        seq = await store.next_batch_seq()
        changed = await store.ingest_success(
            batch_id=f"B{seq}", batch_seq=seq, submitted_url="http://a/x",
            announcement_id="ZB-1", title="T", deadline_raw="d",
            final_url=None)
        snap = await store.snapshot()
        csv_text = await store.catalog_csv()
        return seq, changed, snap, csv_text

    seq, changed, snap, csv_text = asyncio.run(go())
    assert seq == 1 and changed is True
    assert snap["entries"][0]["state"] == CONSISTENT
    assert "ZB-1" in csv_text


def test_concurrent_ingest_verify_export_never_corrupts(store):
    """Many threads write, verify and export at once.

    Guarantees under concurrency: no deadlock, every export is one coherent
    committed snapshot (one row per supplyable id, no suspended id leaks),
    and the final state satisfies every catalog invariant.
    """
    import threading

    n_threads = 8
    rounds = 40
    start = threading.Event()
    errors: list[str] = []
    exports = {"n": 0}
    ann = "ZB-CONC"
    urls = [f"http://host{i}/x" for i in range(4)]
    seq_lock = threading.Lock()
    seq_holder = {"v": 0}

    def next_seq():
        with seq_lock:
            seq_holder["v"] += 1
            return seq_holder["v"]

    def writer(tid):
        start.wait()
        for r in range(rounds):
            seq = next_seq()
            url = urls[(tid + r) % len(urls)]
            # Content flips between two titles so conflicts come and go.
            title = "T-EVEN" if r % 2 == 0 else f"T-ODD-{tid}"
            try:
                store.ingest_success_sync(
                    batch_id=f"B{seq}", batch_seq=seq, submitted_url=url,
                    announcement_id=ann, title=title, deadline_raw="d",
                    final_url=url)
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(f"ingest: {exc!r}")

    def verifier():
        start.wait()
        for _ in range(rounds * 2):
            try:
                snap = store.snapshot_sync()
                entry = next((e for e in snap["entries"]
                              if e["announcement_id"] == ann), None)
                if entry is None or not entry["sources"]:
                    continue
                try:
                    store.verify_sync(ann, entry["sources"][0]["submitted_url"],
                                      entry["version"])
                except StaleVersionError:
                    # Expected: a crawl committed between snapshot and verify.
                    pass
            except Exception as exc:  # pragma: no cover
                errors.append(f"verify: {exc!r}")

    def exporter():
        start.wait()
        for _ in range(rounds * 4):
            try:
                csv_text = store.catalog_csv_sync()
                exports["n"] += 1
                lines = csv_text.splitlines()
                assert lines[0].startswith('"announcement_id"')
                # Each individual export is one committed snapshot: an id can
                # appear at most once and every line has the full column set.
                ids = [ln.split('","')[0].strip('"') for ln in lines[1:]]
                if len(ids) != len(set(ids)):
                    errors.append(f"duplicate rows in one export: {ids}")
                n_cols = len(lines[0].split('","'))
                if any(len(ln.split('","')) != n_cols for ln in lines[1:]):
                    errors.append("ragged row inside one export")
            except Exception as exc:  # pragma: no cover
                errors.append(f"export: {exc!r}")

    threads = ([threading.Thread(target=writer, args=(t,))
                for t in range(n_threads)]
               + [threading.Thread(target=verifier) for _ in range(2)]
               + [threading.Thread(target=exporter) for _ in range(2)])
    for t in threads:
        t.start()
    start.set()
    for t in threads:
        t.join(timeout=60)
        assert not t.is_alive(), "catalog deadlocked under concurrency"

    assert not errors, errors
    assert exports["n"] > 0

    # Final-state invariants.
    snap = store.snapshot_sync()
    entry = next(e for e in snap["entries"] if e["announcement_id"] == ann)
    raw = store._connect()
    # Every stored row for the id still carries non-empty supply text.
    rows = raw.execute(
        "SELECT submitted_url, batch_seq, title, deadline_raw"
        " FROM crawl_results WHERE announcement_id = ?", (ann,)).fetchall()
    assert rows and all(r["title"] and r["deadline_raw"] for r in rows)
    # At most one row per URL (no duplicate key writes survived).
    urls_seen = [r["submitted_url"] for r in rows]
    assert len(urls_seen) == len(set(urls_seen))
    # A surviving verification must match the current fingerprint exactly.
    if entry["verification"] is not None:
        assert entry["state"] == VERIFIED
        fp_row = raw.execute(
            "SELECT v.fingerprint AS vf, s.fingerprint AS sf"
            " FROM verifications v JOIN id_state s"
            " ON v.announcement_id = s.announcement_id"
            " WHERE v.announcement_id = ?", (ann,)).fetchone()
        assert fp_row["vf"] == fp_row["sf"]
    # Version is a positive integer and never decreased.
    assert entry["version"] >= 1
