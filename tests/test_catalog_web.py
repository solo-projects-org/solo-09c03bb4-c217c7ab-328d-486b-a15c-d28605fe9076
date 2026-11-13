"""End-to-end tests for the verification catalog through the web app.

Spins up the two real mock portals (same fixtures as the crawler tests) and
drives the FastAPI app with TestClient:

* two-source consistent -> catalog entry supplyable, in catalog CSV;
* title/deadline conflicts -> suspended, absent from CSV;
* clerk verification with the rendered page version -> supplyable;
* stale page submission -> rejected even after content is restored;
* runtime source edit + re-crawl -> verification revoked;
* identical re-crawl -> verification kept;
* catalog persists in SQLite and survives an app reload (restart).
"""

from __future__ import annotations

import importlib
import os
import socket
import subprocess
import sys
import tempfile
import time
from typing import Iterator

import httpx
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


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


@pytest.fixture(scope="module")
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


@pytest.fixture()
def app_client(portals, tmp_path, monkeypatch):
    """Fresh app instance backed by an isolated SQLite file."""
    db_path = str(tmp_path / "catalog.db")
    monkeypatch.setenv("CATALOG_DB", db_path)
    monkeypatch.setenv("SOURCE_A_URL", portals["A"])
    monkeypatch.setenv("SOURCE_B_URL", portals["B"])
    monkeypatch.setenv("SAMPLE_BASE_A", portals["A"])
    monkeypatch.setenv("SAMPLE_BASE_B", portals["B"])

    import app.main as main
    importlib.reload(main)
    main.SOURCE_A_PUBLIC = portals["A"]
    main.SOURCE_B_PUBLIC = portals["B"]

    from fastapi.testclient import TestClient
    with TestClient(main.app) as client:
        yield client, main, db_path
    main.manager._client = None
    main.manager.batches.clear()


def _collect(client, urls):
    resp = client.post("/collect", data={"urls": "\n".join(urls)},
                       follow_redirects=False)
    assert resp.status_code == 303
    batch_id = resp.headers["location"].rsplit("/", 1)[1]
    for _ in range(100):
        data = client.get(f"/api/batches/{batch_id}").json()
        if data["batch"]["status"] in ("done", "aborted"):
            break
        time.sleep(0.1)
    assert data["batch"]["status"] == "done"
    return data


def _entry(client, ann_id):
    snap = client.get("/api/catalog").json()
    entries = {e["announcement_id"]: e for e in snap["entries"]}
    assert ann_id in entries, snap
    return entries[ann_id]


def test_consistent_and_conflict_flow_and_catalog_csv(app_client, portals):
    client, main, _ = app_client
    httpx.post(portals["A"] + "/__reset")
    httpx.post(portals["B"] + "/__reset")

    data = _collect(client, [
        f"{portals['A']}/notice/shared/N2026-501",
        f"{portals['B']}/notice/shared/N2026-501",
        f"{portals['A']}/notice/conflict-title/N2026-510",
        f"{portals['B']}/notice/conflict-title/N2026-510",
    ])
    assert data["batch"]["failed"] == 0
    ann_shared = "ZB-2026-N2026-501"
    ann_conflict = "ZB-2026-N2026-510"

    consistent = _entry(client, ann_shared)
    assert consistent["state"] == "consistent"
    assert len(consistent["sources"]) == 2

    conflict = _entry(client, ann_conflict)
    assert conflict["state"] == "conflict"
    titles = {s["title"] for s in conflict["sources"]}
    assert len(titles) == 2

    # Catalog page renders the verification form with the version token.
    page = client.get("/catalog").text
    assert ann_conflict in page and 'name="version"' in page
    assert "暂停供数" in page

    # CSV: consistent id present, conflict id absent, one row per id.
    csv_text = client.get("/catalog.csv").content.decode("utf-8-sig")
    body = [ln for ln in csv_text.splitlines() if ann_shared in ln]
    assert len(body) == 1
    assert all(ann_conflict not in ln for ln in csv_text.splitlines())


def test_verification_then_change_revokes_and_stale_page_rejected(
        app_client, portals):
    client, main, _ = app_client
    httpx.post(portals["A"] + "/__reset")
    httpx.post(portals["B"] + "/__reset")

    _collect(client, [
        f"{portals['A']}/notice/conflict-title/N2026-510",
        f"{portals['B']}/notice/conflict-title/N2026-510",
    ])
    ann = "ZB-2026-N2026-510"
    entry = _entry(client, ann)
    version = entry["version"]
    chosen = next(s["submitted_url"] for s in entry["sources"]
                  if portals["B"] in s["submitted_url"])

    # Stale token (wrong version) is rejected.
    stale = client.post(f"/catalog/{ann}/verify",
                        data={"chosen_url": chosen, "version": version + 99},
                        follow_redirects=False)
    assert stale.status_code == 303
    follow = client.get(stale.headers["location"])
    assert "核定被拒绝" in follow.text and "过期" in follow.text
    assert _entry(client, ann)["state"] == "conflict"

    # Correct token -> verified and supplyable via CSV.
    ok = client.post(f"/catalog/{ann}/verify",
                     data={"chosen_url": chosen, "version": version},
                     follow_redirects=False)
    assert ok.status_code == 303
    entry = _entry(client, ann)
    assert entry["state"] == "verified"
    assert entry["verification"]["chosen_url"] == chosen
    csv_text = client.get("/catalog.csv").content.decode("utf-8-sig")
    assert ann in csv_text

    # Keep the old page open. Edit portal A's copy of a *shared* notice and
    # re-crawl: source content changes -> verification revoked, version bumps.
    _collect(client, [
        f"{portals['A']}/notice/shared/N2026-502",
        f"{portals['B']}/notice/shared/N2026-502",
    ])
    shared = "ZB-2026-N2026-502"
    s_entry = _entry(client, shared)
    s_version = s_entry["version"]
    s_choice = s_entry["sources"][0]["submitted_url"]
    client.post(f"/catalog/{shared}/verify",
                data={"chosen_url": s_choice, "version": s_version},
                follow_redirects=False)
    assert _entry(client, shared)["state"] == "verified"

    edit = httpx.get(portals["A"] +
                     "/__edit?code=N2026-502&title=源A临时改标题&deadline=2026-12-20 09:30:00")
    assert edit.status_code == 200
    _collect(client, [f"{portals['A']}/notice/shared/N2026-502"])
    changed = _entry(client, shared)
    assert changed["state"] == "conflict"
    assert changed["verification"] is None
    assert changed["version"] > s_version

    # The old page's token is rejected even though the verification existed
    # when the page was rendered.
    stale2 = client.post(f"/catalog/{shared}/verify",
                         data={"chosen_url": s_choice,
                               "version": s_version},
                         follow_redirects=False)
    page = client.get(stale2.headers["location"]).text
    assert "核定被拒绝" in page

    # Restore portal A's original content and re-crawl: the version keeps
    # climbing, so the old token stays rejected (no implicit re-verify).
    httpx.post(portals["A"] + "/__reset")
    _collect(client, [f"{portals['A']}/notice/shared/N2026-502"])
    restored = _entry(client, shared)
    assert restored["state"] == "consistent"
    stale3 = client.post(f"/catalog/{shared}/verify",
                         data={"chosen_url": s_choice,
                               "version": s_version},
                         follow_redirects=False)
    assert "核定被拒绝" in client.get(stale3.headers["location"]).text


def test_identical_recrawl_keeps_verification(app_client, portals):
    client, main, _ = app_client
    httpx.post(portals["A"] + "/__reset")
    httpx.post(portals["B"] + "/__reset")

    _collect(client, [
        f"{portals['A']}/notice/conflict-deadline/N2026-511",
        f"{portals['B']}/notice/conflict-deadline/N2026-511",
    ])
    ann = "ZB-2026-N2026-511"
    entry = _entry(client, ann)
    assert entry["state"] == "conflict"
    version = entry["version"]
    chosen = entry["sources"][0]["submitted_url"]
    client.post(f"/catalog/{ann}/verify",
                data={"chosen_url": chosen, "version": version},
                follow_redirects=False)

    # Re-crawl both sources without any content change.
    _collect(client, [
        f"{portals['A']}/notice/conflict-deadline/N2026-511",
        f"{portals['B']}/notice/conflict-deadline/N2026-511",
    ])
    again = _entry(client, ann)
    assert again["state"] == "verified"
    assert again["version"] == version
    assert again["verification"]["chosen_url"] == chosen


def test_catalog_persists_across_app_restart(app_client, portals, tmp_path,
                                             monkeypatch):
    client, main, db_path = app_client
    httpx.post(portals["A"] + "/__reset")
    httpx.post(portals["B"] + "/__reset")

    _collect(client, [
        f"{portals['A']}/notice/shared/N2026-501",
        f"{portals['B']}/notice/shared/N2026-501",
    ])
    ann = "ZB-2026-N2026-501"
    assert _entry(client, ann)["state"] == "consistent"
    main.store.close()

    # Simulate a container restart: reload the app against the same SQLite
    # file inside the data volume.
    monkeypatch.setenv("CATALOG_DB", db_path)
    monkeypatch.setenv("SOURCE_A_URL", portals["A"])
    monkeypatch.setenv("SOURCE_B_URL", portals["B"])
    monkeypatch.setenv("SAMPLE_BASE_A", portals["A"])
    monkeypatch.setenv("SAMPLE_BASE_B", portals["B"])
    import app.main as main2
    importlib.reload(main2)
    main2.SOURCE_A_PUBLIC = portals["A"]
    main2.SOURCE_B_PUBLIC = portals["B"]
    from fastapi.testclient import TestClient
    with TestClient(main2.app) as client2:
        entry = _entry(client2, ann)
        assert entry["state"] == "consistent"
        assert len(entry["sources"]) == 2
        # Batch sequence persisted: new batches get higher sequence numbers.
        data = _collect(client2, [
            f"{portals['A']}/notice/shared/N2026-501"])
        assert data["batch"]["success"] == 1
        again = _entry(client2, ann)
        # Identical re-crawl after restart keeps state and version.
        assert again["version"] == entry["version"]
    main2.manager._client = None
    main2.manager.batches.clear()


def test_batch_csv_still_available_alongside_catalog(app_client, portals):
    client, main, _ = app_client
    httpx.post(portals["A"] + "/__reset")
    data = _collect(client, [
        f"{portals['A']}/notice/N2026-700",
        f"{portals['A']}/notice/broken/N2026-701",
    ])
    assert data["batch"]["failed"] == 1
    import re
    batch_id = re.search(r"/batches/(B\d+)", client.get("/").text).group(1)
    batch_csv = client.get(f"/batches/{batch_id}/csv")
    assert batch_csv.status_code == 200
    text = batch_csv.content.decode("utf-8-sig")
    assert "ZB-2026-N2026-700" in text
    # The original per-batch CSV columns are unchanged.
    assert text.splitlines()[0].startswith('"url"')
