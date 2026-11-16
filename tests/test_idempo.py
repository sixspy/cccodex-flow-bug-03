import sqlite3
import threading

import pytest

from app import db, idempo


@pytest.fixture()
def conn(tmp_path):
    path = tmp_path / "land.db"
    db.init_db(str(path))
    c = db.connect(str(path))
    c.row_factory = sqlite3.Row
    yield c
    c.close()


def _extract(conn, flow_id=1):
    db.run_extract(conn, flow_id)
    conn.commit()


def _inventory(conn, flow_id=1):
    return idempo.inventory_count(conn, flow_id)


def _flow(conn, flow_id=1):
    return dict(conn.execute("SELECT * FROM flows WHERE id=?", (flow_id,)).fetchone())


def _panel(conn):
    return idempo.panel_payload(conn, {})


def test_land_writes_whole_batch_and_list_matches_inventory(conn):
    _extract(conn)
    out = idempo.land_batch(conn, 1)
    assert out["verdict"] == "landed"
    assert out["written"] == _flow(conn)["extracted"]
    assert _inventory(conn) == out["written"]
    assert _flow(conn)["landed"] == _inventory(conn)
    assert _flow(conn)["status"] == "已落库"


def test_collision_skips_whole_batch_zero_writes_and_counts(conn):
    _extract(conn)
    first = idempo.land_batch(conn, 1)
    before = _inventory(conn)

    again = idempo.land_batch(conn, 1)
    assert again["verdict"] == "skipped"
    assert again["skip"] is True
    assert again["half"] is False
    # 整批同一判定：全部跳过，跳过计数 = 整批行数，写入 0 行
    assert again["written"] == 0
    assert again["skipped"] == first["written"]
    assert again["skipped_total"] == first["written"]
    # 不得半写入：库存不变，列表与库存一致
    assert _inventory(conn) == before
    assert _flow(conn)["landed"] == before
    assert {v["verdict"] for v in again["verdicts"]} == {"skipped"}


def test_single_colliding_row_skips_whole_batch(conn):
    _extract(conn)
    idempo.land_batch(conn, 1)
    existing = conn.execute("SELECT key FROM land_keys LIMIT 1").fetchone()["key"]

    out = idempo.land_batch(conn, 1, batch_key="EXTRA", keys=[existing, "NEW-A", "NEW-B"])
    assert out["verdict"] == "skipped"
    assert out["written"] == 0
    assert out["skipped"] == 3
    # 批内没有一行被写进去（无半写入）
    assert conn.execute(
        "SELECT COUNT(*) c FROM land_keys WHERE batch_key='EXTRA'"
    ).fetchone()["c"] == 0


def test_key_collision_is_case_and_space_insensitive(conn):
    _extract(conn)
    idempo.land_batch(conn, 1)
    key = conn.execute("SELECT key FROM land_keys LIMIT 1").fetchone()["key"]

    out = idempo.land_batch(conn, 1, batch_key="CASE", keys=[f"  {key.lower()} "])
    assert out["verdict"] == "skipped"
    assert out["written"] == 0
    assert conn.execute(
        "SELECT COUNT(*) c FROM land_keys WHERE batch_key='CASE'"
    ).fetchone()["c"] == 0


def test_rollback_is_atomic_and_realigns_list_with_inventory(conn):
    _extract(conn)
    landed = idempo.land_batch(conn, 1)
    assert _inventory(conn) == landed["written"]

    out = idempo.rollback_batch(conn, 1)
    assert out["rolled"] is True
    assert out["removed"] == landed["written"]
    assert _inventory(conn) == 0
    assert _flow(conn)["landed"] == 0
    assert out["consistent"] is True
    # 回滚后仍可整批重新落库
    again = idempo.land_batch(conn, 1)
    assert again["verdict"] == "landed"
    assert _inventory(conn) == again["written"] == _flow(conn)["landed"]


def test_panel_reports_consistency_without_mismatch_or_half_writes(conn):
    _extract(conn, 1)
    _extract(conn, 2)
    idempo.land_batch(conn, 1)
    idempo.land_batch(conn, 2)
    idempo.land_batch(conn, 2)  # 一次撞钥跳过

    panel = _panel(conn)
    assert panel["consistency"]["ok"] is True
    assert panel["consistency"]["mismatches"] == []
    assert panel["consistency"]["half_writes"] == []
    assert panel["skip_total"] == _flow(conn, 2)["extracted"]
    assert panel["list_landed_total"] == panel["landed_total"]
    # 策略标题随列表可核对
    assert panel["strategy"]["title"] in ("撞钥跳过 · 整批原子",)
    assert panel["strategy_name"] == panel["strategy"]["title"]

    states = {b["batch_key"]: b["state"] for b in panel["batches"]}
    assert states["FLOW-1"] == "landed"
    assert states["FLOW-2"] == "landed"
    skipped = [b for b in panel["batches"] if b["batch_key"] == "FLOW-2"][0]
    assert skipped["last_verdict"] == "skipped"
    assert skipped["written"] == skipped["inventory"]


def test_detects_legacy_mismatch_instead_of_hiding_it(conn):
    _extract(conn)
    idempo.land_batch(conn, 1)
    # 人为把列表数字冒充成库存（模拟账实分裂），panel 必须报不一致。
    conn.execute("UPDATE flows SET landed = landed + 5 WHERE id=1")
    conn.commit()
    panel = _panel(conn)
    assert panel["consistency"]["ok"] is False
    assert panel["consistency"]["mismatches"]


def test_concurrent_same_batch_lands_are_serialized(tmp_path):
    path = tmp_path / "race.db"
    db.init_db(str(path))
    setup = db.connect(str(path))
    _extract(setup, 2)
    expected = db.get_flow(setup, 2)["extracted"]
    setup.close()

    n = 8
    barrier = threading.Barrier(n)
    results = []
    errors = []

    def worker():
        c = db.connect(str(path))
        try:
            barrier.wait()
            results.append(idempo.land_batch(c, 2))
        except Exception as exc:  # pragma: no cover - 失败时给出原因
            errors.append(exc)
        finally:
            c.close()

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    check = db.connect(str(path))
    try:
        # 串行落库：只写一遍，库存 = 行数，列表与库存一致
        assert idempo.inventory_count(check, 2) == expected
        assert db.get_flow(check, 2)["landed"] == expected
        landed = [r for r in results if r["verdict"] == "landed"]
        skipped = [r for r in results if r["verdict"] == "skipped"]
        assert len(landed) == 1
        assert len(skipped) == n - 1
        assert all(r["written"] == 0 for r in skipped)
        panel = idempo.panel_payload(check, {})
        assert panel["consistency"]["ok"] is True
        assert panel["skip_total"] == expected * (n - 1)
        # 后到者记争用
        assert panel["contention_total"] >= 1
    finally:
        check.close()


def test_re_extract_revokes_old_inventory(conn):
    _extract(conn)
    idempo.land_batch(conn, 1)
    assert _inventory(conn) == _flow(conn)["landed"]

    conn.execute("UPDATE flows SET status='待抽取' WHERE id=1")
    conn.commit()
    db.run_extract(conn, 1)
    conn.commit()
    assert _inventory(conn) == 0
    assert _flow(conn)["landed"] == 0
    assert _panel(conn)["consistency"]["ok"] is True


def test_api_land_and_rollback_keep_panel_consistent(client):
    _req, resp = client.post("/api/login", json={"username": "admin", "password": "123456"})
    cookie = resp.cookies.get("flow_session")
    headers = {"Cookie": f"flow_session={cookie}"}

    _req, resp = client.post("/api/flows/1/extract", headers=headers)
    assert resp.status == 200

    _req, resp = client.post("/api/flows/1/land", headers=headers)
    assert resp.status == 200
    landing = resp.json["landing"]
    card = [b for b in landing["batches"] if b["flow_id"] == 1][0]
    assert card["state"] == "landed"
    assert card["inventory"] == card["list_landed"]
    assert landing["consistency"]["ok"] is True

    _req, resp = client.post(
        "/api/idempo/action", json={"action": "rollback", "flow_id": 1}, headers=headers
    )
    assert resp.status == 200
    assert resp.json["rolled"] is True
    assert resp.json["panel"]["consistency"]["ok"] is True
    assert resp.json["panel"]["inventory"][0]["inventory"] == 0


def test_api_skip_is_counted_and_visible_in_batch_detail(client):
    _req, resp = client.post("/api/login", json={"username": "admin", "password": "123456"})
    headers = {"Cookie": f"flow_session={resp.cookies.get('flow_session')}"}
    client.post("/api/flows/2/extract", headers=headers)
    client.post("/api/flows/2/land", headers=headers)
    _req, resp = client.post(
        "/api/idempo/action", json={"action": "land", "flow_id": 2}, headers=headers
    )
    assert resp.status == 200
    body = resp.json
    assert body["verdict"] == "skipped"
    assert body["written"] == 0
    assert body["skipped"] > 0

    _req, resp = client.get("/api/idempo/batch/FLOW-2", headers=headers)
    assert resp.status == 200
    batch = resp.json["batch"]
    assert batch["last_verdict"] == "skipped"
    assert batch["skipped"] == body["skipped_total"]
    assert batch["half_write"] is False


def test_migration_is_skipped_for_memory_db_but_runs_per_file(tmp_path):
    a = tmp_path / "a.db"
    db.init_db(str(a))
    ca = db.connect(str(a))
    cb = db.connect(str(a))
    try:
        idempo.ensure(ca)
        ca.commit()
        # 第二个连接即使没有在建表时提交事务，也能看到表
        cb.row_factory = sqlite3.Row
        assert idempo.inventory_count(cb, 1) == 0
    finally:
        ca.close()
        cb.close()


def test_legacy_half_written_rows_are_cleaned_on_migration(tmp_path):
    """旧版半写入留下的变形钥匙在迁移时被清掉，库存不再虚高。"""
    path = tmp_path / "legacy.db"
    raw = sqlite3.connect(str(path))
    raw.executescript(
        """
        CREATE TABLE flows (
          id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL,
          code TEXT NOT NULL UNIQUE, status TEXT NOT NULL, source_uri TEXT NOT NULL,
          extracted INTEGER NOT NULL DEFAULT 0, landed INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE land_keys (key TEXT PRIMARY KEY, flow_id INTEGER NOT NULL,
          payload TEXT NOT NULL);
        CREATE TABLE land_rollbacks (id INTEGER PRIMARY KEY AUTOINCREMENT,
          flow_id INTEGER NOT NULL, note TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0);
        INSERT INTO flows (title, code, status, source_uri, extracted, landed)
          VALUES ('旧流', 'legacy', '已落库', 'mock://legacy', 3, 5);
        INSERT INTO land_keys VALUES ('LEG-01', 1, 'full');
        INSERT INTO land_keys VALUES ('LEG-02', 1, 'full');
        INSERT INTO land_keys VALUES ('LEG-03#half', 1, 'partial');
        INSERT INTO land_keys VALUES ('LEG-04#half', 1, 'partial');
        INSERT INTO land_keys VALUES ('LEG-05#half', 1, 'partial');
        """
    )
    raw.commit()
    raw.close()

    c = db.connect(str(path))
    try:
        c.row_factory = sqlite3.Row
        idempo.ensure(c)
        c.commit()
        assert idempo.inventory_count(c, 1) == 2
        # 迁移后库存 2 行；flows.landed 仍是旧值 5，panel 如实报不一致。
        panel = idempo.panel_payload(c, {})
        assert panel["inventory"][0]["inventory"] == 2
        assert panel["consistency"]["ok"] is False
        # 整笔回滚后强制对齐：列表与库存都归零。
        out = idempo.rollback_batch(c, 1)
        assert out["removed"] == 2
        assert out["consistent"] is True
        assert idempo.panel_payload(c, {})["consistency"]["ok"] is True
    finally:
        c.close()


def test_skipped_batch_still_keeps_inventory_after_case_variant_attempt(conn):
    """撞钥跳过不得改动库存；大小写变形只算撞钥，不算新行。"""
    _extract(conn)
    landed = idempo.land_batch(conn, 1)
    key = conn.execute("SELECT key FROM land_keys LIMIT 1").fetchone()["key"]
    before = _inventory(conn)

    out = idempo.land_batch(conn, 1, batch_key="VAR", keys=[key.swapcase()])
    assert out["verdict"] == "skipped"
    assert out["written"] == 0
    assert _inventory(conn) == before == landed["written"]
    assert _flow(conn)["landed"] == before


def test_no_half_write_visible_in_panel_after_many_skips(conn):
    _extract(conn)
    idempo.land_batch(conn, 1)
    for i in range(5):
        idempo.land_batch(conn, 1, batch_key=f"SKIP-{i}", keys=["FLOW-1-01"])
    panel = _panel(conn)
    assert panel["consistency"]["half_writes"] == []
    # 所有批次都满足 库存 == 写入行数（整批口径，无部分留存）
    for batch in panel["batches"]:
        assert batch["inventory"] == batch["written"]
        assert batch["half_write"] is False


def test_batch_cannot_be_shared_across_flows(conn):
    _extract(conn, 1)
    _extract(conn, 2)
    idempo.land_batch(conn, 1)
    before = _inventory(conn, 2)

    out = idempo.land_batch(conn, 2, batch_key="FLOW-1", keys=["X-1"])
    assert out["ok"] is False
    assert "跨流" in out["error"]
    assert _inventory(conn, 2) == before
    # 回滚也不能跨流撤别人的批次
    rb = idempo.rollback_batch(conn, 2, "FLOW-1")
    assert rb["ok"] is False
    assert _inventory(conn, 1) > 0
