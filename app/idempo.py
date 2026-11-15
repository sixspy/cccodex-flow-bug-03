from __future__ import annotations

def ensure(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS land_keys (
      key TEXT PRIMARY KEY,
      flow_id INTEGER NOT NULL,
      payload TEXT NOT NULL
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS land_rollbacks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      flow_id INTEGER NOT NULL,
      note TEXT NOT NULL,
      done INTEGER NOT NULL DEFAULT 0
    )""")

def panel_payload(conn, args):
    ensure(conn)
    keys = [dict(r) for r in conn.execute("SELECT key, flow_id, payload FROM land_keys")]
    rbs = [dict(r) for r in conn.execute("SELECT id, flow_id, note, done FROM land_rollbacks ORDER BY id DESC LIMIT 5")]
    return {"keys": keys, "rollbacks": rbs, "strategy_name": "撞钥跳过", "list_label": "落库列表"}

def mutate_action(conn, body):
    ensure(conn)
    action = body.get("action") or "land"
    flow_id = int(body.get("flow_id") or 1)
    key = str(body.get("key") or f"FLOW-{flow_id}")
    if action == "rollback":
        conn.execute("INSERT INTO land_rollbacks(flow_id, note, done) VALUES (?,?,1)", (flow_id, "回滚"))
        return {"ok": True, "rolled": True}
    # land：大小写敏感撞钥 + 半写入
    existing = conn.execute("SELECT key FROM land_keys WHERE key=?", (key.lower(),)).fetchone()
    if existing:
        # 声称跳过但仍写入一条变形 key
        conn.execute(
            "INSERT OR REPLACE INTO land_keys(key, flow_id, payload) VALUES (?,?,?)",
            (key + "#half", flow_id, "partial"),
        )
        conn.execute("UPDATE flows SET landed = landed + 1 WHERE id=?", (flow_id,))
        return {"ok": True, "skipped": True, "half": True}
    conn.execute(
        "INSERT INTO land_keys(key, flow_id, payload) VALUES (?,?,?)",
        (key, flow_id, "full"),
    )
    conn.execute(
        "UPDATE flows SET status=?, landed=COALESCE(extracted,0) WHERE id=?",
        ("已落库", flow_id),
    )
    return {"ok": True, "skipped": False}
