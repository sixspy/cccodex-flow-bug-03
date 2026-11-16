"""幂等落库 / 回滚（整批原子口径）。

正确性口径只取一种，不与“部分留存”混用：

* 落库：**整批原子**。同一批里任一行撞到幂等钥匙，整批同一判定——全部跳过，
  跳过计数 +N，写入 0 行，绝不做半写入。
* 回滚：**原子整笔**。一次撤销整批（默认撤销该流全部库存），撤销后强制把
  ``flows.landed`` / 状态与库存 ``land_keys`` 对齐，列表与库存必须一致。
* 并发：同批落库串行化（按批进程内锁 + SQLite ``BEGIN IMMEDIATE``），后到者
  不重复写入，并在批次上记争用。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime

STRATEGY = {
    "title": "撞钥跳过 · 整批原子",
    "mode": "atomic_batch",
    "mode_label": "原子整批",
    "skip_policy": "同批任一行撞钥，整批同一判定：全部跳过并计数，写入 0 行",
    "rollback_policy": "回滚按批原子整笔撤销，撤销后列表与库存强制对齐",
    "consistency": "正确性口径只取原子整批（全落或全跳过、整笔回滚），不与部分留存混用",
}

_BUSY_RETRIES = 120
_SAMPLE_LIMIT = 500

_LOCKS: dict[str, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()
_INFLIGHT: dict[str, int] = {}
_LAST_DECISION: dict[str, tuple[float, str]] = {}
# 同批判定刚结束的极短窗口内再次到达的落库，视作同一波争抢里的“后到”。
# 单 worker 下并发请求会被事件循环排队，等到执行时首个请求已判定完，
# 光靠等锁探测不到；窗口取短值，避免把人工连点误记为争用。
_RACE_WINDOW = 0.5
_DECIDED = ("landed", "skipped")


def _now() -> str:
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


def normalize_key(value) -> str:
    """落库行钥匙的展示/存储形态：去首尾空白。"""
    return str(value or "").strip()


def norm_key(value) -> str:
    """撞钥判定形态：去空白 + 大小写折叠，避免以大小写变形绕过判重。"""
    return normalize_key(value).upper()


def _batch_key(value, flow_id: int) -> str:
    key = norm_key(value)
    return key or f"FLOW-{flow_id}"


def _add_column(conn, table: str, column: str, ddl: str) -> None:
    cols = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def _dedupe_key_norms(conn) -> None:
    """迁移旧库：同一规范化钥匙只保留最早一行，保证唯一索引可建。"""
    dup = conn.execute(
        "SELECT key_norm FROM land_keys WHERE key_norm != ''"
        " GROUP BY key_norm HAVING COUNT(*) > 1"
    ).fetchall()
    for row in dup:
        rows = conn.execute(
            "SELECT rowid FROM land_keys WHERE key_norm=? ORDER BY rowid", (row["key_norm"],)
        ).fetchall()
        for extra in rows[1:]:
            conn.execute("DELETE FROM land_keys WHERE rowid=?", (extra["rowid"],))


_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY: set[str] = set()


def _db_identity(conn) -> str:
    for row in conn.execute("PRAGMA database_list"):
        if row[1] == "main":
            return row[2] or ":memory:"
    return ":memory:"


def _schema_present(conn) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='land_keys'"
    ).fetchone()
    return row is not None


def ensure(conn) -> None:
    """建表 + 迁移；每个库每个进程只做一次，避免并发调用在 DDL 上互相争锁。"""
    identity = _db_identity(conn)
    if identity != ":memory:" and identity in _SCHEMA_READY and _schema_present(conn):
        return
    with _SCHEMA_LOCK:
        if identity in _SCHEMA_READY and _schema_present(conn):
            return
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS land_keys (
              key TEXT PRIMARY KEY,
              flow_id INTEGER NOT NULL,
              batch_key TEXT NOT NULL DEFAULT '',
              key_norm TEXT NOT NULL DEFAULT '',
              payload TEXT NOT NULL DEFAULT 'full',
              created_at TEXT NOT NULL DEFAULT ''
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS land_batches (
              batch_key TEXT PRIMARY KEY,
              flow_id INTEGER NOT NULL,
              state TEXT NOT NULL DEFAULT 'landed',
              rows INTEGER NOT NULL DEFAULT 0,
              written INTEGER NOT NULL DEFAULT 0,
              skipped INTEGER NOT NULL DEFAULT 0,
              contention INTEGER NOT NULL DEFAULT 0,
              last_verdict TEXT NOT NULL DEFAULT '',
              last_reason TEXT NOT NULL DEFAULT '',
              sample_keys TEXT NOT NULL DEFAULT '[]',
              created_at TEXT NOT NULL DEFAULT '',
              updated_at TEXT NOT NULL DEFAULT ''
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS land_rollbacks (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              flow_id INTEGER NOT NULL,
              batch_key TEXT NOT NULL DEFAULT '',
              note TEXT NOT NULL,
              removed INTEGER NOT NULL DEFAULT 0,
              done INTEGER NOT NULL DEFAULT 1,
              created_at TEXT NOT NULL DEFAULT ''
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS land_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              batch_key TEXT NOT NULL,
              flow_id INTEGER NOT NULL,
              kind TEXT NOT NULL,
              count INTEGER NOT NULL DEFAULT 0,
              note TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL DEFAULT ''
            )"""
        )
        # 旧库迁移：补齐新增列，把历史散钥匙归入默认批次。
        _add_column(conn, "land_keys", "batch_key", "TEXT NOT NULL DEFAULT ''")
        _add_column(conn, "land_keys", "key_norm", "TEXT NOT NULL DEFAULT ''")
        _add_column(conn, "land_keys", "created_at", "TEXT NOT NULL DEFAULT ''")
        _add_column(conn, "land_batches", "sample_keys", "TEXT NOT NULL DEFAULT '[]'")
        _add_column(conn, "land_rollbacks", "batch_key", "TEXT NOT NULL DEFAULT ''")
        _add_column(conn, "land_rollbacks", "removed", "INTEGER NOT NULL DEFAULT 0")
        _add_column(conn, "land_rollbacks", "created_at", "TEXT NOT NULL DEFAULT ''")
        conn.execute(
            "UPDATE land_keys SET batch_key = 'FLOW-' || flow_id WHERE batch_key = ''"
        )
        conn.execute("UPDATE land_keys SET key_norm = UPPER(key) WHERE key_norm = ''")
        # 旧版半写入留下的变形钥匙（payload='partial' / 后缀 #half）不属于合法库存，
        # 迁移时清掉并记事件，否则会永久抬高库存、让账实对不上。
        ghosts = conn.execute(
            "SELECT flow_id, COUNT(*) AS c FROM land_keys"
            " WHERE payload = 'partial' OR LOWER(key) LIKE '%#half'"
            " GROUP BY flow_id"
        ).fetchall()
        if ghosts:
            now = _now()
            conn.execute(
                "DELETE FROM land_keys WHERE payload = 'partial' OR LOWER(key) LIKE '%#half'"
            )
            for row in ghosts:
                conn.execute(
                    "INSERT INTO land_events(batch_key, flow_id, kind, count, note, created_at)"
                    " VALUES (?,?,?,?,?,?)",
                    (
                        f"FLOW-{row['flow_id']}", int(row["flow_id"]), "cleanup",
                        int(row["c"]), "清理旧版半写入变形钥匙", now,
                    ),
                )
        _dedupe_key_norms(conn)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_land_keys_batch ON land_keys(batch_key)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_land_keys_flow ON land_keys(flow_id)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_land_keys_norm ON land_keys(key_norm)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_land_events_batch ON land_events(batch_key)"
        )
        conn.commit()
        _SCHEMA_READY.add(identity)


def inventory_count(conn, flow_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM land_keys WHERE flow_id=?", (flow_id,)
    ).fetchone()
    return int(row["c"])


def sync_inventory(conn, flow_id: int) -> int:
    """以库存（land_keys）为准，把列表 flows.landed 与状态对齐。"""
    count = inventory_count(conn, flow_id)
    flow = conn.execute("SELECT extracted FROM flows WHERE id=?", (flow_id,)).fetchone()
    if flow is None:
        return count
    if count > 0:
        conn.execute(
            "UPDATE flows SET landed=?, status=? WHERE id=?", (count, "已落库", flow_id)
        )
    else:
        status = "已抽取待落库" if int(flow["extracted"] or 0) > 0 else "待抽取"
        conn.execute(
            "UPDATE flows SET landed=0, status=? WHERE id=?", (status, flow_id)
        )
    return count


@contextmanager
def flow_guard(conn, flow_id: int):
    """同流的写事务护栏：与落库/回滚共用一把流锁，并在事务里串行。"""
    lock_key, lock = _flow_lock(flow_id)
    lock.acquire()
    try:
        _begin_immediate(conn)
        yield
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        lock.release()


def revoke_inventory(
    conn, flow_id: int, reason: str = "重新抽取，撤销旧批次库存", guard: bool = True
) -> int:
    """整笔撤销某流的落库库存，并记事件；用于重新抽取。"""
    ensure(conn)
    if not guard:
        return _revoke_locked(conn, flow_id, reason)
    with flow_guard(conn, flow_id):
        return _revoke_locked(conn, flow_id, reason)


def _revoke_locked(conn, flow_id: int, reason: str) -> int:
    now = _now()
    batch_keys = [
        row["batch_key"]
        for row in conn.execute(
            "SELECT DISTINCT batch_key FROM land_keys WHERE flow_id=?", (flow_id,)
        )
    ]
    removed = conn.execute("DELETE FROM land_keys WHERE flow_id=?", (flow_id,)).rowcount
    for batch_key in batch_keys:
        conn.execute(
            "UPDATE land_batches SET state='pending', written=0, rows=0,"
            " sample_keys='[]', last_verdict='pending', last_reason='', updated_at=?"
            " WHERE batch_key=?",
            (now, batch_key),
        )
    conn.execute(
        "INSERT INTO land_events(batch_key, flow_id, kind, count, note, created_at)"
        " VALUES (?,?,?,?,?,?)",
        (f"FLOW-{flow_id}", flow_id, "revoke", removed, reason, now),
    )
    sync_inventory(conn, flow_id)
    return removed


def _begin_immediate(conn) -> bool:
    """开启写事务；返回是否因等锁产生过争用（同进程或跨进程）。"""
    if conn.in_transaction:
        conn.commit()
    # 先短等探测是否有争抢，再退回长等，避免无谓的长阻塞。
    conn.execute("PRAGMA busy_timeout=50")
    waited = False
    for attempt in range(_BUSY_RETRIES):
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("PRAGMA busy_timeout=10000")
            return waited
        except sqlite3.OperationalError as exc:
            msg = str(exc).lower()
            if "locked" in msg or "busy" in msg:
                waited = True
                time.sleep(min(0.01 * (attempt + 1), 0.05))
                continue
            raise
    conn.execute("PRAGMA busy_timeout=30000")
    for attempt in range(3):
        try:
            conn.execute("BEGIN IMMEDIATE")
            return True
        except sqlite3.OperationalError as exc:
            if attempt == 2 or "lock" not in str(exc).lower():
                raise
    return True


def _batch_lock(batch_key: str) -> threading.Lock:
    with _LOCKS_GUARD:
        lock = _LOCKS.get(batch_key)
        if lock is None:
            lock = threading.Lock()
            _LOCKS[batch_key] = lock
        return lock


def _flow_lock(flow_id: int) -> tuple[str, threading.Lock]:
    """同一流的落库/回滚共用一把锁：同批或跨批都串行，互不双写。"""
    key = f"FLOW#{flow_id}"
    return key, _batch_lock(key)


def _enter(batch_key: str, check_window: bool = True) -> bool:
    """登记一次落库调用；返回是否判定为“后到争用”。

    判据之一成立即算后到：已有同批调用在做、拿到流锁前阻塞过（_acquire 返回），
    或同批刚被别的调用判定过（_RACE_WINDOW 窗口内）——单 worker 下并发请求会在
    事件循环里排队，等到执行时前一个已判定完，只有该窗口能识别出它是后到的。
    """
    now = time.monotonic()
    with _LOCKS_GUARD:
        inflight = _INFLIGHT.get(batch_key, 0)
        _INFLIGHT[batch_key] = inflight + 1
        if inflight > 0:
            return True
        if not check_window:
            return False
        last = _LAST_DECISION.get(batch_key)
        return bool(last and last[1] in _DECIDED and now - last[0] <= _RACE_WINDOW)


def _leave(batch_key: str) -> None:
    with _LOCKS_GUARD:
        left = _INFLIGHT.get(batch_key, 0) - 1
        if left > 0:
            _INFLIGHT[batch_key] = left
        else:
            _INFLIGHT.pop(batch_key, None)


def _mark_decided(batch_key: str, verdict: str) -> None:
    with _LOCKS_GUARD:
        _LAST_DECISION[batch_key] = (time.monotonic(), verdict)


def _acquire(lock: threading.Lock) -> bool:
    """非阻塞探测：立刻拿到 = 无争抢；拿不到 = 后到，需记争用。"""
    if lock.acquire(blocking=False):
        return False
    lock.acquire()
    return True


def _row_keys(conn, flow_id: int, batch_key: str, keys) -> list[tuple[str, str]]:
    """本批要落库的行钥匙，返回 [(展示 key, 判重 norm)]，批内按 norm 去重。"""
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    if keys:
        for raw in keys:
            key = normalize_key(raw)
            norm = norm_key(raw)
            if key and norm not in seen:
                seen.add(norm)
                out.append((key, norm))
        return out
    flow = conn.execute("SELECT extracted FROM flows WHERE id=?", (flow_id,)).fetchone()
    if flow is None:
        return []
    count = int(flow["extracted"] or 0)
    if count <= 0:
        # 未抽取就没有批次行；需要手工落单行时必须显式给出 keys。
        return []
    for i in range(1, count + 1):
        key = f"{batch_key}-{i:02d}"
        out.append((key, norm_key(key)))
    return out


def _batch_row(conn, batch_key: str):
    return conn.execute(
        "SELECT * FROM land_batches WHERE batch_key=?", (batch_key,)
    ).fetchone()


def _event(conn, batch_key: str, flow_id: int, kind: str, count: int, note: str, now: str) -> None:
    conn.execute(
        "INSERT INTO land_events(batch_key, flow_id, kind, count, note, created_at)"
        " VALUES (?,?,?,?,?,?)",
        (batch_key, flow_id, kind, count, note, now),
    )


def _upsert_batch(
    conn,
    batch_key: str,
    flow_id: int,
    state: str,
    rows: int,
    written: int,
    skipped_delta: int,
    contention_delta: int,
    verdict: str,
    reason: str,
    sample_keys: list[str],
    now: str,
) -> None:
    batch = _batch_row(conn, batch_key)
    sample = json.dumps(sample_keys[:_SAMPLE_LIMIT], ensure_ascii=False)
    if batch is None:
        conn.execute(
            "INSERT INTO land_batches(batch_key, flow_id, state, rows, written, skipped,"
            " contention, last_verdict, last_reason, sample_keys, created_at, updated_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                batch_key, flow_id, state, rows, written, skipped_delta,
                contention_delta, verdict, reason, sample, now, now,
            ),
        )
    else:
        conn.execute(
            "UPDATE land_batches SET state=?, rows=?, written=?, skipped=skipped+?,"
            " contention=contention+?, last_verdict=?, last_reason=?, sample_keys=?,"
            " updated_at=? WHERE batch_key=?",
            (
                state, rows, written, skipped_delta, contention_delta, verdict, reason,
                sample, now, batch_key,
            ),
        )


def _flow_row(conn, flow_id: int):
    return conn.execute(
        "SELECT id, title, code, status, extracted, landed FROM flows WHERE id=?", (flow_id,)
    ).fetchone()


def _flow_summary(conn, flow_id: int) -> dict:
    flow = _flow_row(conn, flow_id)
    inventory = inventory_count(conn, flow_id)
    landed = int(flow["landed"] or 0) if flow else 0
    return {
        "flow_id": flow_id,
        "status": flow["status"] if flow else "",
        "inventory": inventory,
        "list_landed": landed,
        "consistent": inventory == landed,
    }


def land_batch(conn, flow_id: int, batch_key=None, keys=None) -> dict:
    """整批原子落库：全落或全跳过，跳过计数，无半写入，同批串行。"""
    ensure(conn)
    flow = _flow_row(conn, flow_id)
    if flow is None:
        return {
            "ok": False,
            "action": "land",
            "flow_id": flow_id,
            "error": "流不存在",
            "strategy": STRATEGY,
        }
    batch_key = _batch_key(batch_key, flow_id)
    rows = _row_keys(conn, flow_id, batch_key, keys)
    total = len(rows)
    if total == 0:
        return {
            "ok": False,
            "action": "land",
            "batch_key": batch_key,
            "flow_id": flow_id,
            "error": "本批钥匙为空，无可落库数据",
            "verdict": "empty",
            "total": 0,
            "written": 0,
            "skipped": 0,
            "contended": False,
            "strategy": STRATEGY,
        }
    sample = [key for key, _norm in rows]

    lock_key, lock = _flow_lock(flow_id)
    contended = _enter(lock_key)
    try:
        contended = _acquire(lock) or contended
        contended = _begin_immediate(conn) or contended
        now = _now()
        batch = _batch_row(conn, batch_key)
        if batch is not None and int(batch["flow_id"]) != flow_id:
            conn.rollback()
            return {
                "ok": False,
                "action": "land",
                "batch_key": batch_key,
                "flow_id": flow_id,
                "error": f"批次 {batch_key} 已属于流 {batch['flow_id']}，不能跨流复用",
                "strategy": STRATEGY,
            }
        norms = [norm for _key, norm in rows]
        placeholders = ",".join("?" * total)
        collisions = [
            row["key_norm"]
            for row in conn.execute(
                f"SELECT key_norm FROM land_keys WHERE key_norm IN ({placeholders})", norms
            )
        ]

        if collisions:
            reason = "撞钥：" + "、".join(sorted(collisions)[:5])
            if len(collisions) > 5:
                reason += f" 等 {len(collisions)} 行"
            verdict = "skipped"
        else:
            reason = ""
            verdict = "landed"

        if verdict == "skipped":
            # 整批同一判定：只计数，零写入，不动 flows.landed。
            # 该批此前已落库且库存仍在时，保留 landed 状态（库存没变）。
            keys_in_batch = conn.execute(
                "SELECT COUNT(*) AS c FROM land_keys WHERE batch_key=?", (batch_key,)
            ).fetchone()["c"]
            state = "landed" if keys_in_batch else "skipped"
            is_contended = contended
            _upsert_batch(
                conn, batch_key, flow_id, state, total, int(keys_in_batch), total,
                int(is_contended), "skipped", reason, sample, now,
            )
            _event(conn, batch_key, flow_id, "skip", total, reason, now)
            if is_contended:
                _event(
                    conn, batch_key, flow_id, "contention", 1,
                    "后到争用：同批落库已串行化，未重复写入", now,
                )
            sync_inventory(conn, flow_id)
            summary = _flow_summary(conn, flow_id)
            batch = _batch_row(conn, batch_key)
            conn.commit()
            _mark_decided(lock_key, "skipped")
            return {
                "ok": True,
                "action": "land",
                "batch_key": batch_key,
                "flow_id": flow_id,
                "verdict": "skipped",
                "skip": True,
                "half": False,
                "reason": reason,
                "total": total,
                "written": 0,
                "skipped": total,
                "skipped_total": int(batch["skipped"]),
                "contention": int(batch["contention"]),
                "contended": is_contended,
                "inventory": summary["inventory"],
                "list_landed": summary["list_landed"],
                "consistent": summary["consistent"],
                "verdicts": [{"key": key, "verdict": "skipped"} for key, _n in rows],
                "strategy": STRATEGY,
                "state_label": _state_label(state),
            }

        # 无撞钥：整批写入，同一事务里落库 + 对齐库存。
        conn.executemany(
            "INSERT INTO land_keys(key, flow_id, batch_key, key_norm, payload, created_at)"
            " VALUES (?,?,?,?,?,?)",
            [(key, flow_id, batch_key, norm, "full", now) for key, norm in rows],
        )
        is_contended = contended
        _upsert_batch(
            conn, batch_key, flow_id, "landed", total, total, 0,
            int(is_contended), "landed", "", sample, now,
        )
        _event(conn, batch_key, flow_id, "land", total, "整批落库", now)
        if is_contended:
            _event(
                conn, batch_key, flow_id, "contention", 1,
                "后到争用：同批落库已串行化，未重复写入", now,
            )
        sync_inventory(conn, flow_id)
        summary = _flow_summary(conn, flow_id)
        batch = _batch_row(conn, batch_key)
        conn.commit()
        _mark_decided(lock_key, "landed")
        return {
            "ok": True,
            "action": "land",
            "batch_key": batch_key,
            "flow_id": flow_id,
            "verdict": "landed",
            "skip": False,
            "half": False,
            "reason": "",
            "total": total,
            "written": total,
            "skipped": 0,
            "skipped_total": int(batch["skipped"]),
            "contention": int(batch["contention"]),
            "contended": is_contended,
            "inventory": summary["inventory"],
            "list_landed": summary["list_landed"],
            "consistent": summary["consistent"],
            "verdicts": [{"key": key, "verdict": "landed"} for key, _n in rows],
            "strategy": STRATEGY,
            "state_label": _state_label("landed"),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        lock.release()
        _leave(lock_key)


def _rollback_batches(conn, flow_id: int, batch_key=None) -> list[str]:
    if batch_key:
        return [_batch_key(batch_key, flow_id)]
    keys = [
        row["batch_key"]
        for row in conn.execute(
            "SELECT DISTINCT batch_key FROM land_keys WHERE flow_id=?", (flow_id,)
        )
    ]
    for row in conn.execute(
        "SELECT batch_key FROM land_batches WHERE flow_id=?"
        " AND state='landed' AND batch_key NOT IN (SELECT batch_key FROM land_keys)",
        (flow_id,),
    ):
        if row["batch_key"] not in keys:
            keys.append(row["batch_key"])
    return keys


def rollback_batch(conn, flow_id: int, batch_key=None) -> dict:
    """原子整笔回滚：一次撤销整批（默认该流全部库存），回滚后列表与库存一致。"""
    ensure(conn)
    flow = _flow_row(conn, flow_id)
    if flow is None:
        return {
            "ok": False,
            "action": "rollback",
            "flow_id": flow_id,
            "error": "流不存在",
            "strategy": STRATEGY,
        }
    targets = _rollback_batches(conn, flow_id, batch_key)
    primary = _batch_key(batch_key, flow_id) if batch_key else f"FLOW-{flow_id}"
    lock_key, lock = _flow_lock(flow_id)
    contended = _enter(lock_key, check_window=False)
    try:
        contended = _acquire(lock) or contended
        contended = _begin_immediate(conn) or contended
        now = _now()
        batch = _batch_row(conn, batch_key)
        if batch is not None and int(batch["flow_id"]) != flow_id:
            conn.rollback()
            return {
                "ok": False,
                "action": "rollback",
                "batch_key": batch_key,
                "flow_id": flow_id,
                "error": f"批次 {batch_key} 属于流 {batch['flow_id']}，不能跨流回滚",
                "strategy": STRATEGY,
            }
        per_batch = {
            row["batch_key"]: int(row["c"])
            for row in conn.execute(
                "SELECT batch_key, COUNT(*) AS c FROM land_keys WHERE flow_id=?"
                " GROUP BY batch_key",
                (flow_id,),
            )
        }
        if batch_key:
            removed_scope: list[str] = [primary]
            removed = conn.execute(
                "DELETE FROM land_keys WHERE flow_id=? AND batch_key=?",
                (flow_id, primary),
            ).rowcount
        else:
            removed_scope = list(per_batch)
            removed = conn.execute(
                "DELETE FROM land_keys WHERE flow_id=?", (flow_id,)
            ).rowcount
        known = {
            row["batch_key"]: dict(row)
            for row in conn.execute(
                "SELECT * FROM land_batches WHERE flow_id=?", (flow_id,)
            )
        }
        removable = [
            key for key in targets
            if per_batch.get(key, 0) > 0
            or known.get(key, {}).get("state") == "landed"
        ]
        if removed == 0 and not removable:
            conn.commit()
            return {
                "ok": False,
                "action": "rollback",
                "batch_key": primary,
                "flow_id": flow_id,
                "error": "无可回滚批次",
                "removed": 0,
                "rolled": False,
                "strategy": STRATEGY,
            }
        for key in targets:
            if key in known:
                conn.execute(
                    "UPDATE land_batches SET state='rolled_back', written=0,"
                    " contention=contention+?, last_verdict='rolled_back',"
                    " last_reason='', sample_keys='[]', updated_at=?"
                    " WHERE batch_key=?",
                    (int(contended), now, key),
                )
        for key in removed_scope:
            _event(
                conn, key, flow_id, "rollback", per_batch.get(key, 0), "原子整笔回滚", now
            )
        for key in targets:
            if key not in removed_scope:
                _event(conn, key, flow_id, "rollback", 0, "原子整笔回滚", now)
        conn.execute(
            "INSERT INTO land_rollbacks(flow_id, batch_key, note, removed, done, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (flow_id, primary, "原子整笔回滚", removed, 1, now),
        )
        if contended:
            _event(
                conn, primary, flow_id, "contention", 1,
                "后到争用：同批落库已串行化，未重复写入", now,
            )
        sync_inventory(conn, flow_id)
        summary = _flow_summary(conn, flow_id)
        conn.commit()
        return {
            "ok": True,
            "action": "rollback",
            "batch_key": primary,
            "flow_id": flow_id,
            "rolled": True,
            "removed": removed,
            "inventory": summary["inventory"],
            "list_landed": summary["list_landed"],
            "consistent": summary["consistent"],
            "contended": contended,
            "state_label": _state_label("rolled_back"),
            "strategy": STRATEGY,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        lock.release()
        _leave(lock_key)


def mutate_action(conn, body) -> dict:
    action = str(body.get("action") or "land").strip().lower()
    try:
        flow_id = int(body.get("flow_id") or 1)
    except (TypeError, ValueError):
        return {"ok": False, "error": "flow_id 非法", "strategy": STRATEGY}
    if action == "rollback":
        raw_batch = body.get("batch_key")
        return rollback_batch(conn, flow_id, raw_batch if raw_batch else None)
    keys = body.get("keys")
    if isinstance(keys, str):
        keys = [keys]
    elif not isinstance(keys, list):
        keys = None
    return land_batch(conn, flow_id, body.get("batch_key"), keys)


def _state_label(state: str) -> str:
    return {
        "landed": "整批已落库",
        "skipped": "整批跳过（零写入）",
        "rolled_back": "已整笔回滚",
        "pending": "待落库",
    }.get(state, state)


def _batch_view(conn, flow, batch: dict, batch_keys: list[dict]) -> dict:
    state = batch["state"]
    events = [
        dict(row)
        for row in conn.execute(
            "SELECT kind, count, note, created_at FROM land_events"
            " WHERE batch_key=? ORDER BY id DESC LIMIT 8",
            (batch["batch_key"],),
        )
    ]
    if state in ("landed", "rolled_back"):
        keys = [
            {"key": row["key"], "verdict": "landed", "state": state}
            for row in batch_keys
        ]
    else:
        try:
            sample = json.loads(batch.get("sample_keys") or "[]")
        except ValueError:
            sample = []
        keys = [
            {"key": key, "verdict": "skipped", "state": state} for key in sample
        ]
    written = int(batch["written"])
    inventory = len(batch_keys)
    return {
        "batch_key": batch["batch_key"],
        "flow_id": flow["id"],
        "flow_title": flow["title"],
        "flow_code": flow["code"],
        "state": state,
        "state_label": _state_label(state),
        "rows": int(batch["rows"]),
        "written": written,
        "skipped": int(batch["skipped"]),
        "contention": int(batch["contention"]),
        "last_verdict": batch["last_verdict"],
        "last_reason": batch["last_reason"],
        "inventory": inventory,
        "list_landed": int(flow["landed"] or 0),
        "half_write": inventory != written,
        "keys": keys,
        "events": events,
    }


def panel_payload(conn, args) -> dict:
    ensure(conn)
    flows = [
        dict(row)
        for row in conn.execute(
            "SELECT id, title, code, status, extracted, landed FROM flows ORDER BY id"
        )
    ]
    all_keys = [
        dict(row)
        for row in conn.execute(
            "SELECT key, flow_id, batch_key, payload, created_at FROM land_keys ORDER BY key"
        )
    ]
    keys_by_batch: dict[str, list[dict]] = {}
    keys_by_flow: dict[int, int] = {}
    for row in all_keys:
        keys_by_batch.setdefault(row["batch_key"] or "", []).append(row)
        keys_by_flow[row["flow_id"]] = keys_by_flow.get(row["flow_id"], 0) + 1

    batch_rows = {
        row["batch_key"]: dict(row)
        for row in conn.execute("SELECT * FROM land_batches")
    }
    batches: list[dict] = []
    for flow in flows:
        owned = sorted(
            (b for b in batch_rows.values() if int(b["flow_id"]) == int(flow["id"])),
            key=lambda b: b["batch_key"],
        )
        for batch in owned:
            batches.append(
                _batch_view(conn, flow, batch, keys_by_batch.get(batch["batch_key"], []))
            )
        default_key = _batch_key(None, flow["id"])
        if default_key not in batch_rows and int(flow["extracted"] or 0) > 0:
            pending = {
                "batch_key": default_key,
                "state": "pending",
                "rows": int(flow["extracted"] or 0),
                "written": 0,
                "skipped": 0,
                "contention": 0,
                "last_verdict": "pending",
                "last_reason": "",
                "sample_keys": "[]",
            }
            batches.append(_batch_view(conn, flow, pending, []))

    rollbacks = [
        dict(row)
        for row in conn.execute(
            "SELECT id, flow_id, batch_key, note, removed, done, created_at"
            " FROM land_rollbacks ORDER BY id DESC LIMIT 10"
        )
    ]

    inventory = []
    mismatches = []
    for flow in flows:
        counted = int(keys_by_flow.get(flow["id"], 0))
        landed = int(flow["landed"] or 0)
        row = {
            "flow_id": flow["id"],
            "title": flow["title"],
            "code": flow["code"],
            "status": flow["status"],
            "list_landed": landed,
            "inventory": counted,
            "consistent": counted == landed,
        }
        inventory.append(row)
        if not row["consistent"]:
            mismatches.append(
                {"flow_id": flow["id"], "list_landed": landed, "inventory": counted}
            )

    half_writes = [b["batch_key"] for b in batches if b["half_write"]]
    return {
        "strategy": STRATEGY,
        "strategy_name": STRATEGY["title"],
        "list_label": "落库列表",
        "batches": batches,
        "keys": all_keys,
        "rollbacks": rollbacks,
        "inventory": inventory,
        "consistency": {
            "ok": not mismatches and not half_writes,
            "mismatches": mismatches,
            "half_writes": half_writes,
        },
        "skip_total": sum(b["skipped"] for b in batches),
        "contention_total": sum(b["contention"] for b in batches),
        "landed_total": sum(keys_by_flow.values()),
        "list_landed_total": sum(int(f["landed"] or 0) for f in flows),
    }
