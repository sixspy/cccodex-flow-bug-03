import sqlite3
from contextlib import contextmanager
from pathlib import Path

from app import idempo
from app.config import Config

SEEDS = [
    ("目录扫描流", "scan_flow", "待抽取", "https://mock.local/scan"),
    ("列表翻页流", "page_flow", "待抽取", "https://mock.local/list"),
    ("附件下载流", "file_flow", "待抽取", "file://drops/pack"),
    ("变更监听流", "watch_flow", "待抽取", "https://mock.local/watch"),
]

def connect(db_path=None):
    path = db_path or Config.database()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn

@contextmanager
def get_db(db_path=None):
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db(db_path=None):
    with get_db(db_path) as conn:
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS flows (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              title TEXT NOT NULL,
              code TEXT NOT NULL UNIQUE,
              status TEXT NOT NULL,
              source_uri TEXT NOT NULL,
              extracted INTEGER NOT NULL DEFAULT 0,
              landed INTEGER NOT NULL DEFAULT 0
            );
        ''')
        if conn.execute("SELECT COUNT(*) c FROM flows").fetchone()["c"] == 0:
            conn.executemany(
                "INSERT INTO flows (title, code, status, source_uri) VALUES (?,?,?,?)",
                SEEDS,
            )

def list_flows(conn):
    return [dict(r) for r in conn.execute(
        "SELECT id, title, code, status, source_uri, extracted, landed FROM flows ORDER BY id"
    )]

def get_flow(conn, flow_id: int):
    row = conn.execute(
        "SELECT id, title, code, status, source_uri, extracted, landed FROM flows WHERE id=?",
        (flow_id,),
    ).fetchone()
    return dict(row) if row else None

def run_extract(conn, flow_id: int):
    item = get_flow(conn, flow_id)
    if not item:
        return None
    if item["status"] in ("待抽取", "抽取失败"):
        n = 15 + (flow_id * 4) % 10
        # 重新抽取即视为新批次：旧批次库存整笔撤销，避免列表与库存分叉。
        # 撤销与状态更新同事务、同流锁，避免与并发落库交错。
        with idempo.flow_guard(conn, flow_id):
            idempo.revoke_inventory(conn, flow_id, guard=False)
            conn.execute(
                "UPDATE flows SET status=?, extracted=?, landed=0 WHERE id=?",
                ("已抽取待落库", n, flow_id),
            )
    return get_flow(conn, flow_id)

def run_land(conn, flow_id: int):
    """落库统一走整批原子口径，列表与库存同事务对齐。

    已落库的批次再次落库会走撞钥判定：整批跳过并计数，不重复写入。
    """
    item = get_flow(conn, flow_id)
    if not item:
        return None
    if item["status"] in ("已抽取待落库", "已落库"):
        idempo.land_batch(conn, flow_id)
    return get_flow(conn, flow_id)
