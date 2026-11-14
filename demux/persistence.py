"""拆样任务的 SQLite 持久层。

任务状态机：``pending``（已登记，工作线程尚未开始）→ ``running`` →
``completed`` / ``failed``。进程崩溃只会停在 ``running``，重启后用相同
请求号与样本表重新提交即可从检查点续跑。

断点语义：``processed_pairs`` 表示**已稳定提交**的配对数；输出分块
（chunk）按 ``[start_seq, end_seq)`` 命名且内容不可变，每个分块在
fsync 落盘后才随同一事务登记，因此：

* 已登记的分块绝不会重写，重启后不重复处理已提交读段；
* 未登记的分块在恢复时整体清理，不会有残留文件混入输出。
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS demux_tasks (
    request_id       TEXT PRIMARY KEY,
    run_id           TEXT NOT NULL,
    sample_sheet     TEXT NOT NULL,          -- 规范化后的样本表 JSON
    status           TEXT NOT NULL,          -- pending|running|completed|failed
    total_pairs      INTEGER NOT NULL DEFAULT 0,
    processed_pairs  INTEGER NOT NULL DEFAULT 0,
    output_dir       TEXT,
    error            TEXT,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS demux_buckets (
    request_id   TEXT NOT NULL,
    bucket       TEXT NOT NULL,              -- 样本名或 'unassigned'
    pair_count   INTEGER NOT NULL DEFAULT 0, -- 已提交分块中的累计对数
    PRIMARY KEY (request_id, bucket)
);
CREATE TABLE IF NOT EXISTS demux_chunks (
    request_id   TEXT NOT NULL,
    bucket       TEXT NOT NULL,
    side         INTEGER NOT NULL,          -- 1=R1, 2=R2
    start_seq    INTEGER NOT NULL,           -- 全局配对序号区间 [start, end)
    end_seq      INTEGER NOT NULL,
    relpath      TEXT NOT NULL,              -- 相对任务目录的分块文件路径
    byte_size    INTEGER NOT NULL,
    PRIMARY KEY (request_id, bucket, side, start_seq)
);
"""

# 与工作线程约定的状态取值
PENDING = "pending"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"


def connect(db_path: str) -> sqlite3.Connection:
    parent = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def get_task(conn: sqlite3.Connection, request_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM demux_tasks WHERE request_id=?", (request_id,)
    ).fetchone()


def list_tasks(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM demux_tasks ORDER BY created_at").fetchall()


def create_task(
    conn: sqlite3.Connection,
    request_id: str,
    run_id: str,
    sample_sheet_json: str,
    now: float,
) -> None:
    conn.execute(
        "INSERT INTO demux_tasks(request_id, run_id, sample_sheet, status,"
        " created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        (request_id, run_id, sample_sheet_json, PENDING, now, now),
    )
    conn.commit()


def mark_running(
    conn: sqlite3.Connection,
    request_id: str,
    total_pairs: int,
    now: float,
) -> None:
    conn.execute(
        "UPDATE demux_tasks SET status=?, total_pairs=?, updated_at=?"
        " WHERE request_id=?",
        (RUNNING, total_pairs, now, request_id),
    )
    conn.commit()


def commit_checkpoint(
    conn: sqlite3.Connection,
    request_id: str,
    processed_pairs: int,
    bucket_deltas: dict[str, int],
    chunk_rows: list[tuple[str, int, int, str, int]],
    now: float,
) -> None:
    """单事务登记一批已 fsync 的分块并前推进度与桶计数。

    任何一步失败整体回滚：分块文件即使已落盘也不会被当作已完成，
    恢复时作为未登记残留清理，保证"不混入残留文件"。
    """
    with conn:
        for bucket, delta in bucket_deltas.items():
            conn.execute(
                "INSERT INTO demux_buckets(request_id, bucket, pair_count)"
                " VALUES (?, ?, ?) ON CONFLICT(request_id, bucket) DO UPDATE SET"
                " pair_count = pair_count + excluded.pair_count",
                (request_id, bucket, delta),
            )
        for bucket, side, start_seq, end_seq, relpath, byte_size in chunk_rows:
            conn.execute(
                "INSERT INTO demux_chunks(request_id, bucket, side, start_seq, end_seq,"
                " relpath, byte_size) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (request_id, bucket, side, start_seq, end_seq, relpath, byte_size),
            )
        conn.execute(
            "UPDATE demux_tasks SET processed_pairs=?, updated_at=? WHERE request_id=?",
            (processed_pairs, now, request_id),
        )


def list_chunks(conn: sqlite3.Connection, request_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT bucket, side, start_seq, end_seq, relpath, byte_size FROM demux_chunks"
        " WHERE request_id=? ORDER BY bucket, side, start_seq",
        (request_id,),
    ).fetchall()


def list_buckets(conn: sqlite3.Connection, request_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT bucket, pair_count FROM demux_buckets WHERE request_id=?"
        " ORDER BY bucket",
        (request_id,),
    ).fetchall()


def mark_completed(
    conn: sqlite3.Connection, request_id: str, output_dir: str, now: float
) -> None:
    conn.execute(
        "UPDATE demux_tasks SET status=?, output_dir=?, error=NULL, updated_at=?"
        " WHERE request_id=?",
        (COMPLETED, output_dir, now, request_id),
    )
    conn.commit()


def mark_failed(
    conn: sqlite3.Connection, request_id: str, error: dict, now: float
) -> None:
    conn.execute(
        "UPDATE demux_tasks SET status=?, error=?, updated_at=? WHERE request_id=?",
        (FAILED, json.dumps(error, ensure_ascii=False), now, request_id),
    )
    conn.commit()


def snapshot(row: sqlite3.Row, buckets: list[sqlite3.Row]) -> dict[str, Any]:
    error = json.loads(row["error"]) if row["error"] else None
    return {
        "request_id": row["request_id"],
        "run_id": row["run_id"],
        "sample_sheet": json.loads(row["sample_sheet"]),
        "status": row["status"],
        "total_pairs": row["total_pairs"],
        "processed_pairs": row["processed_pairs"],
        "output_dir": row["output_dir"],
        "error": error,
        "buckets": [
            {"bucket": b["bucket"], "pair_count": b["pair_count"]} for b in buckets
        ],
    }
