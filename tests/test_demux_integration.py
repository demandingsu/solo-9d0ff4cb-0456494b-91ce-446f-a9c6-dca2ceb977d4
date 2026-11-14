"""拆样端到端集成测试：真实 uvicorn 子进程、HTTP 提交、崩溃注入与重启恢复。

覆盖：
* 精确/容错（每端 1 替换）/不足 4 碱基/无匹配/歧义 的归属规则与桶计数；
* 桶内保留原始记录与原始顺序；
* 同参复用、异参 409、并发提交只执行一次；
* 未完成下载门控（409）；
* 写盘失败后进程退出、重启按原任务号续跑（不重复读段、不混残留）；
* "文件完成而状态未保存"崩溃窗口的恢复（不重读段，补状态即可下载）；
* 已完成任务在重启后仍可下载。
"""
import os
import time

import httpx
import pytest

from tests.conftest import Server

SHEET = [
    {"sample_name": "S1", "barcode_r1": "AAAA", "barcode_r2": "CCCC"},
    {"sample_name": "S2", "barcode_r1": "GGGG", "barcode_r2": "TTTT"},
    # 与 S1 的 R1 仅差 1、R2 相同：用于制造歧义桶
    {"sample_name": "S3", "barcode_r1": "AAAG", "barcode_r2": "CCCC"},
]

# (R1 序列, R2 序列, 期望桶, 期望原因)
CASES = [
    ("AAAATTTT", "CCCCAAAA", "S1", "exact"),            # 0 精确 S1（不被 S3 邻域稀释）
    ("GGGGTTTT", "TTTTAAAA", "S2", "exact"),            # 1 精确 S2
    ("AAGATTTT", "CCCTAAAA", "S1", "one-sub-each"),     # 2 两端各 1 替换 → S1
    ("GCGGTTTT", "TTATAAAA", "S2", "one-sub-each"),     # 3 两端各 1 替换 → S2
    ("AATTTTTT", "CCCCAAAA", "unassigned", "no-match"),  # 4 R1 与所有条码距离 >=2
    ("AAAATTTT", "GGGGAAAA", "unassigned", "no-match"), # 5 R2 与任一条码距离 >=2
    ("ACG", "CCC", "unassigned", "short"),              # 6 两端不足 4 碱基
    ("AAAATTTT", "CCCCAAAA", "S1", "exact-priority"),   # 7 精确 S1：S3 邻域不构成歧义
    ("AAAGTTTT", "CCCCAAAA", "S3", "exact-priority"),   # 8 精确 S3：S1 邻域不构成歧义
    ("GGGGTTTT", "GTTTAAAA", "S2", "r2-one-sub"),       # 9 R2 1 替换 → S2
    # 真正的歧义：R1=AAAT 距 AAAA/AAAG 各 1，R2=CCCA 距 CCCC 恰 1，
    # 两端候选交集同时含 S1、S3 → 归未分配
    ("AAATTTTT", "CCCAAAAA", "unassigned", "ambiguous"),
]


def _record(name: str, side: int, seq: str) -> bytes:
    qual = "I" * len(seq)
    return f"@{name}/{side}\n{seq}\n+\n{qual}\n".encode()


def _collector_env(meta):
    return {
        "COLLECTOR_DB": meta["db_path"],
        "DEMUX_DB": meta["demux_db"],
        "DEMUX_OUTPUT": meta["demux_output"],
        "DEMUX_CHUNK_PAIRS": "2",
    }


def _write_completed_run(meta, run_id: str, cases=CASES):
    """直接在采集库构造一个 status=completed 的运行及其配对读段。"""
    import sqlite3

    rdir = os.path.join(meta["relay_data"], run_id)
    os.makedirs(rdir, exist_ok=True)
    conn = sqlite3.connect(meta["db_path"])
    try:
        now = time.time()
        conn.execute(
            "INSERT INTO runs(run_id, base_url, status, r1_offset, r2_offset,"
            " pairs, created_at, updated_at) VALUES (?, ?, 'completed', 0, 0, 0, ?, ?)",
            (run_id, f"file://{rdir}", now, now),
        )
        for seq, (s1, s2, _bucket, _reason) in enumerate(cases):
            name = f"read{seq:04d}"
            conn.execute(
                "INSERT INTO pairs(run_id, seq, name, r1_record, r2_record)"
                " VALUES (?, ?, ?, ?, ?)",
                (run_id, seq, name, _record(name, 1, s1), _record(name, 2, s2)),
            )
        conn.execute(
            "UPDATE runs SET pairs=?, r1_offset=?, r2_offset=? WHERE run_id=?",
            (len(cases), 1000, 1000, run_id),
        )
        conn.commit()
    finally:
        conn.close()


def _poll_task(client, base, request_id, timeout=30, expect="completed",
               allow_recovering_failed=False):
    """轮询任务终态。

    ``allow_recovering_failed`` 用于故障恢复场景：重试请求发出的瞬间，
    工作线程可能尚未把旧的 failed 状态翻成 running，轮询不要在这个
    短暂快照上提前判失败。
    """
    deadline = time.time() + timeout
    last = None
    failed_seen_at = None
    while time.time() < deadline:
        r = client.get(f"{base}/demux/tasks/{request_id}")
        last = r.json()
        if last["status"] == "completed":
            assert expect == "completed", last
            return last
        if last["status"] == "failed":
            if not allow_recovering_failed:
                assert last["status"] == expect, last
                return last
            if failed_seen_at is None:
                failed_seen_at = time.time()
            elif time.time() - failed_seen_at > 5:
                raise AssertionError(f"任务持续 failed 未恢复: {last}")
        time.sleep(0.1)
    raise AssertionError(f"任务未在 {timeout}s 内结束: {last}")


def _bucket_map(snap):
    return {b["bucket"]: b["pair_count"] for b in snap["buckets"]}


@pytest.fixture()
def isolated_collector(tmp_path):
    """故障注入会 os._exit 杀死进程，必须使用与会话 fixture 隔离的临时目录。"""
    meta = {
        "db_path": str(tmp_path / "collector.db"),
        "demux_db": str(tmp_path / "demux.db"),
        "demux_output": str(tmp_path / "demux"),
        "relay_data": str(tmp_path / "relay"),
    }
    os.makedirs(meta["relay_data"], exist_ok=True)
    srv = Server("collector.app:app", _collector_env(meta))
    srv.start()
    try:
        yield srv, meta
    finally:
        srv.stop()


def test_demux_assignment_rules_and_counts(servers, client):
    _relay, collector, meta = servers
    run_id = "dm-rules"
    _write_completed_run(meta, run_id)

    r = client.post(f"{collector}/demux/tasks", json={
        "request_id": "req-rules", "run_id": run_id, "sample_sheet": SHEET,
    })
    assert r.status_code == 202, r.text
    snap = _poll_task(client, collector, "req-rules")
    counts = _bucket_map(snap)
    # S1: 0,2,7  S2: 1,3,9  S3: 8  unassigned: 4,5,6,10
    assert counts == {"S1": 3, "S2": 3, "S3": 1, "unassigned": 4}
    assert snap["total_pairs"] == snap["processed_pairs"] == 11

    # 未分配桶与各样本桶都能下载，且记录顺序与源顺序一致
    r1 = client.get(f"{collector}/demux/tasks/req-rules/download/unassigned/R1").text
    names = [line[1:].split("/")[0] for line in r1.splitlines() if line.startswith("@")]
    assert names == ["read0004", "read0005", "read0006", "read0010"]

    s2_r1 = client.get(f"{collector}/demux/tasks/req-rules/download/S2/R1").text
    s2_r2 = client.get(f"{collector}/demux/tasks/req-rules/download/S2/R2").text
    s2_names_r1 = [l[1:].split("/")[0] for l in s2_r1.splitlines() if l.startswith("@")]
    s2_names_r2 = [l[1:].split("/")[0] for l in s2_r2.splitlines() if l.startswith("@")]
    assert s2_names_r1 == s2_names_r2 == ["read0001", "read0003", "read0009"]
    # 原始记录四行结构完整
    assert all(len(block) == 4 for block in
               [s2_r1.splitlines()[i:i + 4] for i in range(0, len(s2_r1.splitlines()), 4)])

    # 精确优先：第 7、8 对分别归属 S1、S3 而非歧义桶
    s1_r1 = client.get(f"{collector}/demux/tasks/req-rules/download/S1/R1").text
    s3_r1 = client.get(f"{collector}/demux/tasks/req-rules/download/S3/R1").text
    assert "read0007" in s1_r1 and "read0008" in s3_r1


def test_download_gated_until_completed(servers, client):
    _relay, collector, meta = servers
    run_id = "dm-gate"
    _write_completed_run(meta, run_id, cases=CASES[:4])
    # 不存在的任务 → 404
    assert client.get(f"{collector}/demux/tasks/nope/download/S1/R1").status_code == 404
    r = client.post(f"{collector}/demux/tasks", json={
        "request_id": "req-gate", "run_id": run_id, "sample_sheet": SHEET,
    })
    assert r.status_code == 202
    # 完成前任何下载都 409（极快任务可能已完成，则跳过该断言窗口）
    snap = client.get(f"{collector}/demux/tasks/req-gate").json()
    if snap["status"] != "completed":
        got = client.get(f"{collector}/demux/tasks/req-gate/download/S1/R1")
        assert got.status_code == 409
    _poll_task(client, collector, "req-gate")
    assert client.get(
        f"{collector}/demux/tasks/req-gate/download/S1/R3"
    ).status_code == 404


def test_same_params_reuse_conflict_and_concurrency(servers, client):
    _relay, collector, meta = servers
    run_id = "dm-reuse"
    _write_completed_run(meta, run_id, cases=CASES[:6])
    body = {"request_id": "req-reuse", "run_id": run_id, "sample_sheet": SHEET}

    r1 = client.post(f"{collector}/demux/tasks", json=body)
    assert r1.status_code == 202 and r1.json().get("reused") is False
    # 同参重复提交：复用，不再起第二个工作线程
    r2 = client.post(f"{collector}/demux/tasks", json=body)
    assert r2.status_code == 202 and r2.json().get("reused") is True

    # 异参冲突：换样本表 / 换运行号都是 409
    other_sheet = [SHEET[0], SHEET[1]]
    r = client.post(f"{collector}/demux/tasks", json={
        "request_id": "req-reuse", "run_id": run_id, "sample_sheet": other_sheet,
    })
    assert r.status_code == 409
    r = client.post(f"{collector}/demux/tasks", json={
        "request_id": "req-reuse", "run_id": "other-run", "sample_sheet": SHEET,
    })
    assert r.status_code == 409

    snap = _poll_task(client, collector, "req-reuse")
    counts = _bucket_map(snap)
    # 前 6 对：S1=0,2  S2=1,3  unassigned=4,5（无读段进入 S3，故不出现在桶列表）
    assert counts.get("S1") == 2 and counts.get("S2") == 2
    assert counts.get("unassigned") == 2 and counts.get("S3", 0) == 0
    assert sum(counts.values()) == 6

    # 已完成任务再次同参提交仍为复用，且可继续下载
    r = client.post(f"{collector}/demux/tasks", json=body)
    assert r.json()["status"] == "completed" and r.json().get("reused") is True
    assert client.get(
        f"{collector}/demux/tasks/req-reuse/download/S1/R1"
    ).status_code == 200


def test_concurrent_submits_execute_once(servers, client):
    _relay, collector, meta = servers
    run_id = "dm-concurrent"
    _write_completed_run(meta, run_id, cases=CASES[:8])
    body = {"request_id": "req-concurrent", "run_id": run_id, "sample_sheet": SHEET}
    # 并发提交同一请求号
    results = []
    with httpx.Client(timeout=30) as c:
        for _ in range(5):
            results.append(c.post(f"{collector}/demux/tasks", json=body))
    codes = [r.status_code for r in results]
    assert codes == [202] * 5
    created = [r.json().get("reused") for r in results]
    assert created.count(False) == 1 and created.count(True) == 4
    snap = _poll_task(client, collector, "req-concurrent")
    # 只执行一次：桶计数之和恰好等于总对数，没有重复写
    assert sum(b["pair_count"] for b in snap["buckets"]) == snap["total_pairs"] == 8
    # 前 8 对分布：S1=0,2,7（3 条），S2=1,3（2 条），unassigned=4,5,6（3 条），S3 无读段
    counts = _bucket_map(snap)
    assert counts.get("S1") == 3 and counts.get("S2") == 2
    assert counts.get("unassigned") == 3 and counts.get("S3", 0) == 0


def test_submit_requires_completed_run(servers, client):
    _relay, collector, _meta = servers
    # 运行不存在
    r = client.post(f"{collector}/demux/tasks", json={
        "request_id": "req-bad-run", "run_id": "ghost", "sample_sheet": SHEET,
    })
    assert r.status_code == 422


def test_invalid_sample_sheet_rejected(servers, client):
    _relay, collector, meta = servers
    run_id = "dm-bad-sheet"
    _write_completed_run(meta, run_id, cases=CASES[:2])
    base = {"request_id": "req-bad-sheet", "run_id": run_id}
    # 条码不是 4 位
    r = client.post(f"{collector}/demux/tasks", json={
        **base, "sample_sheet": [
            {"sample_name": "S1", "barcode_r1": "AAA", "barcode_r2": "CCCC"},
        ],
    })
    assert r.status_code == 422
    # 非法碱基
    r = client.post(f"{collector}/demux/tasks", json={
        **base, "sample_sheet": [
            {"sample_name": "S1", "barcode_r1": "AAAN", "barcode_r2": "CCCC"},
        ],
    })
    assert r.status_code == 422
    # 保留桶名
    r = client.post(f"{collector}/demux/tasks", json={
        **base, "sample_sheet": [
            {"sample_name": "unassigned", "barcode_r1": "AAAA", "barcode_r2": "CCCC"},
        ],
    })
    assert r.status_code == 422
    # 任务未被创建
    assert client.get(f"{collector}/demux/tasks/req-bad-sheet").status_code == 404


def _wait_exit(srv, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if srv.proc.poll() is not None:
            return srv.proc.poll()
        time.sleep(0.1)
    raise AssertionError("进程未在预期时间内退出")


def _restart(srv):
    # 保持原端口：旧进程已终止，SO_REUSEADDR 下新进程可立即绑定，
    # 这样测试 client 的 base_url 无需变化
    srv.restart(new_port=False)


def test_disk_failure_then_restart_resumes_without_duplication(isolated_collector, client):
    srv, meta = isolated_collector
    run_id = "dm-fault"
    _write_completed_run(meta, run_id)

    # 实例 A：注入故障，处理到第 3 对后写盘失败并退出
    r = client.post(f"{srv.base_url}/demux/tasks", json={
        "request_id": "req-fault", "run_id": run_id, "sample_sheet": SHEET,
        "fault_after_pairs": 3,
    })
    assert r.status_code == 202
    code = _wait_exit(srv)
    assert code != 0, "故障注入后进程应非零退出"

    # 落库状态为 failed；检查点停在批次边界（批次大小 2，故障点 3 → 已提交 2 对）
    import sqlite3
    conn = sqlite3.connect(meta["demux_db"])
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM demux_tasks WHERE request_id='req-fault'").fetchone()
    chunks = conn.execute(
        "SELECT COUNT(*) AS n FROM demux_chunks WHERE request_id='req-fault'"
    ).fetchone()
    conn.close()
    assert row["status"] == "failed" and row["processed_pairs"] == 2
    assert chunks["n"] > 0

    # 全新进程 B，相同请求号 + 相同样本表重试（持久卷与 DB 不变）
    _restart(srv)
    r = client.post(f"{srv.base_url}/demux/tasks", json={
        "request_id": "req-fault", "run_id": run_id, "sample_sheet": SHEET,
    })
    assert r.status_code == 202
    snap = _poll_task(client, srv.base_url, "req-fault",
                      allow_recovering_failed=True)
    counts = _bucket_map(snap)
    assert counts == {"S1": 3, "S2": 3, "S3": 1, "unassigned": 4}
    assert snap["processed_pairs"] == 11  # 从检查点续到末尾，没有重置为 0 重跑
    # 下载内容与源顺序一致，且没有重复读段
    r1 = client.get(f"{srv.base_url}/demux/tasks/req-fault/download/unassigned/R1").text
    names = [l[1:].split("/")[0] for l in r1.splitlines() if l.startswith("@")]
    assert names == ["read0004", "read0005", "read0006", "read0010"]
    assert sum(b["pair_count"] for b in snap["buckets"]) == 11


def test_corrupted_chunks_trigger_clean_restart(isolated_collector, client):
    """已登记分块与磁盘不一致（模拟残留/损坏）时，清空后从头重跑且结果正确。"""
    import sqlite3
    srv, meta = isolated_collector
    run_id = "dm-corrupt"
    _write_completed_run(meta, run_id)

    client.post(f"{srv.base_url}/demux/tasks", json={
        "request_id": "req-corrupt", "run_id": run_id, "sample_sheet": SHEET,
        "fault_after_pairs": 3,
    })
    assert _wait_exit(srv) != 0

    # 人为破坏一个已登记分块（追加脏字节），并放入一个未登记残留文件
    tmp_root = os.path.join(meta["demux_output"], "req-corrupt", "tmp")
    chunk_files = []
    for root, _dirs, files in os.walk(tmp_root):
        chunk_files.extend(os.path.join(root, f) for f in files)
    assert chunk_files, "故障后应至少落有一个分块"
    with open(chunk_files[0], "ab") as fh:
        fh.write(b"CORRUPTED-BYTES\n")
    with open(os.path.join(tmp_root, "stray_residual.fastq"), "wb") as fh:
        fh.write(b"@stray/1\nACGT\n+\nIIII\n")

    _restart(srv)
    client.post(f"{srv.base_url}/demux/tasks", json={
        "request_id": "req-corrupt", "run_id": run_id, "sample_sheet": SHEET,
    })
    snap = _poll_task(client, srv.base_url, "req-corrupt",
                      allow_recovering_failed=True)
    # 触发了全量重置：processed_pairs 重新走到总对数，结果与正常任务一致
    assert snap["processed_pairs"] == 11
    assert _bucket_map(snap) == {"S1": 3, "S2": 3, "S3": 1, "unassigned": 4}
    # 残留文件已被清理；最终输出不含损坏内容
    assert not os.path.exists(os.path.join(tmp_root, "stray_residual.fastq"))
    r1 = client.get(
        f"{srv.base_url}/demux/tasks/req-corrupt/download/unassigned/R1"
    ).text
    assert "CORRUPTED-BYTES" not in r1 and "stray" not in r1
    names = [l[1:].split("/")[0] for l in r1.splitlines() if l.startswith("@")]
    assert names == ["read0004", "read0005", "read0006", "read0010"]
    # 分块登记被重建且与磁盘一一对应
    conn = sqlite3.connect(meta["demux_db"])
    n_chunks = conn.execute(
        "SELECT COUNT(*) AS n FROM demux_chunks WHERE request_id='req-corrupt'"
    ).fetchone()[0]
    on_disk = sum(len(files) for _r, _d, files in os.walk(tmp_root))
    conn.close()
    assert n_chunks == on_disk and n_chunks > 0


def test_status_not_saved_window_resumes_without_rereading(isolated_collector, client):
    srv, meta = isolated_collector
    run_id = "dm-window"
    _write_completed_run(meta, run_id, cases=CASES[:6])

    r = client.post(f"{srv.base_url}/demux/tasks", json={
        "request_id": "req-window", "run_id": run_id, "sample_sheet": SHEET,
        "fault_before_status": True,
    })
    assert r.status_code == 202
    assert _wait_exit(srv) != 0
    # 输出文件已发布，但状态仍停在 running
    out = os.path.join(meta["demux_output"], "req-window", "output")
    assert os.path.isfile(os.path.join(out, "MANIFEST.json"))
    import sqlite3
    conn = sqlite3.connect(meta["demux_db"])
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT status FROM demux_tasks WHERE request_id='req-window'"
    ).fetchone()
    conn.close()
    assert row["status"] == "running"

    # 重启后重试：发布完整性校验通过，只补状态、不重读段
    _restart(srv)
    r = client.post(f"{srv.base_url}/demux/tasks", json={
        "request_id": "req-window", "run_id": run_id, "sample_sheet": SHEET,
    })
    assert r.status_code == 202
    snap = _poll_task(client, srv.base_url, "req-window")
    assert snap["status"] == "completed"
    assert snap["processed_pairs"] == 6
    # 已成功任务可下载，且重启后依然可下载
    for bucket in ("S1", "S2", "unassigned"):
        assert client.get(
            f"{srv.base_url}/demux/tasks/req-window/download/{bucket}/R1"
        ).status_code == 200


def test_completed_output_survives_full_restart(isolated_collector, client):
    srv, meta = isolated_collector
    run_id = "dm-restart-dl"
    _write_completed_run(meta, run_id, cases=CASES[:4])
    client.post(f"{srv.base_url}/demux/tasks", json={
        "request_id": "req-restart-dl", "run_id": run_id, "sample_sheet": SHEET,
    })
    _poll_task(client, srv.base_url, "req-restart-dl")
    before = client.get(
        f"{srv.base_url}/demux/tasks/req-restart-dl/download/S1/R1"
    ).content

    # 普通重启（无故障注入）：已完成任务状态与文件都在持久卷上
    _restart(srv)
    snap = client.get(f"{srv.base_url}/demux/tasks/req-restart-dl").json()
    assert snap["status"] == "completed"
    after = client.get(
        f"{srv.base_url}/demux/tasks/req-restart-dl/download/S1/R1"
    ).content
    assert before == after
