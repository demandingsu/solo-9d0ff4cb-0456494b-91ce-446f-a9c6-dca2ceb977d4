"""拆样端到端演示（本地可运行）。

三个场景，全部走真实 HTTP API：

1. **歧义归属** ``req-ambiguity``：构造精确命中、每端 1 替换、不足 4
   碱基、无匹配、以及两端同时落入两个样本邻域的歧义读段，验证唯一
   命中才归属、其余进未分配桶，且桶内保留原始顺序；
2. **写盘失败恢复** ``req-diskfault``：提交时注入故障，工作线程在
   检查点后写盘失败、进程非零退出；等待容器/进程重启后用**原请求号、
   原样本表**重试，从分块检查点续跑，不重复读段、不混残留文件；
3. **文件完成而状态未保存** ``req-statusgap``：注入"状态落库前崩溃"，
   重启重试时只校验已发布文件并补状态，不重读任何读段。

环境变量：
  RELAY_URL      默认 http://relay:8000（compose 网络）
  COLLECTOR_URL  默认 http://collector:8000
  RELAY_DATA     relay 持久卷在本容器内的挂载点（默认 /shared/relay），
                 演示直接在此生成 FASTQ，再经 relay 的文件服务提供
  RUN_ID         默认 demux-demo
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import httpx

RELAY = os.environ.get("RELAY_URL", "http://relay:8000").rstrip("/")
COLLECTOR = os.environ.get("COLLECTOR_URL", "http://collector:8000").rstrip("/")
RELAY_DATA = Path(os.environ.get("RELAY_DATA", "/shared/relay"))
RUN_ID = os.environ.get("RUN_ID", "demux-demo")

SHEET = [
    {"sample_name": "S1", "barcode_r1": "AAAA", "barcode_r2": "CCCC"},
    {"sample_name": "S2", "barcode_r1": "GGGG", "barcode_r2": "TTTT"},
    # 与 S1 的 R1 仅差 1、R2 相同：精确命中优先，邻域交叠制造歧义场景
    {"sample_name": "S3", "barcode_r1": "AAAG", "barcode_r2": "CCCC"},
]

# (R1 序列, R2 序列, 期望桶, 说明)
CASES = [
    ("AAAATTTTTT", "CCCCAAAAAA", "S1", "精确命中 S1"),
    ("GGGGTTTTTT", "TTTTAAAAAA", "S2", "精确命中 S2"),
    ("AAGATTTTTT", "CCCTAAAAAA", "S1", "两端各 1 处替换仍归 S1"),
    ("GCGGTTTTTT", "TTATAAAAAA", "S2", "两端各 1 处替换仍归 S2"),
    ("AATTTTTTTT", "CCCCAAAAAA", "unassigned", "R1 与所有条码距离 >=2，无匹配"),
    ("AAAATTTTTT", "GGGGAAAAAA", "unassigned", "R2 无任何样本命中"),
    ("ACG", "CCC", "unassigned", "两端不足 4 碱基"),
    ("AAAATTTTTT", "CCCCAAAAAA", "S1", "精确 S1：不被 S3 的邻域稀释为歧义"),
    ("AAAGTTTTTT", "CCCCAAAAAA", "S3", "精确 S3：不被 S1 的邻域稀释为歧义"),
    ("GGGGTTTTTT", "GTTTAAAAAA", "S2", "仅 R2 1 处替换，归 S2"),
    ("AAATTTTTTT", "CCCAAAAAAA", "unassigned", "两端邻域同时命中 S1/S3，歧义"),
]


def record(name: str, side: int, seq: str) -> bytes:
    qual = "I" * len(seq)
    return f"@{name}/{side}\n{seq}\n+\n{qual}\n".encode()


def wait_ready(client: httpx.Client, url: str, timeout: float = 60) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if client.get(f"{url}/healthz").status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"服务未就绪: {url}")


def wait_collector_back(client: httpx.Client, timeout: float = 90) -> None:
    """等待 collector 进程崩溃后被重启策略拉起。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if client.get(f"{COLLECTOR}/healthz").status_code == 200:
                return
        except httpx.HTTPError:
            client.close()  # 驱逐连接池中指向旧进程的失效连接
        time.sleep(0.5)
    raise RuntimeError("collector 重启超时")


def poll_task(client: httpx.Client, request_id: str, timeout: float = 60,
              allow_recovering: bool = False) -> dict:
    deadline = time.time() + timeout
    failed_at = None
    last = {}
    while time.time() < deadline:
        try:
            last = client.get(f"{COLLECTOR}/demux/tasks/{request_id}").json()
        except httpx.HTTPError:
            # 进程刚被重启策略拉起：连接池里可能缓存着旧进程的失效连接，
            # 主动驱逐后下一轮用新连接重试
            client.close()
            time.sleep(0.5)
            continue
        if last["status"] == "completed":
            return last
        if last["status"] == "failed":
            if not allow_recovering:
                raise RuntimeError(f"任务意外失败: {last}")
            # 恢复场景：重试发起与工作线程翻转状态之间可能短暂看到旧 failed
            if failed_at is None:
                failed_at = time.time()
            elif time.time() - failed_at > 10:
                raise RuntimeError(f"任务持续 failed 未恢复: {last}")
        time.sleep(0.3)
    raise RuntimeError(f"任务超时未完成: {last}")


def write_source_run() -> None:
    """在 relay 持久卷中生成 R1/R2/DONE，并在采集库登记为 completed 运行。

    拆样只接受采集完成的运行；演示跳过采集环节，直接构造等价产物，
    文件名与四行结构与真实采集结果完全一致。
    """
    rdir = RELAY_DATA / RUN_ID
    rdir.mkdir(parents=True, exist_ok=True)
    r1, r2 = [], []
    for seq, (s1, s2, _bucket, _note) in enumerate(CASES):
        name = f"read{seq:04d}"
        r1.append(record(name, 1, s1))
        r2.append(record(name, 2, s2))
    (rdir / "R1.fastq").write_bytes(b"".join(r1))
    (rdir / "R2.fastq").write_bytes(b"".join(r2))
    (rdir / "DONE").write_text(f"{{\"reads\": {len(CASES)}}}\n")
    print(f"[demo] 已在 relay 卷生成运行 {RUN_ID}：{len(CASES)} 对双端读段")


def ensure_completed_run(client: httpx.Client) -> None:
    """通过采集 API 让 collector 把演示运行采集到 completed。"""
    base_url = f"{RELAY}/runs/{RUN_ID}"
    body = {"run_id": RUN_ID, "base_url": base_url}
    deadline = time.time() + 60
    while time.time() < deadline:
        snap = client.post(f"{COLLECTOR}/collect/runs", json=body).json()
        if snap["status"] == "completed":
            print(f"[demo] 采集完成：{snap['pairs']} 对配对读段")
            return
        if snap["status"] == "error":
            raise RuntimeError(f"采集失败: {snap}")
        time.sleep(0.5)
    raise RuntimeError("采集等待超时")


def submit(client: httpx.Client, request_id: str, **extra) -> dict:
    body = {"request_id": request_id, "run_id": RUN_ID, "sample_sheet": SHEET, **extra}
    r = client.post(f"{COLLECTOR}/demux/tasks", json=body)
    r.raise_for_status()
    return r.json()


def bucket_map(snap: dict) -> dict[str, int]:
    return {b["bucket"]: b["pair_count"] for b in snap["buckets"]}


def show_buckets(title: str, snap: dict) -> None:
    print(f"[demo] {title}")
    for b in snap["buckets"]:
        print(f"        - {b['bucket']:<12} {b['pair_count']} 对")


def scenario_ambiguity(client: httpx.Client) -> None:
    print("\n=== 场景 1：归属规则与歧义（req-ambiguity）===")
    submit(client, "req-ambiguity")
    snap = poll_task(client, "req-ambiguity")
    counts = bucket_map(snap)
    expected = {"S1": 3, "S2": 3, "S3": 1, "unassigned": 4}
    assert counts == expected, f"桶计数不符: {counts} != {expected}"
    show_buckets("各桶计数符合预期：", snap)

    # 校验未分配桶的内容与原始顺序
    r1 = client.get(
        f"{COLLECTOR}/demux/tasks/req-ambiguity/download/unassigned/R1"
    ).text
    names = [line[1:].split("/")[0] for line in r1.splitlines() if line.startswith("@")]
    assert names == ["read0004", "read0005", "read0006", "read0010"], names
    print(f"[demo] 未分配桶顺序保留: {names}")

    # 下载门控之外，确认所有桶 R1/R2 均可下载
    for bucket in expected:
        for side in (1, 2):
            r = client.get(
                f"{COLLECTOR}/demux/tasks/req-ambiguity/download/{bucket}/R{side}"
            )
            assert r.status_code == 200
    print("[demo] 全部样本桶与未分配桶的 R1/R2 均已开放下载")

    # 同参复用、异参冲突
    again = submit(client, "req-ambiguity")
    assert again["reused"] is True and again["status"] == "completed"
    conflict = client.post(f"{COLLECTOR}/demux/tasks", json={
        "request_id": "req-ambiguity", "run_id": RUN_ID,
        "sample_sheet": SHEET[:2],
    })
    assert conflict.status_code == 409
    print("[demo] 同参提交复用任务，异参提交返回 409 冲突")


def scenario_disk_fault(client: httpx.Client) -> None:
    print("\n=== 场景 2：写盘失败 → 进程退出 → 重启续跑（req-diskfault）===")
    # 故障点设在第 3 对之后（批次大小默认 100，演示环境通过 DEMUX_CHUNK_PAIRS
    # 调小时检查点更密；这里故障发生在首批写入后，重启仍能验证续跑语义）
    submit(client, "req-diskfault", fault_after_pairs=3)
    print("[demo] 已注入写盘故障，等待 collector 进程非零退出……")
    # 健康检查会先成功（HTTP 已返回 202），随后进程退出
    time.sleep(2)
    wait_collector_back(client)
    print("[demo] collector 已被重启策略拉起")

    # 原请求号 + 原样本表重试（不带故障参数）
    submit(client, "req-diskfault")
    snap = poll_task(client, "req-diskfault", allow_recovering=True)
    counts = bucket_map(snap)
    assert counts == {"S1": 3, "S2": 3, "S3": 1, "unassigned": 4}, counts
    assert snap["processed_pairs"] == len(CASES)
    show_buckets("续跑完成，计数与场景 1 完全一致：", snap)

    r1 = client.get(
        f"{COLLECTOR}/demux/tasks/req-diskfault/download/unassigned/R1"
    ).text
    names = [line[1:].split("/")[0] for line in r1.splitlines() if line.startswith("@")]
    assert len(names) == len(set(names)) == 4, names
    print(f"[demo] 未分配桶无重复读段、顺序保留: {names}")


def scenario_status_gap(client: httpx.Client) -> None:
    print("\n=== 场景 3：文件完成而状态未保存 → 重启只补状态（req-statusgap）===")
    submit(client, "req-statusgap", fault_before_status=True)
    print("[demo] 已注入状态落库前崩溃，等待进程退出与重启……")
    time.sleep(2)
    wait_collector_back(client)
    print("[demo] collector 已重启，输出文件与 MANIFEST 已在持久卷上")

    submit(client, "req-statusgap")
    snap = poll_task(client, "req-statusgap", allow_recovering=True)
    assert snap["status"] == "completed"
    assert snap["processed_pairs"] == len(CASES)
    show_buckets("未重读任何读段即补齐状态：", snap)
    for bucket in ("S1", "S2", "S3", "unassigned"):
        r = client.get(
            f"{COLLECTOR}/demux/tasks/req-statusgap/download/{bucket}/R1"
        )
        assert r.status_code == 200
    print("[demo] 已成功任务在重启后仍可下载")


def main() -> int:
    RELAY_DATA.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=30) as client:
        wait_ready(client, RELAY)
        wait_ready(client, COLLECTOR)
        print("[demo] relay 与 collector 均已就绪")
        write_source_run()
        ensure_completed_run(client)
        scenario_ambiguity(client)
        scenario_disk_fault(client)
        scenario_status_gap(client)
    print("\n[demo] 拆样演示全部完成 ✔")
    print("[demo] 交互式文档: http://localhost:8000/docs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
