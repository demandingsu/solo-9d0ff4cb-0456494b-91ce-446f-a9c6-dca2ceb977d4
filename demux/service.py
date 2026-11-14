"""拆样服务：后台工作线程 + 分块断点 + 崩溃安全发布。

单进程模型
==========
拆样在独立的**后台线程**中执行，线程内全部是阻塞式 SQLite/文件 IO，
不占用 FastAPI 事件循环，因此拆样期间采集与查询接口照常响应。
同一任务在进程内有活动标志保护，并发提交只会执行一次。

崩溃安全（写盘失败 / 进程退出）
================================
* 配对读段按固定批次切成不可变分块文件，先 ``flush + fsync``，再在
  **单个 SQLite 事务**里登记分块、累计桶计数、前推 ``processed_pairs``；
* 事务未提交则分块"不存在于状态中"，恢复时作为残留整体清理，
  已提交分块绝不重写——重启续跑不重复读段、不混入残留文件；
* 全部读段处理完后，先把分块原子拼接（同卷 ``os.replace``）成最终
  R1/R2，再原子写 MANIFEST，最后才把状态置为 ``completed``；
* 若"文件已完成而状态未保存"（例如状态提交前被杀死），重启续跑会
  先校验已发布文件与 MANIFEST，校验通过则只补状态，不再重读任何读段；
* 写盘失败时先落 ``failed`` 状态再以非零码退出进程（演示故障注入用），
  持久卷中的分块与 DB 均保持一致，容器重启后按原请求号重试即可续跑。
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from . import persistence as db
from .matcher import (
    BARCODE_LEN,
    REASON_OK,
    SampleBarcode,
    UNASSIGNED,
    assign,
    build_index,
    first_four_bases,
)

CHUNK_PAIRS = int(os.environ.get("DEMUX_CHUNK_PAIRS", "100"))
MANIFEST_NAME = "MANIFEST.json"
SHEET_FILENAME = "sample_sheet.json"


class DemuxConflict(RuntimeError):
    """同请求号但参数与已登记任务不一致（HTTP 409）。"""


class DemuxRequestError(RuntimeError):
    """请求本身不合法（运行不存在/未完成、样本表非法，HTTP 4xx）。"""


class DiskWriteError(RuntimeError):
    """持久卷写盘失败，触发状态落库后进程退出。"""


@dataclass(frozen=True)
class SheetEntry:
    sample_name: str
    barcode_r1: str
    barcode_r2: str


def normalize_sheet(entries: list[dict]) -> list[SheetEntry]:
    """校验并规范化样本表。

    要求：样本名非空且不与保留名冲突、两端条码均为 4 位 A/C/G/T，
    样本名与条码组合均不得重复。
    """
    if not entries:
        raise DemuxRequestError("样本表不能为空")
    normalized: list[SheetEntry] = []
    seen_names: set[str] = set()
    seen_barcodes: set[tuple[str, str]] = set()
    allowed = set("ACGT")
    for i, entry in enumerate(entries):
        name = str(entry.get("sample_name", "")).strip()
        b1 = str(entry.get("barcode_r1", "")).upper()
        b2 = str(entry.get("barcode_r2", "")).upper()
        if not name:
            raise DemuxRequestError(f"样本表第 {i + 1} 行缺少样本名")
        if name == UNASSIGNED:
            raise DemuxRequestError(f"样本名 {UNASSIGNED!r} 为保留桶名，禁止使用")
        if len(b1) != BARCODE_LEN or set(b1) - allowed:
            raise DemuxRequestError(
                f"样本 {name!r} 的 R1 条码必须为 {BARCODE_LEN} 位 A/C/G/T，实际为 {b1!r}"
            )
        if len(b2) != BARCODE_LEN or set(b2) - allowed:
            raise DemuxRequestError(
                f"样本 {name!r} 的 R2 条码必须为 {BARCODE_LEN} 位 A/C/G/T，实际为 {b2!r}"
            )
        if name in seen_names:
            raise DemuxRequestError(f"样本名重复: {name!r}")
        pair = (b1, b2)
        if pair in seen_barcodes:
            raise DemuxRequestError(f"条码组合重复: {name!r} {pair}")
        seen_names.add(name)
        seen_barcodes.add(pair)
        normalized.append(SheetEntry(name, b1, b2))
    return normalized


def sheet_to_json(entries: list[SheetEntry]) -> str:
    payload = [
        {"sample_name": e.sample_name, "barcode_r1": e.barcode_r1,
         "barcode_r2": e.barcode_r2}
        for e in entries
    ]
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _fsync_dir(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_write(path: Path, data: bytes) -> None:
    """同目录临时文件 + fsync + os.replace 的原子写。"""
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


class DemuxService:
    def __init__(
        self,
        demux_db_path: str,
        collector_db_path: str,
        output_root: str,
        chunk_pairs: int | None = None,
    ):
        self.demux_db_path = demux_db_path
        self.collector_db_path = collector_db_path
        self.output_root = Path(output_root)
        self.output_root.mkdir(parents=True, exist_ok=True)
        # 故障注入只通过单次提交参数（fault_after_pairs / fault_before_status）
        # 传入工作线程，进程级默认始终关闭，避免影响普通任务
        self.chunk_pairs = chunk_pairs or CHUNK_PAIRS
        self._conn = db.connect(demux_db_path)
        db.init_db(self._conn)
        self._active: set[str] = set()
        self._active_lock = threading.Lock()

    def close(self) -> None:
        self._conn.close()

    # --------------------------------------------------------------- 任务提交
    def submit(
        self,
        request_id: str,
        run_id: str,
        sheet_entries: list[dict],
        fault_after_pairs: int | None = None,
        fault_before_status: bool = False,
    ) -> tuple[dict, bool]:
        """提交拆样任务。返回 (状态快照, 是否新建)。

        * 同请求号同参数：复用任务，已完成直接返回，运行中不重复启动；
        * 同请求号异参数：抛 :class:`DemuxConflict`；
        * 并发提交由进程内活动标志 + SQLite 主键共同保证只执行一次。
        """
        entries = normalize_sheet(sheet_entries)
        sheet_json = sheet_to_json(entries)
        now = time.time()

        existing = db.get_task(self._conn, request_id)
        if existing is not None:
            if existing["run_id"] != run_id or existing["sample_sheet"] != sheet_json:
                raise DemuxConflict(
                    f"请求号 {request_id!r} 已绑定不同的运行号或样本表"
                )
            # completed 直接复用（已成功任务仍可下载）；
            # pending/running 进程内若已无活动线程则重启续跑；
            # failed 多为写盘失败退出后留下，允许用原任务号原样本表重试。
            # 故障注入参数只对**首次执行**生效：任何复用/重试都剥离注入，
            # 否则"故障后重试"会被同一个故障点再次打死。
            if existing["status"] != db.COMPLETED:
                self._start_if_idle(request_id, None, False)
            return self.snapshot(request_id), False
        # 新建任务前先核验源运行确实已完成采集
        self._require_completed_run(run_id)
        try:
            db.create_task(self._conn, request_id, run_id, sheet_json, now)
        except sqlite3.IntegrityError:
            # 并发提交下另一个线程已抢先创建：按复用路径处理
            row = db.get_task(self._conn, request_id)
            if row["run_id"] != run_id or row["sample_sheet"] != sheet_json:
                raise DemuxConflict(
                    f"请求号 {request_id!r} 已绑定不同的运行号或样本表"
                )
            if row["status"] != db.COMPLETED:
                self._start_if_idle(request_id, None, False)
            return self.snapshot(request_id), False
        self._start_if_idle(request_id, fault_after_pairs, fault_before_status)
        return self.snapshot(request_id), True

    def _start_if_idle(
        self, request_id: str, fault_after_pairs: int | None,
        fault_before_status: bool,
    ) -> None:
        with self._active_lock:
            if request_id in self._active:
                return
            self._active.add(request_id)
        thread = threading.Thread(
            target=self._thread_main,
            args=(request_id, fault_after_pairs, fault_before_status),
            name=f"demux-{request_id}",
            daemon=True,
        )
        thread.start()

    def _require_completed_run(self, run_id: str) -> None:
        """拆样只接受采集端已完成（配对齐全）的运行。"""
        if not os.path.exists(self.collector_db_path):
            raise DemuxRequestError(f"采集库不存在，无法核验运行 {run_id!r}")
        conn = sqlite3.connect(self.collector_db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT status, pairs FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise DemuxRequestError(f"运行 {run_id!r} 不存在，请先完成采集")
        if row["status"] != "completed":
            raise DemuxRequestError(
                f"运行 {run_id!r} 当前状态为 {row['status']!r}，"
                "只有采集完成（completed）的运行才能拆样"
            )

    # ------------------------------------------------------------------ 查询
    def snapshot(self, request_id: str) -> dict:
        row = db.get_task(self._conn, request_id)
        if row is None:
            raise KeyError(request_id)
        return db.snapshot(row, db.list_buckets(self._conn, request_id))

    def list_snapshots(self) -> list[dict]:
        return [
            db.snapshot(row, db.list_buckets(self._conn, row["request_id"]))
            for row in db.list_tasks(self._conn)
        ]

    def output_path(self, request_id: str, bucket: str, side: int) -> Path:
        """供下载端点做完成门控后的路径解析（含越界保护）。"""
        row = db.get_task(self._conn, request_id)
        if row is None:
            raise KeyError(request_id)
        if row["status"] != db.COMPLETED or not row["output_dir"]:
            raise PermissionError("not_completed")
        base = Path(row["output_dir"]).resolve()
        path = (base / f"{bucket}_R{side}.fastq").resolve()
        if base not in path.parents:
            raise PermissionError("path_escape")
        if not path.is_file():
            raise FileNotFoundError(str(path))
        return path

    # ------------------------------------------------------------- 工作线程
    def _thread_main(
        self, request_id: str, fault_after_pairs: int | None,
        fault_before_status: bool,
    ) -> None:
        # 线程使用独立连接，避免与 API 查询连接跨线程共用
        conn = db.connect(self.demux_db_path)
        task_dir = self.output_root / request_id
        tmp_dir = task_dir / "tmp"
        out_dir = task_dir / "output"
        try:
            row = db.get_task(conn, request_id)
            entries = [SheetEntry(**e) for e in json.loads(row["sample_sheet"])]
            total_pairs = self._source_total_pairs(row["run_id"])

            # 恢复窗口 1：上次已发布全部文件但状态未保存 → 只补状态，不重读段
            if self._published_output_is_complete(out_dir, entries, total_pairs):
                db.mark_running(conn, request_id, total_pairs, time.time())
                self._republish_status(conn, request_id, out_dir, entries,
                                       total_pairs, fault_before_status)
                return

            tmp_dir.mkdir(parents=True, exist_ok=True)
            samples = [
                SampleBarcode(e.sample_name, e.barcode_r1.encode(),
                              e.barcode_r2.encode())
                for e in entries
            ]
            index_r1, index_r2 = build_index(samples)
            bucket_names = [e.sample_name for e in entries] + [UNASSIGNED]

            start_seq = self._resume_or_reset(conn, request_id, tmp_dir)
            # 续跑（含 failed 后的重试）：状态回到 running，总对数以源库为准
            db.mark_running(conn, request_id, total_pairs, time.time())

            # 内存中的当前批次缓冲：bucket -> [r1 bytearray, r2 bytearray, 对数]
            buffers: dict[str, list] = {
                name: [bytearray(), bytearray(), 0] for name in bucket_names
            }
            chunk_index = start_seq // self.chunk_pairs
            processed = start_seq

            for seq, r1_raw, r2_raw in self._iter_source_pairs(
                row["run_id"], start_seq
            ):
                idx, reason = assign(
                    index_r1, index_r2,
                    first_four_bases(r1_raw), first_four_bases(r2_raw),
                )
                bucket = bucket_names[idx] if reason == REASON_OK else UNASSIGNED
                buf = buffers[bucket]
                buf[0].extend(r1_raw)
                buf[1].extend(r2_raw)
                buf[2] += 1
                processed = seq + 1

                if fault_after_pairs is not None and processed >= fault_after_pairs:
                    raise DiskWriteError(
                        f"故障注入：处理到第 {processed} 对时持久卷写入失败"
                    )

                if processed % self.chunk_pairs == 0:
                    self._flush_chunk(conn, request_id, tmp_dir, buffers,
                                      processed - self.chunk_pairs, processed,
                                      chunk_index)
                    chunk_index += 1
                    buffers = {
                        name: [bytearray(), bytearray(), 0] for name in bucket_names
                    }

            if processed != total_pairs:
                # 源运行在任务登记后被改动：拒绝产出，避免对数对不上
                raise DiskWriteError(
                    f"源运行对数变化：登记时 {total_pairs}，实际遍历到 {processed}"
                )

            # 收尾分块（不足一个批次的尾部）
            tail_start = (processed // self.chunk_pairs) * self.chunk_pairs
            if processed > tail_start:
                self._flush_chunk(conn, request_id, tmp_dir, buffers,
                                  tail_start, processed, chunk_index)

            self._publish(conn, request_id, tmp_dir, out_dir, entries,
                          total_pairs, fault_before_status)
        except DiskWriteError as exc:
            self._handle_disk_failure(conn, request_id, exc)
        except Exception as exc:  # 数据类异常：落 failed 但不退出进程
            db.mark_failed(
                conn, request_id,
                {"code": type(exc).__name__, "message": str(exc)}, time.time(),
            )
        finally:
            conn.close()
            with self._active_lock:
                self._active.discard(request_id)

    # ------------------------------------------------------------- 源数据读取
    def _source_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.collector_db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def _source_total_pairs(self, run_id: str) -> int:
        conn = self._source_conn()
        try:
            row = conn.execute(
                "SELECT pairs, status FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row is None or row["status"] != "completed":
                raise DemuxRequestError(f"运行 {run_id!r} 不可拆样")
            return int(row["pairs"])
        finally:
            conn.close()

    def _iter_source_pairs(self, run_id: str, start_seq: int):
        """按原始配对顺序流式产出 (seq, r1_record, r2_record)。"""
        conn = self._source_conn()
        try:
            cur = conn.execute(
                "SELECT seq, r1_record, r2_record FROM pairs WHERE run_id=? AND seq>=?"
                " ORDER BY seq",
                (run_id, start_seq),
            )
            for row in cur:
                yield row["seq"], bytes(row["r1_record"]), bytes(row["r2_record"])
        finally:
            conn.close()

    # ------------------------------------------------------------- 分块与检查点
    def _resume_or_reset(
        self, conn: sqlite3.Connection, request_id: str, tmp_dir: Path
    ) -> int:
        """决定续跑起点。

        * 已登记分块与磁盘文件（路径/大小/序号区间）完全一致：
          从 ``processed_pairs`` 续跑，不重复读段；
        * 任何不一致（残留、缺块、尺寸不符）：清空 tmp 与分块登记，
          从 0 重新开始，杜绝残留文件混入。
        """
        row = db.get_task(conn, request_id)
        processed = int(row["processed_pairs"])
        chunks = db.list_chunks(conn, request_id)
        if processed == 0 and not chunks:
            return 0
        ok = True
        for chunk in chunks:
            p = tmp_dir / chunk["relpath"]
            if not p.is_file() or p.stat().st_size != chunk["byte_size"]:
                ok = False
                break
            if chunk["end_seq"] > processed:
                ok = False
                break
        # tmp 目录里不允许存在任何未登记文件
        if ok:
            registered = {str((tmp_dir / c["relpath"]).resolve()) for c in chunks}
            for p in tmp_dir.rglob("*"):
                if p.is_file() and str(p.resolve()) not in registered:
                    ok = False
                    break
        if ok:
            return processed
        # 不可信：全量清理后从头再来
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        with conn:
            conn.execute("DELETE FROM demux_chunks WHERE request_id=?", (request_id,))
            conn.execute("DELETE FROM demux_buckets WHERE request_id=?", (request_id,))
            conn.execute(
                "UPDATE demux_tasks SET processed_pairs=0, total_pairs=0,"
                " updated_at=? WHERE request_id=?",
                (time.time(), request_id),
            )
        return 0

    def _flush_chunk(
        self,
        conn: sqlite3.Connection,
        request_id: str,
        tmp_dir: Path,
        buffers: dict[str, list],
        start_seq: int,
        end_seq: int,
        chunk_index: int,
    ) -> None:
        """把当前批次各桶缓冲写成不可变分块，并在单事务中登记检查点。"""
        chunk_rows: list[tuple] = []
        bucket_deltas: dict[str, int] = {}
        for bucket, (r1_blob, r2_blob, count) in buffers.items():
            if count == 0:
                continue
            bdir = tmp_dir / bucket
            bdir.mkdir(parents=True, exist_ok=True)
            for side, blob in ((1, r1_blob), (2, r2_blob)):
                rel = f"{bucket}/chunk_{chunk_index:08d}_R{side}.fastq"
                target = tmp_dir / rel
                try:
                    # 不可变分块：正常路径下绝不允许覆盖已存在文件
                    fd = os.open(str(target), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
                    try:
                        os.write(fd, blob)
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                    _fsync_dir(bdir)
                except OSError as exc:
                    raise DiskWriteError(f"分块写入失败 {rel}: {exc}") from exc
                chunk_rows.append((bucket, side, start_seq, end_seq, rel, len(blob)))
            # 一条配对在两端各写一个分块，桶对数只累计一次
            bucket_deltas[bucket] = count
        try:
            db.commit_checkpoint(conn, request_id, end_seq, bucket_deltas,
                                 chunk_rows, time.time())
        except sqlite3.Error as exc:
            raise DiskWriteError(f"检查点事务提交失败: {exc}") from exc

    # ------------------------------------------------------------- 发布与门控
    def _publish(
        self,
        conn: sqlite3.Connection,
        request_id: str,
        tmp_dir: Path,
        out_dir: Path,
        entries: list[SheetEntry],
        total_pairs: int,
        fault_before_status: bool,
    ) -> None:
        """分块 → 最终 R1/R2 → MANIFEST → completed，严格按序对外可见。"""
        if out_dir.exists():
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True)
        buckets = [e.sample_name for e in entries] + [UNASSIGNED]
        chunks = db.list_chunks(conn, request_id)
        # (bucket, side) -> 按 start_seq 排序的分块行，拼接即桶内原始顺序
        by_bucket_side: dict[tuple[str, int], list[sqlite3.Row]] = {}
        for chunk in chunks:
            by_bucket_side.setdefault((chunk["bucket"], chunk["side"]), []).append(chunk)

        manifest_buckets = []
        # 分块的 [start_seq, end_seq) 是**全局**配对序号区间，一个桶在每批
        # 只占其中若干条，因此桶对数不能由 end-start 求和，而以检查点事务
        # 累计的 demux_buckets 计数表为唯一权威
        bucket_counts = {
            row["bucket"]: row["pair_count"]
            for row in db.list_buckets(conn, request_id)
        }
        for bucket in buckets:
            pair_count = int(bucket_counts.get(bucket, 0))
            for side in (1, 2):
                parts = by_bucket_side.get((bucket, side), [])
                final_path = out_dir / f"{bucket}_R{side}.fastq"
                tmp_final = final_path.with_name(final_path.name + ".part")
                try:
                    with open(tmp_final, "wb") as out:
                        for chunk in parts:
                            src = tmp_dir / chunk["relpath"]
                            with open(src, "rb") as fh:
                                shutil.copyfileobj(fh, out, length=1024 * 1024)
                        out.flush()
                        os.fsync(out.fileno())
                    os.replace(tmp_final, final_path)
                    _fsync_dir(out_dir)
                except OSError as exc:
                    raise DiskWriteError(f"最终文件发布失败 {final_path.name}: {exc}") from exc
            manifest_buckets.append({"bucket": bucket, "pair_count": pair_count})

        if sum(b["pair_count"] for b in manifest_buckets) != total_pairs:
            raise DiskWriteError("发布计数与源运行总对数不一致，拒绝开放下载")

        manifest = {
            "request_id": request_id,
            "run_id": db.get_task(conn, request_id)["run_id"],
            "total_pairs": total_pairs,
            "buckets": manifest_buckets,
            "sample_sheet": [e.__dict__ for e in entries],
            "published_at": time.time(),
        }
        try:
            _atomic_write(out_dir / MANIFEST_NAME,
                          (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode())
            _atomic_write(out_dir / SHEET_FILENAME,
                          (sheet_to_json(entries) + "\n").encode())
        except OSError as exc:
            raise DiskWriteError(f"MANIFEST 写入失败: {exc}") from exc

        self._republish_status(conn, request_id, out_dir, entries, total_pairs,
                               fault_before_status, manifest_buckets)

    def _republish_status(
        self,
        conn: sqlite3.Connection,
        request_id: str,
        out_dir: Path,
        entries: list[SheetEntry],
        total_pairs: int,
        fault_before_status: bool,
        manifest_buckets: list[dict] | None = None,
    ) -> None:
        """文件齐备后的最后一步：状态置 completed。

        ``fault_before_status`` 注入"文件完成而状态未保存"的崩溃：
        状态落库前直接退出，重启续跑时由发布完整性校验补状态。
        """
        if fault_before_status:
            os._exit(86)  # 模拟进程被杀死，绕过 finally 与解释器清理
        # 状态保存前再做一次 MANIFEST 对账，防止目录被外部破坏
        self._verify_manifest(out_dir, entries, total_pairs, manifest_buckets)
        db.mark_completed(conn, request_id, str(out_dir), time.time())

    def _published_output_is_complete(
        self, out_dir: Path, entries: list[SheetEntry], total_pairs: int
    ) -> bool:
        if not out_dir.is_dir() or not (out_dir / MANIFEST_NAME).is_file():
            return False
        try:
            self._verify_manifest(out_dir, entries, total_pairs, None)
        except (OSError, ValueError, json.JSONDecodeError):
            return False
        return True

    def _verify_manifest(
        self,
        out_dir: Path,
        entries: list[SheetEntry],
        total_pairs: int,
        manifest_buckets: list[dict] | None,
    ) -> None:
        manifest = json.loads((out_dir / MANIFEST_NAME).read_text(encoding="utf-8"))
        if manifest["total_pairs"] != total_pairs:
            raise ValueError("MANIFEST 总对数不符")
        buckets = [e.sample_name for e in entries] + [UNASSIGNED]
        if [b["bucket"] for b in manifest["buckets"]] != buckets:
            raise ValueError("MANIFEST 桶集合与样本表不符")
        total = 0
        for item in manifest["buckets"]:
            total += item["pair_count"]
            for side in (1, 2):
                p = out_dir / f"{item['bucket']}_R{side}.fastq"
                if not p.is_file() or p.stat().st_size < 0:
                    raise ValueError(f"输出文件缺失: {p.name}")
        if total != total_pairs:
            raise ValueError("MANIFEST 桶计数之和与总对数不符")
        if manifest_buckets is not None and manifest_buckets != manifest["buckets"]:
            raise ValueError("发布计数与 MANIFEST 不一致")

    # ------------------------------------------------------------- 故障处理
    def _handle_disk_failure(
        self, conn: sqlite3.Connection, request_id: str, exc: DiskWriteError
    ) -> None:
        """写盘失败：先尽量保存 failed 状态，再非零退出，交由容器重启重试。"""
        try:
            db.mark_failed(
                conn, request_id,
                {"code": "disk_write_error", "message": str(exc)}, time.time(),
            )
        except sqlite3.Error:
            pass  # DB 本身不可写时也必须退出，重启后从分块/状态一致性恢复
        # 守护线程退出不会带走进程，显式非零退出以触发容器重启策略
        os._exit(86)
