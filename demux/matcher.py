"""条码匹配规则（纯函数，便于单元测试）。

规则（需求原文）：
* 用 R1/R2 两端序列的**前 4 个碱基**分别匹配样本表中两端 4 位 A/C/G/T 条码；
* 每端最多允许 1 处替换（Hamming 距离 <= 1）；
* 只有**两端都命中且唯一命中同一个样本**时才归属该样本；
* 序列不足 4 碱基、无任何样本命中、或同时命中多个样本，一律归未分配。

精确优先语义
============
每个条码建立两张表：

* **精确表** ``exact``：4-mer -> 以该 4-mer 为条码本身的样本；
* **邻域表** ``near``：4-mer -> 与该 4-mer 距离恰好为 1 的样本。

判定某一端时，精确命中优先于容错命中：只要存在精确样本，该端的候选
就只取精确集合——一个样本自己的条码读段，不能因为恰好落在另一个
样本的 1-替换邻域里而被稀释成歧义。两端都没有精确命中时，才使用
邻域候选。两端候选交集唯一才归属，否则按无匹配/歧义归未分配。
"""
from __future__ import annotations

from dataclasses import dataclass

BARCODE_LEN = 4
BASES = b"ACGT"
UNASSIGNED = "unassigned"

# 未分配原因
REASON_OK = "ok"
REASON_SHORT = "short_sequence"
REASON_NO_MATCH = "no_match"
REASON_AMBIGUOUS = "ambiguous"


@dataclass(frozen=True)
class SampleBarcode:
    sample_name: str
    barcode_r1: bytes
    barcode_r2: bytes


def hamming(a: bytes, b: bytes) -> int:
    """等长字节串的 Hamming 距离。"""
    if len(a) != len(b):
        raise ValueError("仅支持等长序列比较")
    return sum(1 for x, y in zip(a, b) if x != y)


def variants_within_one(barcode: bytes) -> list[bytes]:
    """生成条码所有恰好 1 处替换的 4-mer（共 4*3=12 个，不含自身）。"""
    if len(barcode) != BARCODE_LEN:
        raise ValueError(f"条码必须为 {BARCODE_LEN} 位")
    out: list[bytes] = []
    for pos in range(BARCODE_LEN):
        for base in BASES:
            if base == barcode[pos]:
                continue
            variant = bytearray(barcode)
            variant[pos] = base
            out.append(bytes(variant))
    return out


@dataclass(frozen=True)
class Lookup:
    """一端的匹配表：精确表 + 1-替换邻域表。"""

    exact: dict[bytes, set[int]]
    near: dict[bytes, set[int]]


def build_index(samples: list[SampleBarcode]) -> tuple[Lookup, Lookup]:
    """为两端分别构建 ``(精确表, 邻域表)``。"""
    def build(barcodes: list[bytes]) -> Lookup:
        exact: dict[bytes, set[int]] = {}
        near: dict[bytes, set[int]] = {}
        for idx, barcode in enumerate(barcodes):
            exact.setdefault(barcode, set()).add(idx)
            for variant in variants_within_one(barcode):
                near.setdefault(variant, set()).add(idx)
        return Lookup(exact=exact, near=near)

    return (
        build([s.barcode_r1 for s in samples]),
        build([s.barcode_r2 for s in samples]),
    )


def _end_candidates(lookup: Lookup, prefix: bytes) -> set[int]:
    """单端候选：精确命中优先；无精确命中时退回 1-替换邻域。"""
    exact = lookup.exact.get(prefix)
    if exact:
        return set(exact)
    return set(lookup.near.get(prefix, set()))


def assign(
    lookup_r1: Lookup,
    lookup_r2: Lookup,
    prefix_r1: bytes | None,
    prefix_r2: bytes | None,
) -> tuple[int | None, str]:
    """按规则判定一条配对读段的归属。

    参数 ``prefix_*`` 为两端序列前 4 碱基；``None`` 表示该端序列不足 4 碱基。

    返回 ``(样本下标或 None, 原因)``：
    唯一命中返回 ``(idx, "ok")``，其余返回 ``(None, 原因)``。
    """
    if prefix_r1 is None or prefix_r2 is None:
        return None, REASON_SHORT
    candidates = _end_candidates(lookup_r1, prefix_r1) & _end_candidates(
        lookup_r2, prefix_r2
    )
    if len(candidates) == 1:
        return next(iter(candidates)), REASON_OK
    if not candidates:
        return None, REASON_NO_MATCH
    return None, REASON_AMBIGUOUS


def first_four_bases(record_raw: bytes) -> bytes | None:
    """从一条四行 FASTQ 记录的原始字节中取序列行前 4 碱基；不足 4 位返回 None。"""
    parts = record_raw.split(b"\n", 2)
    if len(parts) < 2:
        return None
    sequence = parts[1]
    if len(sequence) < BARCODE_LEN:
        return None
    return sequence[:BARCODE_LEN]
