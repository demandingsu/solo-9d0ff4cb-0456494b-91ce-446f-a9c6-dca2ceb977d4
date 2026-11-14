"""拆样条码匹配规则的单元测试（纯函数，无需起服务）。"""
import pytest

from demux.matcher import (
    BARCODE_LEN,
    REASON_AMBIGUOUS,
    REASON_NO_MATCH,
    REASON_OK,
    REASON_SHORT,
    SampleBarcode,
    assign,
    build_index,
    first_four_bases,
    hamming,
    variants_within_one,
)

SAMPLES = [
    SampleBarcode("S1", b"AAAA", b"CCCC"),
    SampleBarcode("S2", b"GGGG", b"TTTT"),
    SampleBarcode("S3", b"ACGT", b"TGCA"),
]


@pytest.fixture(scope="module")
def lookups():
    return build_index(SAMPLES)


def test_hamming():
    assert hamming(b"AAAA", b"AAAA") == 0
    assert hamming(b"AAAA", b"AACA") == 1
    assert hamming(b"ACGT", b"TGCA") == 4
    with pytest.raises(ValueError):
        hamming(b"AC", b"ACGT")


def test_variants_neighborhood_size_and_content():
    vs = variants_within_one(b"AAAA")
    assert len(vs) == BARCODE_LEN * 3  # 恰好 12 个单替换，不含自身
    assert b"AAAA" not in vs
    assert b"AACA" in vs and b"AAGA" in vs and b"AATA" in vs
    # 距离 2 的邻域不应出现
    assert b"AAGG" not in vs
    assert all(len(v) == BARCODE_LEN for v in vs)


def test_exact_barcode_assignment(lookups):
    idx, reason = assign(*lookups, b"AAAA", b"CCCC")
    assert (idx, reason) == (0, REASON_OK)
    idx, reason = assign(*lookups, b"GGGG", b"TTTT")
    assert (idx, reason) == (1, REASON_OK)
    idx, reason = assign(*lookups, b"ACGT", b"TGCA")
    assert (idx, reason) == (2, REASON_OK)


def test_one_substitution_per_end_is_allowed(lookups):
    # R1 一处替换、R2 精确
    idx, reason = assign(*lookups, b"AAGA", b"CCCC")
    assert (idx, reason) == (0, REASON_OK)
    # 两端各一处替换
    idx, reason = assign(*lookups, b"ACGT", b"TGTA")
    assert (idx, reason) == (2, REASON_OK)
    # R2 一处替换、R1 精确
    idx, reason = assign(*lookups, b"GGGG", b"TATT")
    assert (idx, reason) == (1, REASON_OK)


def test_two_substitutions_on_one_end_rejected(lookups):
    # R1 距离 2：无命中
    idx, reason = assign(*lookups, b"AAGG", b"CCCC")
    assert idx is None and reason == REASON_NO_MATCH


def test_short_sequence_is_unassigned(lookups):
    idx, reason = assign(*lookups, None, b"CCCC")
    assert idx is None and reason == REASON_SHORT
    idx, reason = assign(*lookups, b"AAAA", None)
    assert idx is None and reason == REASON_SHORT


def test_unknown_barcode_no_match(lookups):
    idx, reason = assign(*lookups, b"NNNN", b"NNNN")
    assert idx is None and reason == REASON_NO_MATCH
    # 一端命中另一端不命中，同样整体不匹配
    idx, reason = assign(*lookups, b"AAAA", b"GGGG")
    assert idx is None and reason == REASON_NO_MATCH


def test_exact_match_takes_priority_over_neighbor():
    """两个样本条码相距 1：读段精确等于其一条码时必须唯一归属，不能算歧义。"""
    samples = [
        SampleBarcode("A", b"AAAA", b"CCCC"),
        SampleBarcode("B", b"AAAG", b"CCCC"),
    ]
    l1, l2 = build_index(samples)
    idx, reason = assign(l1, l2, b"AAAA", b"CCCC")
    assert (idx, reason) == (0, REASON_OK)
    idx, reason = assign(l1, l2, b"AAAG", b"CCCC")
    assert (idx, reason) == (1, REASON_OK)
    # 两端都只落在邻域（距两个样本都为 1）才是歧义
    idx, reason = assign(l1, l2, b"AAAT", b"CCCT")
    assert idx is None and reason == REASON_AMBIGUOUS
    # 距离两个 R1 都 >1 时为无匹配
    idx, reason = assign(l1, l2, b"AATT", b"CCCC")
    assert idx is None and reason == REASON_NO_MATCH


def test_ambiguous_when_both_ends_only_have_near_hits():
    # 两端候选交集含两个样本，且任一端都没有精确命中 → 歧义
    samples = [
        SampleBarcode("A", b"AAAA", b"CCCC"),
        SampleBarcode("B", b"AAAG", b"CCCG"),
    ]
    l1, l2 = build_index(samples)
    # R1: AAAT 距 AAAA=1、距 AAAG=1；R2: CCCA 距 CCCC=1、距 CCCG=1
    idx, reason = assign(l1, l2, b"AAAT", b"CCCA")
    assert idx is None and reason == REASON_AMBIGUOUS
    # R1 精确锁定 A 后，即使 R2 邻域同时含 A/B，仍唯一归属 A
    idx, reason = assign(l1, l2, b"AAAA", b"CCCA")
    assert (idx, reason) == (0, REASON_OK)


def test_first_four_bases_from_fastq_record():
    raw = b"@read/1\nACGTACGT\n+\nIIIIIIII\n"
    assert first_four_bases(raw) == b"ACGT"
    short = b"@read/1\nACG\n+\nIII\n"
    assert first_four_bases(short) is None
    # N 也按普通字符参与匹配（不会命中 ACGT 邻域）
    assert first_four_bases(b"@r/1\nNAAA\n+\nIIII\n") == b"NAAA"


def test_index_exact_and_near_tables(lookups):
    l1, l2 = lookups
    # 精确表只含条码自身对应的样本
    assert l1.exact[b"AAAA"] == {0}
    assert l2.exact[b"TTTT"] == {1}
    # 邻域表含距离 1 的样本；精确键不会出现在邻域表中
    assert 0 in l1.near[b"AAGA"]
    assert b"AAAA" not in l1.near
    # 一个 4-mer 可以同时落在多个样本的邻域中（歧义的来源）
    samples = [
        SampleBarcode("A", b"AAAA", b"CCCC"),
        SampleBarcode("B", b"AAAG", b"CCCC"),
    ]
    la, _ = build_index(samples)
    assert la.near[b"AAAT"] == {0, 1}
