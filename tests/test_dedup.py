"""去重与引用完整性测试。

判定依据：blob 物理副本数、文件内容 sha256 与 digest 命名、
多对象共享引用、历史版本引用、GC 存活集合。
"""

from __future__ import annotations

from pathlib import Path

from app.hashing import sha256_hex

from .conftest import log_case


def _physical_blobs(root: Path) -> set[str]:
    return {p.name for p in root.glob("blobs/*/*") if p.is_file()}


def test_same_content_single_physical_copy(service, tmp_root):
    content = b"payload-A" * 100
    digest = sha256_hex(content)

    r1 = service.put_object("obj-1", [content])
    r2 = service.put_object("obj-1", [content])  # 同 key 同内容
    r3 = service.put_object("obj-2", [content])  # 不同 key 同内容

    physical = _physical_blobs(tmp_root)
    log_case(
        "去重-同内容单副本",
        digest=digest,
        versions=(r1.version.version, r2.version.version, r3.version.version),
        dedup=(r1.deduplicated, r2.deduplicated, r3.deduplicated),
        依据="sha256相同则同一blob路径,物理副本数=1",
        physical_count=len(physical),
    )
    assert physical == {digest}
    assert r1.deduplicated is False       # 首次物理写入
    assert r2.deduplicated is True        # 同 key 重复，复用
    assert r3.deduplicated is True        # 跨 key 复用
    # 重复写没有破坏已有对象
    assert service.get_object("obj-1")[0] == content
    assert service.get_object("obj-2")[0] == content


def test_different_content_separate_copies(service, tmp_root):
    a, b = b"alpha", b"beta-different"
    da, db = sha256_hex(a), sha256_hex(b)
    service.put_object("k", [a])
    service.put_object("k", [b])

    physical = _physical_blobs(tmp_root)
    log_case(
        "去重-异内容双副本",
        digests=(da[:12], db[:12]),
        versions="1,2",
        依据="sha256不同->不同blob,历史版本各自保留",
        physical_count=len(physical),
    )
    assert physical == {da, db}
    # 两个历史版本都可独立读取
    assert service.get_object("k", version=1)[0] == a
    assert service.get_object("k", version=2)[0] == b


def test_gc_keeps_referenced_and_collects_orphans(service, tmp_root):
    v1 = service.put_object("k", [b"version-one"]).version
    v2 = service.put_object("k", [b"version-two"]).version
    orphan = service.blob_store.put([b"unreferenced-half-product"]).digest

    collected = service.gc()
    alive = _physical_blobs(tmp_root)
    log_case(
        "GC-引用判定",
        referenced=(v1.digest[:12], v2.digest[:12]),
        orphan=orphan[:12],
        collected=[d[:12] for d in collected],
        依据="任一版本记录(含历史)引用即存活,无引用即回收",
    )
    assert orphan in collected
    assert v1.digest in alive and v2.digest in alive
    assert orphan not in alive
    # 再 GC 一次：无新增垃圾，稳定收敛
    assert service.gc() == []


def test_dedup_does_not_corrupt_existing_object(service, tmp_root):
    content = b"important-data"
    digest = sha256_hex(content)
    service.put_object("important", [content])
    path = service.blob_store._path_for(digest)
    before = path.read_bytes()

    # 大量重复写入
    for i in range(20):
        service.put_object(f"copy-{i}", [content])
    after = path.read_bytes()
    log_case(
        "去重-不破坏已存在对象",
        digest=digest[:12],
        copies=20,
        依据="重复写仅丢弃暂存,不覆盖已校验副本",
        unchanged=before == after,
    )
    assert before == after
    assert len(_physical_blobs(tmp_root)) == 1
