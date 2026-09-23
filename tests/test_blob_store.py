"""物理层：去重、校验拒绝、损坏检测、缺失检测、中断残留恢复。"""

from __future__ import annotations

import pathlib

import pytest

from app.blob_store import BlobStore
from app.errors import (
    ChecksumMismatchError,
    InvalidArgumentError,
    ObjectCorruptedError,
)
from tests.conftest import digest


def test_put_dedup_keeps_single_physical_copy(tmp_path):
    bs = BlobStore(tmp_path / "blobs")
    data = b"dedup-content-block"
    d = digest(data)
    d1 = bs.put(data)
    d2 = bs.put(data)  # 重复写同样内容
    assert d1 == d2 == d
    path = bs.get_path(d)
    assert path.exists()
    # 物理副本全局只有一份
    assert bs.list_blobs() == [d]
    print(f"[判定] 内容标识 sha256={d}；物理副本数={len(bs.list_blobs())} 去重成立")


def test_put_existing_digest_not_overwritten(tmp_path):
    bs = BlobStore(tmp_path / "blobs")
    data = b"same-bytes-again"
    d = bs.put(data)
    path = bs.get_path(d)
    before = path.stat().st_mtime_ns
    # 并发/重复写同内容：已存在副本不得被破坏（内容仍可读、摘要一致）
    assert bs.put(data) == d
    assert path.read_bytes() == data
    assert path.stat().st_mtime_ns == before
    assert bs.read_verified(d) == data
    print(f"[判定] sha256={d} 重复写入后路径未重写(mtime 不变)，既有副本完好")


def test_checksum_mismatch_is_rejected_and_leaves_nothing(tmp_path):
    bs = BlobStore(tmp_path / "blobs")
    data = b"actual-body"
    wrong = "a" * 64
    assert wrong != digest(data)
    with pytest.raises(ChecksumMismatchError):
        bs.put(data, expected_sha256=wrong)
    # 拒绝后物理层与暂存区都不能有残留
    assert bs.list_blobs() == []
    leftovers = list(bs.tmp_dir.iterdir())
    assert leftovers == []
    print(f"[判定] 声明={wrong} 实际={digest(data)} -> 写入被拒绝且无残留")


def test_invalid_expected_digest_format(tmp_path):
    bs = BlobStore(tmp_path / "blobs")
    with pytest.raises(InvalidArgumentError):
        bs.put(b"x", expected_sha256="not-a-sha256")
    print("[判定] 非法摘要格式 -> InvalidArgumentError，与内容不匹配区分")


def test_read_corrupted_blob_is_detected(tmp_path):
    bs = BlobStore(tmp_path / "blobs")
    d = bs.put(b"original-content")
    path = bs.get_path(d)
    # 模拟磁盘静默损坏：篡改字节但文件名（摘要）不变
    path.write_bytes(b"corrupted-bytes")
    with pytest.raises(ObjectCorruptedError) as exc:
        bs.read_verified(d)
    assert "损坏" in str(exc.value)
    print(f"[判定] 期望摘要={d} 实际={digest(b'corrupted-bytes')} -> ObjectCorruptedError")


def test_read_missing_blob_is_detected(tmp_path):
    bs = BlobStore(tmp_path / "blobs")
    d = digest(b"ghost")
    with pytest.raises(ObjectCorruptedError) as exc:
        bs.read_verified(d)
    assert "缺失" in str(exc.value)
    print(f"[判定] 缺失副本 sha256={d} -> ObjectCorruptedError(缺失)，与损坏可区分")


def test_recover_cleans_tmp_leftovers(tmp_path):
    bs = BlobStore(tmp_path / "blobs")
    # 模拟写入中途崩溃：暂存区留下半成品，blobs 下无最终副本
    (bs.tmp_dir / "abc123").write_bytes(b"half-written")
    removed = bs.recover()
    assert removed == ["abc123"]
    assert list(bs.tmp_dir.iterdir()) == []
    assert bs.list_blobs() == []
    print(f"[判定] 中断残留 {removed} 被 recover 清理，半成品从未进入 blobs/")


def test_reopen_storage_recovers_automatically(tmp_path):
    from app.storage import ObjectStorage

    root = tmp_path / "store"
    bs = BlobStore(root)
    (bs.tmp_dir / "leftover").write_bytes(b"x")
    ObjectStorage(root)  # 重新初始化（模拟进程重启）自动触发恢复
    assert not (bs.tmp_dir / "leftover").exists()
    print("[判定] 进程重启后暂存残留被清理，无半成品可读")
