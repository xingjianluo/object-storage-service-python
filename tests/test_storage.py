"""逻辑层：多版本可见性、去重、删除/回滚、引用与 GC、可区分错误。"""

from __future__ import annotations

import pytest

from app.errors import (
    ChecksumMismatchError,
    InvalidArgumentError,
    ObjectCorruptedError,
    ObjectDeletedError,
    ObjectNotFoundError,
    VersionNotFoundError,
)
from tests.conftest import digest


def test_put_creates_versions_and_current_visibility(storage):
    r1 = storage.put_object("docs/a", b"v1-body")
    r2 = storage.put_object("docs/a", b"v2-body")
    assert (r1.version, r2.version) == (1, 2)
    data, cur = storage.get_object("docs/a")
    assert data == b"v2-body" and cur.version == 2
    # 历史版本仍可按版本读取
    data1, rec1 = storage.get_object("docs/a", version=1)
    assert data1 == b"v1-body" and rec1.version == 1
    print(f"[判定] 版本链 v1({digest(b'v1-body')[:12]}) v2({digest(b'v2-body')[:12]})；"
          f"current_version=2 决定可见性，历史可按版本读")


def test_same_content_dedup_physical_and_idempotent_version(storage):
    d = digest(b"shared")
    r1 = storage.put_object("k", b"shared")
    r2 = storage.put_object("k", b"shared")  # 与当前版本内容相同
    assert r1.version == r2.version == 1
    assert storage.blobs.list_blobs() == [d]
    print(f"[判定] sha256={d} 相同内容物理副本一份、不新增版本（幂等）")


def test_same_content_across_keys_shares_blob(storage):
    d = digest(b"cross-key-content")
    storage.put_object("k1", b"cross-key-content")
    storage.put_object("k2", b"cross-key-content")
    assert storage.blobs.list_blobs() == [d]  # 跨 key 全局去重
    print(f"[判定] 不同 key 相同内容共享物理副本 sha256={d}")


def test_checksum_mismatch_rejects_write(storage):
    with pytest.raises(ChecksumMismatchError):
        storage.put_object("k", b"real", expected_sha256="f" * 64)
    # 被拒绝的写入既无元数据也无物理副本
    with pytest.raises(ObjectNotFoundError):
        storage.get_metadata("k")
    assert storage.blobs.list_blobs() == []
    print(f"[判定] 声明={'f'*8}... 实际={digest(b'real')[:8]}... 拒绝写入且零残留")


def test_delete_marker_visibility_and_read_errors(storage):
    storage.put_object("k", b"data")
    storage.delete_object("k")
    with pytest.raises(ObjectDeletedError):
        storage.get_object("k")
    # 已删除仍可显式读历史版本
    data, rec = storage.get_object("k", version=1)
    assert data == b"data"
    # 删除幂等：重复删除不产生新版本
    rd2 = storage.delete_object("k")
    assert rd2.version == 2 and storage.get_metadata("k").current_version == 2
    print("[判定] delete 标记 v2 遮蔽内容；默认读=ObjectDeletedError，历史 v1 仍可读")


def test_delete_nonexistent_is_not_found(storage):
    with pytest.raises(ObjectNotFoundError):
        storage.delete_object("ghost")
    print("[判定] 删除从不存在的 key -> not_found，与 object_deleted 区分")


def test_rollback_appends_version_pointing_to_historical_blob(storage):
    storage.put_object("k", b"first")
    storage.put_object("k", b"second")
    r = storage.rollback_object("k", 1)
    assert r.version == 3 and r.sha256 == digest(b"first")
    data, cur = storage.get_object("k")
    assert data == b"first" and cur.version == 3
    # 中间版本 v2 仍然保留
    assert storage.get_metadata("k").get(2).sha256 == digest(b"second")
    print(f"[判定] 回滚到 v1 产生新 put 记录 v3 指向 sha256={r.sha256[:12]}，v2 未被修改")


def test_rollback_after_delete_restores_content(storage):
    storage.put_object("k", b"hello")
    storage.delete_object("k")
    storage.rollback_object("k", 1)
    data, rec = storage.get_object("k")
    assert data == b"hello" and rec.version == 3
    print("[判定] 删除后回滚 v1 -> v3 恢复可见内容")


def test_rollback_to_unknown_version_or_marker_rejected(storage):
    storage.put_object("k", b"hello")
    storage.delete_object("k")
    with pytest.raises(VersionNotFoundError):
        storage.rollback_object("k", 99)
    with pytest.raises(InvalidArgumentError):
        storage.rollback_object("k", 2)  # v2 是删除标记
    print("[判定] 回滚到不存在版本=VersionNotFound；回滚到删除标记=InvalidArgument")


def test_get_unknown_version_and_unknown_key_distinguished(storage):
    storage.put_object("k", b"hello")
    with pytest.raises(VersionNotFoundError):
        storage.get_object("k", version=42)
    with pytest.raises(ObjectNotFoundError):
        storage.get_object("nope")
    print("[判定] key 不存在 vs 版本不存在，错误类型不同")


def test_gc_references_all_versions_including_deleted(storage):
    d1 = digest(b"one")
    storage.put_object("k", b"one")
    storage.put_object("k", b"two")
    storage.delete_object("k")  # 删除标记引用当前内容 two
    d2 = digest(b"two")
    # 两个历史内容都被版本记录引用，GC 不得回收
    assert set(storage.garbage_collect()) == set()
    assert set(storage.blobs.list_blobs()) == {d1, d2}
    print(f"[判定] 引用集合={{v1:{d1[:8]}, v2+删除标记:{d2[:8]}}} 均存活，GC 不误删")


def test_gc_removes_only_orphan_blob(storage):
    d_keep = digest(b"alive")
    storage.put_object("keep", b"alive")
    # 手工放入一个不被任何版本记录引用的孤儿副本
    d_orphan = storage.blobs.put(b"orphan")
    removed = storage.garbage_collect()
    assert removed == [d_orphan]
    assert storage.blobs.list_blobs() == [d_keep]
    print(f"[判定] 无引用 sha256={d_orphan[:8]} 被回收；被引用 {d_keep[:8]} 保留")


def test_read_corrupted_current_blob_detected(storage):
    storage.put_object("k", b"original")
    d = digest(b"original")
    # 模拟磁盘静默损坏
    storage.blobs.get_path(d).write_bytes(b"bit-rot")
    with pytest.raises(ObjectCorruptedError):
        storage.get_object("k")
    print(f"[判定] 当前版本物理副本损坏（期望 {d[:8]} 实际 {digest(b'bit-rot')[:8]}）"
          f" -> ObjectCorruptedError，与 404/410 区分")


def test_read_missing_current_blob_detected(storage):
    storage.put_object("k", b"original")
    d = digest(b"original")
    storage.blobs.delete(d)
    with pytest.raises(ObjectCorruptedError) as exc:
        storage.get_object("k")
    assert "缺失" in str(exc.value)
    print(f"[判定] 元数据引用的 sha256={d[:8]} 物理缺失 -> ObjectCorruptedError(缺失)")


def test_size_mismatch_detected(storage):
    rec = storage.put_object("k", b"original")
    # 构造“摘要一致但大小元数据被破坏”的场景：伪造元数据文件
    import json
    path = storage.meta._meta_path("k")
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["versions"][0]["size"] = 999
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ObjectCorruptedError):
        storage.get_object("k")
    print(f"[判定] v{rec.version} 实际大小与元数据 size 不符 -> ObjectCorruptedError")


def test_persistence_after_restart(tmp_path):
    from app.storage import ObjectStorage

    root = tmp_path / "store"
    s1 = ObjectStorage(root)
    s1.put_object("k", b"persist-me")
    s1.put_object("k", b"second")
    s1.delete_object("k")
    # 重新打开（模拟进程重启）：状态完整，无半成品
    s2 = ObjectStorage(root)
    md = s2.get_metadata("k")
    assert md.current_version == 3
    with pytest.raises(ObjectDeletedError):
        s2.get_object("k")
    data, rec = s2.get_object("k", version=1)
    assert data == b"persist-me"
    print("[判定] 重启后版本链 v1/v2/v3 完整，可见性语义一致")
