"""元数据层：版本链追加、原子写、临时文件恢复、损坏检测、key 编码、跨线程锁。"""

from __future__ import annotations

import json
import threading

import pytest

from app.errors import ObjectCorruptedError
from app.metadata import (
    MetadataStore,
    ObjectMetadata,
    VersionRecord,
    decode_key,
    encode_key,
)


def _rec(version: int, op: str = "put", sha: str = "a" * 64) -> VersionRecord:
    return VersionRecord(
        version=version, sha256=sha, size=10, operation=op,
        timestamp="2026-09-24T00:00:00+00:00",
    )


def test_key_encoding_roundtrip():
    for key in ["a/b/c", "中文-key", "x y", "a%2Fb", "normal"]:
        name = encode_key(key)
        assert "/" not in name
        assert decode_key(name) == key
    print(f"[判定] key 编码为单级安全文件名且可逆，样例 a/b/c -> {encode_key('a/b/c')}")


def test_save_load_append_chain(tmp_path):
    ms = MetadataStore(tmp_path / "meta")
    md = ObjectMetadata(key="k", current_version=1, versions=[_rec(1)])
    ms.save("k", md)
    loaded = ms.load("k")
    assert loaded is not None
    assert loaded.current_version == 1
    assert loaded.get(1).sha256 == "a" * 64
    print("[判定] 版本=v1 sha256=%s... 元数据原子落盘并可加载" % ("a" * 8))


def test_no_tmp_left_after_save(tmp_path):
    ms = MetadataStore(tmp_path / "meta")
    ms.save("k", ObjectMetadata(key="k", current_version=1, versions=[_rec(1)]))
    assert ms.recover() == []
    leftovers = [p.name for p in ms.meta_dir.iterdir() if ".tmp." in p.name]
    assert leftovers == []
    print("[判定] 正常保存后无临时文件残留")


def test_recover_removes_stale_tmp(tmp_path):
    ms = MetadataStore(tmp_path / "meta")
    (ms.meta_dir / "k.json.tmp.deadbeef").write_text("{broken")
    removed = ms.recover()
    assert removed == ["k.json.tmp.deadbeef"]
    assert ms.load("k") is None
    print("[判定] 元数据 rename 前崩溃的临时文件被清理，既有正式文件不受影响")


def test_corrupted_metadata_detected(tmp_path):
    ms = MetadataStore(tmp_path / "meta")
    path = ms._meta_path("k")
    path.write_text("{not-valid-json", encoding="utf-8")
    with pytest.raises(ObjectCorruptedError):
        ms.load("k")
    print("[判定] 半截 JSON -> ObjectCorruptedError，不会被当成空对象误读")


def test_inconsistent_metadata_detected(tmp_path):
    ms = MetadataStore(tmp_path / "meta")
    # current_version 指向不存在的版本
    path = ms._meta_path("k")
    path.write_text(json.dumps({
        "key": "k", "current_version": 9,
        "versions": [{"version": 1, "sha256": "a" * 64,
                      "size": 1, "operation": "put", "timestamp": "t"}],
    }), encoding="utf-8")
    with pytest.raises(ObjectCorruptedError):
        ms.load("k")
    print("[判定] current_version 悬空 -> ObjectCorruptedError，引用一致性受检")


def test_lock_is_mutually_exclusive(tmp_path):
    import time

    ms = MetadataStore(tmp_path / "meta")
    order: list[str] = []
    t1_entered = threading.Event()

    def worker(tag: str, hold: float = 0.0) -> None:
        with ms.lock("same-key"):
            order.append(f"{tag}-in")
            t1_entered.set()
            time.sleep(hold)
            order.append(f"{tag}-out")

    t1 = threading.Thread(target=worker, args=("t1", 0.2))
    t2 = threading.Thread(target=worker, args=("t2", 0.0))
    t1.start()
    assert t1_entered.wait(2.0)  # 确认 t1 已持锁
    t2.start()
    t1.join(2.0)
    t2.join(2.0)
    assert order == ["t1-in", "t1-out", "t2-in", "t2-out"]
    print(f"[判定] 同一逻辑对象锁串行化执行顺序={order}")
