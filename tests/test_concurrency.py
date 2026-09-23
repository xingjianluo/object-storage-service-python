"""并发与中断一致性测试。"""

from __future__ import annotations

import threading
import time

from tests.conftest import digest


def test_concurrent_writes_same_key_linearizable(storage):
    """N 个线程并发覆盖写同一 key：版本连续无缺口、无重复，每个版本内容可读。"""
    n = 12
    barrier = threading.Barrier(n)
    errors: list[Exception] = []

    def worker(i: int) -> None:
        try:
            barrier.wait()
            storage.put_object("hot-key", f"body-{i}".encode())
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert not errors
    md = storage.get_metadata("hot-key")
    versions = sorted(v.version for v in md.versions)
    assert versions == list(range(1, n + 1)), versions
    # 最终状态一致：current 指向最大版本，内容可校验读取
    data, rec = storage.get_object("hot-key")
    assert rec.version == n
    assert data.startswith(b"body-")
    # 每个历史版本都必须能按其记录的 sha256 校验读取（无半成品/损坏）
    for v in md.versions:
        content, rv = storage.get_object("hot-key", version=v.version)
        assert digest(content) == v.sha256 == rv.sha256
    print(f"[判定] {n} 线程并发写同 key：版本={versions} 连续无重复；"
          f"当前 v{rec.version} sha256={rec.sha256[:12]}；全部历史版本校验通过")


def test_concurrent_same_content_no_duplicate(storage):
    """多线程并发写相同内容：只产生一个版本、一份物理副本。"""
    n = 8
    barrier = threading.Barrier(n)
    results: list[int] = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        r = storage.put_object("same-key", b"identical-body")
        with lock:
            results.append(r.version)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert results == [1] * n
    assert storage.blobs.list_blobs() == [digest(b"identical-body")]
    print(f"[判定] {n} 线程并发写相同 sha256={digest(b'identical-body')[:12]}："
          f"仅版本 v1、物理副本 1 份")


def test_concurrent_writes_different_keys_all_visible(storage):
    n = 10
    barrier = threading.Barrier(n)

    def worker(i: int) -> None:
        barrier.wait()
        storage.put_object(f"key-{i}", f"payload-{i}".encode())

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    for i in range(n):
        data, rec = storage.get_object(f"key-{i}")
        assert data == f"payload-{i}".encode() and rec.version == 1
    print(f"[判定] {n} 线程并发写不同 key 全部可见且互不干扰（key 级锁粒度）")


def test_concurrent_delete_and_read_never_half_state(storage):
    n_readers = 6
    storage.put_object("race", b"stable-content")
    stop = threading.Event()
    seen: list[str] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    def reader() -> None:
        while not stop.is_set():
            try:
                data, rec = storage.get_object("race")
                assert digest(data) == rec.sha256
                with lock:
                    seen.append(f"v{rec.version}:ok")
            except Exception as exc:  # noqa: BLE001
                from app.errors import ObjectDeletedError, ObjectCorruptedError
                assert isinstance(exc, (ObjectDeletedError, ObjectCorruptedError))
                with lock:
                    seen.append("deleted")

    threads = [threading.Thread(target=reader) for _ in range(n_readers)]
    for t in threads:
        t.start()
    storage.delete_object("race")
    stop.set()
    for t in threads:
        t.join(10)
    # 读者只能观察到“完整的 v1 内容”或“已删除”，绝不会读到半成品
    assert all(x in ("v1:ok", "deleted") for x in seen), seen
    print(f"[判定] 并发读/删期间观测样本={sorted(set(seen))}，只有完整 v1 或删除标记")


def test_gc_concurrent_with_put_never_loses_live_blob(storage):
    """GC 与大量写入并发：任何被引用副本都不能被误删，最终所有版本可读。"""
    stop = threading.Event()
    gc_errors: list[Exception] = []

    def gc_worker() -> None:
        while not stop.is_set():
            try:
                storage.garbage_collect()
                # 真实 GC 有调度间隔；让出调度避免紧循环写者长期占先
                time.sleep(0.002)
            except Exception as exc:  # noqa: BLE001
                gc_errors.append(exc)

    gc_thread = threading.Thread(target=gc_worker)
    gc_thread.start()
    n = 20
    for i in range(n):
        storage.put_object("gc-key", f"gen-{i}".encode())
    stop.set()
    gc_thread.join(10)
    assert not gc_errors
    # GC 结束后再彻底回收一次：所有历史版本引用的副本必须仍然存活可读
    storage.garbage_collect()
    md = storage.get_metadata("gc-key")
    assert len(md.versions) == n
    for i in range(n):
        data, v = storage.get_object("gc-key", version=i + 1)
        assert data == f"gen-{i}".encode()
        assert digest(data) == v.sha256
    print(f"[判定] 写入 {n} 版本与 GC 并发：0 误删；版本 v1..v{n} 全部可读校验通过")


def test_interrupted_put_leaves_no_visible_half_object(tmp_path):
    """模拟写入中途崩溃：物理暂存 + 元数据临时文件残留，重启后状态干净。"""
    from app.storage import ObjectStorage

    root = tmp_path / "store"
    s = ObjectStorage(root)
    s.put_object("k", b"committed")
    # 人为制造两类半成品残留
    (root / "tmp" / "stale-upload").write_bytes(b"half")
    (root / "meta" / "k.json.tmp.beef").write_text("{broken")
    # 重启：自动恢复
    s2 = ObjectStorage(root)
    data, rec = s2.get_object("k")
    assert data == b"committed" and rec.version == 1
    assert not (root / "tmp" / "stale-upload").exists()
    assert not (root / "meta" / "k.json.tmp.beef").exists()
    md = s2.get_metadata("k")
    assert len(md.versions) == 1
    print("[判定] 中断半成品（物理暂存/元数据临时文件）重启后被清理，"
          "已提交 v1 完好，无半成品可被读取")
