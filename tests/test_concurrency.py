"""并发一致性测试：多线程并发写同一逻辑对象。

预期：
- 版本号 1..N 不重复不跳号；
- 最终只存在一个 current，且其指向的 blob 内容可校验、可读；
- 每个版本记录的 digest 都能在物理层找到且内容一致；
- 内容去重仍然成立。
判定依据：服务写锁串行化“落blob+发布meta”，meta 原子 rename。
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from app.hashing import sha256_hex

from .conftest import log_case


def test_concurrent_writes_same_key(service, tmp_root):
    n = 30
    barrier = threading.Barrier(n)

    def write(i: int) -> bytes:
        content = f"content-{i}".encode()
        barrier.wait()  # 最大化同时进入的概率
        result = service.put_object("hot-key", [content],
                                    expected_digest=sha256_hex(content))
        return result.version.digest

    with ThreadPoolExecutor(max_workers=n) as pool:
        digests = list(pool.map(write, range(n)))

    versions = service.list_versions("hot-key")
    nums = [v.version for v in versions]
    log_case(
        "并发写同一key",
        writers=n,
        versions=(min(nums), max(nums)),
        unique_versions=len(set(nums)),
        依据="写锁串行化->版本号1..N不重不漏,meta原子发布",
        current=service.stat("hot-key").current,
    )
    assert sorted(nums) == list(range(1, n + 1))
    assert len(set(nums)) == n

    # 每个版本引用的 blob 必须物理存在、哈希自洽，且对应某次真实写入内容
    content_digests = {sha256_hex(f"content-{i}".encode()) for i in range(n)}
    for v in versions:
        assert v.digest in content_digests
        assert service.blob_store.verify(v.digest) == v.size
        assert sha256_hex(service.blob_store.get(v.digest)) == v.digest

    # 当前版本可读且与某一写入内容一致
    data, cur = service.get_object("hot-key")
    assert cur.version == n
    assert data.startswith(b"content-")
    assert sha256_hex(data) == cur.digest
    # 物理副本数 == 不同内容数（去重在并发下仍成立）
    physical = service.blob_store.all_digests()
    assert len(physical) == len(set(digests))
    log_case(
        "并发写-去重与可读性",
        physical_blobs=len(physical),
        distinct_contents=len(set(digests)),
        current_digest=cur.digest[:12],
        依据="每版本blob可校验,current内容sha256一致",
    )


def test_concurrent_writes_duplicate_content(service):
    n = 24
    same = b"identical-content"
    digest = sha256_hex(same)
    barrier = threading.Barrier(n)

    def write(_: int):
        barrier.wait()
        return service.put_object("dup-key", [same])

    with ThreadPoolExecutor(max_workers=n) as pool:
        results = list(pool.map(write, range(n)))

    versions = service.list_versions("dup-key")
    dedup_hits = sum(1 for r in results if r.deduplicated)
    log_case(
        "并发写相同内容",
        writers=n,
        versions=len(versions),
        dedup_hits=dedup_hits,
        digest=digest[:12],
        依据="内容相同->物理仅一份;元数据版本仍每个写请求一份",
    )
    assert len(versions) == n
    assert service.blob_store.all_digests() == {digest}
    assert all(v.digest == digest for v in versions)


def test_concurrent_reads_while_writing(service):
    service.put_object("k", [b"init"])
    stop = threading.Event()
    errors: list[Exception] = []

    def reader() -> None:
        while not stop.is_set():
            try:
                data, ver = service.get_object("k")
                # 核心不变量：读到的内容必须与该版本 digest 自洽（无半成品）
                assert sha256_hex(data) == ver.digest
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

    readers = [threading.Thread(target=reader) for _ in range(8)]
    for t in readers:
        t.start()
    try:
        for i in range(1, 41):
            service.put_object("k", [f"v{i}-" .encode() + b"x" * i])
    finally:
        stop.set()
        for t in readers:
            t.join()

    log_case(
        "并发读写-无半成品",
        writes=40,
        readers=8,
        errors=len(errors),
        依据="读者每次读到的内容sha256必须等于版本记录digest",
    )
    assert errors == []
